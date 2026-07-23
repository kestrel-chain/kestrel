//! Measures *sustained* committed throughput under continuous offered load,
//! as opposed to `profile_pipeline`, which submits one burst right after
//! observing the chain height and therefore always pays a one-block cold-start
//! penalty (the block already in flight when the burst lands is necessarily
//! empty). That cold-start cost is a real property of burst-after-observe
//! measurement, but it is paid only once per idle->active transition; on a
//! continuously fed chain it does not recur, so a burst measurement understates
//! steady-state capacity. This tool feeds the chain at a fixed offered rate for
//! several seconds and measures the committed rate over a trimmed middle window
//! (excluding warm-up), sweeping the offered rate to find where the chain stops
//! keeping up.
//!
//! Topology, stated explicitly since it changes what can be concluded: the same
//! four separate real `node` OS processes as `profile_pipeline` -- each running
//! the production `ConsensusCoordinator` (raw-TCP BLS votes/certificates) and
//! `Stage2Pipeline` (real libp2p transaction gossip + `KestrelCast` shred
//! relay), equal stake, a genuine 4-way quorum -- all on one machine over
//! loopback, so this is still not a Stage 2 (real-hosts) result.
//!
//! Committed throughput is read from each node's own durable-commit log line
//! ("committed block", emitted by `BlockLifecycle::poll_commit` after the
//! `RocksDB` write), not from build-time proposal counts, so a proposal that is
//! built but never committed is never counted. Each offered rate is run for
//! several independent repetitions and reported as mean/95% CI.
//!
//! Usage: `cargo run --release -p node --example measure_steady_state -- [VALIDATOR_COUNT] [DURATION_SECS] [REPETITIONS] [RATE...]`

use std::{
    collections::BTreeMap,
    env, fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use consensus::Validator;
use crypto::{Bls12381Scheme, Ed25519Scheme, SignatureScheme};
use execution::{AccessMode, DeclaredObjectRef, ExecutableTransaction, MoveOperation};
use libp2p::identity;
use node::{GENESIS_FORMAT_VERSION, GenesisDocument, GenesisValidator};
use serde_json::Value;
use tempfile::TempDir;
use types::{Address, Hash, Object, Owner, Transaction};

/// Submitter threads. The offered rate is split evenly across them so a single
/// thread's per-submit RPC cost never caps the aggregate offered rate below the
/// target.
const SUBMITTER_THREADS: usize = 4;
/// Leading slice of the run discarded before measuring, so the reported rate is
/// steady-state, not the pipeline filling up.
const WARMUP: Duration = Duration::from_millis(1_000);
/// Grace period after the last submission before logs are parsed, so the final
/// in-window commits are durably written and flushed before the processes die.
const COOLDOWN: Duration = Duration::from_millis(1_000);

struct Sender {
    key: [u8; 32],
    public_key: Vec<u8>,
    address: Address,
    object: Object,
}

struct RunResult {
    /// Transactions actually accepted by an RPC submit call, per second, over
    /// the measurement window. Should track the target rate closely; if it
    /// falls short the submitter (not the chain) was the limit.
    offered_rate: f64,
    /// Durably committed transactions per second over the measurement window --
    /// the actual sustained throughput.
    achieved_rate: f64,
    /// Mean transactions per committed block within the window (batching).
    block_occupancy: f64,
}

#[allow(clippy::too_many_lines)] // Keep the sweep/report timeline auditable in one place.
#[allow(clippy::cast_precision_loss)] // Rates/counts here are small test-run sizes, not chain data.
fn main() {
    let arguments = env::args().collect::<Vec<_>>();
    let validator_count: u8 = arguments
        .get(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let duration_secs: f64 = arguments
        .get(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(5.0);
    let repetitions: usize = arguments
        .get(3)
        .and_then(|value| value.parse().ok())
        .unwrap_or(5);
    let rates: Vec<f64> = {
        let parsed = arguments
            .iter()
            .skip(4)
            .filter_map(|value| value.parse().ok())
            .collect::<Vec<f64>>();
        if parsed.is_empty() {
            vec![150.0, 300.0, 450.0, 600.0]
        } else {
            parsed
        }
    };

    println!(
        "topology: {validator_count} separate real node processes, real raw-TCP consensus \
         (BLS votes/certificates) + real libp2p transaction gossip/KestrelCast, equal stake, \
         all over loopback -- a genuine {validator_count}-way quorum, not a single validator."
    );
    println!(
        "method: continuous paced load for {duration_secs:.0}s per run ({SUBMITTER_THREADS} \
         submitter threads, round-robin across all validators, single-use senders); committed \
         throughput measured from durable-commit logs over the window \
         [{:.1}s, {duration_secs:.0}s]; {repetitions} repetitions per offered rate.\n",
        WARMUP.as_secs_f64()
    );

    println!(
        "{:>12} | {:>14} | {:>16} | {:>12} | {:>10}",
        "offered/s", "achieved/s", "achieved 95% CI", "tx/block", "kept up?"
    );
    println!("{}", "-".repeat(76));

    for target_rate in rates {
        let mut results = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            results.push(run_once(validator_count, duration_secs, target_rate));
        }
        let offered = results.iter().map(|r| r.offered_rate).collect::<Vec<_>>();
        let achieved = results.iter().map(|r| r.achieved_rate).collect::<Vec<_>>();
        let occupancy = results
            .iter()
            .map(|r| r.block_occupancy)
            .collect::<Vec<_>>();
        let (offered_mean, _) = mean_and_95_ci(&offered);
        let (achieved_mean, achieved_ci) = mean_and_95_ci(&achieved);
        let (occupancy_mean, _) = mean_and_95_ci(&occupancy);
        // "Kept up" means committed throughput matched what was actually
        // offered (not merely the target): a ratio near 1.0 means the chain
        // was not the bottleneck at this rate.
        let kept_up = achieved_mean / offered_mean;
        println!(
            "{target_rate:>12.0} | {achieved_mean:>14.1} | {:>16} | {occupancy_mean:>12.1} | {:>9.0}%",
            format!("+/- {achieved_ci:.1}"),
            kept_up * 100.0,
        );
    }
}

#[allow(clippy::too_many_lines)] // Keep the process-spawn/load/parse timeline auditable.
#[allow(clippy::cast_precision_loss)] // Counts here are small test-run sizes, not chain data.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)] // rate*duration is a small positive count.
fn run_once(validator_count: u8, duration_secs: f64, target_rate: f64) -> RunResult {
    let directory = TempDir::new().unwrap();
    let transaction_count = (target_rate * duration_secs).ceil() as usize;
    let senders = (0..transaction_count)
        .map(|index| {
            let key = *Hash::digest(index.to_le_bytes()).as_bytes();
            let public_key = Ed25519Scheme.public_key(&key).unwrap();
            let address = Ed25519Scheme.address(&public_key).unwrap();
            let object = Object {
                id: Hash::digest(format!("steady-object-{index}").as_bytes()),
                owner: Owner::Single(address),
                type_tag: "measure_steady_state::Object".to_owned(),
                version: 0,
                data: vec![0],
                rent_balance: 1_000,
            };
            Sender {
                key,
                public_key,
                address,
                object,
            }
        })
        .collect::<Vec<_>>();

    let (genesis, keys, gossip_identities) = fixture_genesis(
        validator_count,
        senders.iter().map(|sender| sender.object.clone()).collect(),
    );
    let genesis_path = directory.path().join("genesis.json");
    genesis.write_json(&genesis_path).unwrap();

    let node_binary = node_binary_path();
    let log_paths = (0..genesis.validators.len())
        .map(|index| directory.path().join(format!("node-{index}.log")))
        .collect::<Vec<_>>();
    let mut children = Vec::new();
    for (index, entry) in genesis.validators.iter().enumerate() {
        let key_path = directory.path().join(format!("validator-{index}.key"));
        fs::write(&key_path, hex::encode(&keys[index])).unwrap();
        let gossip_key_path = directory.path().join(format!("gossip-{index}.key"));
        fs::write(
            &gossip_key_path,
            gossip_identities[index].to_protobuf_encoding().unwrap(),
        )
        .unwrap();
        let data_path = directory.path().join(format!("data-{index}"));
        let child = Command::new(&node_binary)
            .args([
                "run",
                "--genesis",
                genesis_path.to_str().unwrap(),
                "--rpc",
                &entry.rpc_address,
                "--validator-id",
                &entry.validator.id.to_string(),
                "--validator-key",
                key_path.to_str().unwrap(),
                "--gossip-key",
                gossip_key_path.to_str().unwrap(),
                "--data-dir",
                data_path.to_str().unwrap(),
            ])
            .env("RUST_LOG", "node=debug")
            .stdout(Stdio::from(fs::File::create(&log_paths[index]).unwrap()))
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        children.push(ProcessNode {
            rpc: entry.rpc_address.parse().unwrap(),
            child,
        });
    }
    let rpc_addresses = children.iter().map(|node| node.rpc).collect::<Vec<_>>();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let wait =
        u64::try_from(u128::from(genesis.genesis_unix_ms).saturating_sub(now)).unwrap_or_default();
    thread::sleep(Duration::from_millis(wait.saturating_add(500)));

    let ready_deadline = Instant::now() + Duration::from_secs(40);
    loop {
        let ready = children
            .iter()
            .filter_map(|node| rpc_call(node.rpc, "kestrel_getStatus", &Value::Null).ok())
            .filter(|status| status["result"]["ready"] == true)
            .count();
        if ready == children.len() {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "not all node processes became ready in time"
        );
        thread::sleep(Duration::from_millis(50));
    }

    // Time-of-day, in microseconds since midnight UTC, of the moment paced
    // submission begins -- the same clock basis as the tracing log timestamps
    // (see `parse_timestamp_micros`), so a commit log line can be placed inside
    // or outside the measurement window. Runs are seconds long, so the once-a-day
    // wraparound is not a concern.
    let start_instant = Instant::now();
    let start_tod = time_of_day_micros();
    let duration = Duration::from_secs_f64(duration_secs);
    let window_start_tod = start_tod + WARMUP.as_micros() as f64;
    let window_end_tod = start_tod + duration.as_micros() as f64;
    let window_secs = duration_secs - WARMUP.as_secs_f64();

    // Paced open-loop submission: each thread owns a disjoint stride of the
    // single-use sender pool and releases its k-th transaction at the global
    // schedule time `index / target_rate`, so the aggregate offered rate is
    // `target_rate` regardless of how many threads there are.
    let submitted_in_window = thread::scope(|scope| {
        let handles = (0..SUBMITTER_THREADS)
            .map(|thread_index| {
                let senders = &senders;
                let rpc_addresses = &rpc_addresses;
                scope.spawn(move || {
                    let mut in_window = 0usize;
                    let mut global_index = thread_index;
                    while global_index < senders.len() {
                        let scheduled = global_index as f64 / target_rate;
                        if scheduled >= duration_secs {
                            break;
                        }
                        let target = start_instant + Duration::from_secs_f64(scheduled);
                        let now = Instant::now();
                        if target > now {
                            thread::sleep(target - now);
                        }
                        let sender = &senders[global_index];
                        let transaction = signed_mutation(sender, 0, 1);
                        let encoded = hex::encode(bcs::to_bytes(&transaction).unwrap());
                        let target_rpc = rpc_addresses[global_index % rpc_addresses.len()];
                        let accepted = rpc_call(
                            target_rpc,
                            "kestrel_submitTransaction",
                            &serde_json::json!({ "transaction": encoded }),
                        )
                        .is_ok_and(|response| response.get("result").is_some());
                        if accepted && scheduled >= WARMUP.as_secs_f64() {
                            in_window += 1;
                        }
                        global_index += SUBMITTER_THREADS;
                    }
                    in_window
                })
            })
            .collect::<Vec<_>>();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum::<usize>()
    });

    // Let the final in-window commits reach disk and the logs before the
    // processes are killed.
    thread::sleep(COOLDOWN);
    drop(children); // Drop impl kills every child process.

    // height -> (earliest commit time-of-day across nodes, transaction count).
    // Every node logs a commit for the same height; count each height once.
    let mut commits: BTreeMap<u64, (f64, usize)> = BTreeMap::new();
    for log_path in &log_paths {
        for line in fs::read_to_string(log_path).unwrap_or_default().lines() {
            let Ok(parsed) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if parsed["fields"]["message"] == "committed block"
                && let Some(height) = parsed["fields"]["height"].as_u64()
                && let Some(count) = parsed["fields"]["transaction_count"].as_u64()
                && let Some(timestamp) = parsed["timestamp"].as_str()
                && let Some(tod) = parse_timestamp_micros(timestamp)
            {
                let entry = commits
                    .entry(height)
                    .or_insert((tod, usize::try_from(count).unwrap()));
                if tod < entry.0 {
                    entry.0 = tod;
                }
            }
        }
    }

    let in_window = commits
        .values()
        .filter(|(tod, _)| *tod >= window_start_tod && *tod < window_end_tod)
        .collect::<Vec<_>>();
    let committed_in_window = in_window.iter().map(|(_, count)| *count).sum::<usize>();
    let blocks_in_window = in_window.len();
    eprintln!(
        "    [debug] target={target_rate:.0} submitted_in_window={submitted_in_window} \
         committed_in_window={committed_in_window} blocks_in_window={blocks_in_window}"
    );

    RunResult {
        offered_rate: submitted_in_window as f64 / window_secs,
        achieved_rate: committed_in_window as f64 / window_secs,
        block_occupancy: if blocks_in_window == 0 {
            0.0
        } else {
            committed_in_window as f64 / blocks_in_window as f64
        },
    }
}

#[allow(clippy::cast_precision_loss)] // sample sizes here are tiny test-repetition counts, not chain data.
fn mean_and_95_ci(values: &[f64]) -> (f64, f64) {
    let n = values.len() as f64;
    if n == 0.0 {
        return (f64::NAN, f64::NAN);
    }
    let mean = values.iter().sum::<f64>() / n;
    if n < 2.0 {
        return (mean, f64::NAN);
    }
    let variance = values
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / (n - 1.0);
    let standard_error = variance.sqrt() / n.sqrt();
    (mean, 1.96 * standard_error)
}

/// Microseconds since midnight UTC, matching `parse_timestamp_micros`'s basis.
#[allow(clippy::cast_precision_loss)] // Sub-day microsecond counts are well within f64's exact range.
fn time_of_day_micros() -> f64 {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    (micros % (86_400 * 1_000_000)) as f64
}

/// Parses the time-of-day portion of an RFC3339 timestamp like
/// "2026-07-23T16:24:55.663065Z" into microseconds since midnight UTC. Only
/// used for within-run deltas over a few seconds, so day-boundary wraparound is
/// not a concern.
fn parse_timestamp_micros(rfc3339: &str) -> Option<f64> {
    let time_part = rfc3339.split('T').nth(1)?.trim_end_matches('Z');
    let mut parts = time_part.split(':');
    let hours: f64 = parts.next()?.parse().ok()?;
    let minutes: f64 = parts.next()?.parse().ok()?;
    let seconds: f64 = parts.next()?.parse().ok()?;
    Some((hours * 3600.0 + minutes * 60.0 + seconds) * 1_000_000.0)
}

fn node_binary_path() -> std::path::PathBuf {
    let mut path = env::current_exe().unwrap();
    path.pop();
    path.pop();
    path.push(if cfg!(windows) { "node.exe" } else { "node" });
    assert!(
        path.is_file(),
        "expected a built node binary at {}; run `cargo build -p node` first",
        path.display()
    );
    path
}

struct ProcessNode {
    rpc: SocketAddr,
    child: Child,
}

impl Drop for ProcessNode {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn rpc_call(address: SocketAddr, method: &str, params: &Value) -> Result<Value, std::io::Error> {
    let body = serde_json::json!({"jsonrpc": "2.0", "method": method, "params": params, "id": 1})
        .to_string();
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(200))?;
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    write!(
        stream,
        "POST / HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let body = response.split_once("\r\n\r\n").map_or("", |(_, body)| body);
    serde_json::from_str(body).map_err(std::io::Error::other)
}

fn signed_mutation(sender: &Sender, nonce: u64, data: u8) -> Transaction {
    let executable = ExecutableTransaction {
        operation: MoveOperation::MutateObject {
            sender: sender.address,
            id: sender.object.id,
            expected_version: 0,
            replacement: Object {
                version: 0,
                data: vec![data],
                ..sender.object.clone()
            },
        },
        object_references: vec![DeclaredObjectRef {
            id: sender.object.id,
            owner: Owner::Single(sender.address),
            access: AccessMode::Write,
        }],
        compute_limit: 1_000,
    };
    let mut transaction = Transaction {
        sender: sender.address,
        nonce,
        payload: bcs::to_bytes(&executable).unwrap(),
        scheme_id: 1,
        public_key: sender.public_key.clone(),
        signature: Vec::new(),
    };
    transaction.signature = Ed25519Scheme
        .sign(&sender.key, &transaction.signing_message())
        .unwrap();
    transaction
}

fn fixture_genesis(
    validator_count: u8,
    initial_objects: Vec<Object>,
) -> (GenesisDocument, Vec<Vec<u8>>, Vec<identity::Keypair>) {
    let scheme = Bls12381Scheme;
    let mut keys = Vec::new();
    let mut gossip_identities = Vec::new();
    let validators = (1..=validator_count)
        .map(|index| {
            let key = vec![index; 32];
            let public_key = scheme.public_key(&key).unwrap();
            keys.push(key.clone());
            let gossip_identity = identity::Keypair::ed25519_from_bytes([index; 32]).unwrap();
            let gossip_peer_id = gossip_identity.public().to_peer_id().to_string();
            gossip_identities.push(gossip_identity);
            GenesisValidator {
                name: format!("validator-{index}"),
                validator: Validator {
                    id: Hash::digest([index]),
                    stake: 20,
                    public_key,
                    proof_of_possession: scheme.proof_of_possession(&key).unwrap(),
                },
                network_address: reserve_address().to_string(),
                rpc_address: reserve_address().to_string(),
                gossip_peer_id,
                gossip_address: format!("/ip4/127.0.0.1/tcp/{}", reserve_address().port()),
            }
        })
        .collect();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    (
        GenesisDocument {
            format_version: GENESIS_FORMAT_VERSION,
            chain_id: "kestrel-measure-steady-state".to_owned(),
            genesis_unix_ms: u64::try_from(now).unwrap() + 3_000,
            blocks_per_epoch: 100,
            state_config: state::StateConfig::default(),
            active_signature_schemes: vec![1, 2],
            equivocation_slash_basis_points: 5_000,
            validators,
            initial_objects,
            initial_fee_balances: BTreeMap::new(),
        },
        keys,
        gossip_identities,
    )
}

fn reserve_address() -> SocketAddr {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
}
