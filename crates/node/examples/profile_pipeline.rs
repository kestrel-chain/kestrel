//! Decomposes end-to-end TPS into its actual stages instead of reporting one
//! opaque number: how many blocks were needed, how many transactions landed
//! in each one (batching), each block's pure consensus-round cost
//! (`finality_latency_ms`, the same figure `measure_finality` reports), and
//! the wall-clock overhead beyond that round cost (admission/gossip wait,
//! execution, durable commit, and this tool's own polling granularity, all
//! of which this measurement cannot cleanly separate from each other).
//!
//! Topology, stated explicitly since it changes what can be concluded: four
//! separate real `node` OS processes, each running the actual production
//! `ConsensusCoordinator` (raw-TCP BLS votes/certificates) and `Stage2Pipeline`
//! (real libp2p transaction gossip + `KestrelCast` shred relay), each with its
//! own genuine BLS keypair and equal stake (20 each, 80 total) — a real 4-way
//! quorum, not a single validator with no consensus to run. All four are on
//! one machine over loopback, so this still is not a Stage 2 result (see
//! `docs/TECH_DEBT.md` TD-003).
//!
//! Every transaction in one run is submitted to the SAME validator (not
//! round-robined across all four, unlike `measure_tps`), specifically so
//! batching can be read cleanly off one node's own admission/proposal
//! behavior without also mixing in cross-node admission-timing variance.
//!
//! Runs `REPETITIONS` independent repetitions of the same fixed
//! configuration and reports mean/stddev/95% CI, not a single-run point
//! estimate.
//!
//! Usage: `cargo run --release -p node --example profile_pipeline [VALIDATOR_COUNT] [TRANSACTION_COUNT] [REPETITIONS]`

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

struct Sender {
    key: [u8; 32],
    public_key: Vec<u8>,
    address: Address,
    object: Object,
}

struct RepetitionResult {
    end_to_end_secs: f64,
    /// (height, `transaction_count`) for every block this run's leader built,
    /// parsed from its own tracing log.
    blocks: Vec<(u64, usize)>,
    /// (height, `finality_latency_ms`, `wall_clock_overhead_ms`) per height this
    /// validator observed committing, where `wall_clock_overhead_ms` is the
    /// time between this height's first observed commit and the previous
    /// one, minus this height's own reported consensus-round cost.
    height_timings: Vec<(u64, u64, f64)>,
    /// For each transaction and each of the three non-originating
    /// validators: the wall-clock gap, in milliseconds, between that
    /// transaction's admission on the submitting node and its admission on
    /// that other validator (via gossip). This is the actual
    /// admission-to-remote-mempool latency, measured directly from each
    /// process's own tracing log timestamps, not inferred from block timing.
    propagation_latencies_ms: Vec<f64>,
    /// For the height immediately after `baseline_height` (the height that
    /// is reliably observed empty): the gap, in milliseconds, between when
    /// that height's leader built its proposal and when the earliest
    /// transaction of this run's batch had actually reached that same
    /// leader's local mempool (via gossip, since the batch is submitted only
    /// to node 0). Negative means the leader built its (empty) proposal
    /// before any transaction could possibly have arrived -- a pure timing
    /// race, not a gossip-speed problem. Positive would mean a transaction
    /// had already arrived before the build yet was still excluded, pointing
    /// to a selection/admission bug instead. `None` if this height/leader
    /// pair could not be identified for this repetition.
    first_block_leader_lead_ms: Option<f64>,
}

#[allow(clippy::too_many_lines)] // Keep the process-spawn/measure/report timeline auditable.
#[allow(clippy::cast_precision_loss)] // Counts here are small test-run sizes, not chain data.
fn main() {
    let arguments = env::args().collect::<Vec<_>>();
    let validator_count: u8 = arguments
        .get(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let transaction_count: usize = arguments
        .get(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(50);
    let repetitions: usize = arguments
        .get(3)
        .and_then(|value| value.parse().ok())
        .unwrap_or(15);

    println!(
        "topology: {validator_count} separate real node processes, real raw-TCP consensus \
         (BLS votes/certificates) + real libp2p transaction gossip/KestrelCast, equal stake, \
         all over loopback -- a genuine {validator_count}-way quorum, not a single validator."
    );
    println!(
        "config: {transaction_count} transactions, all submitted to one validator, \
         {repetitions} repetitions\n"
    );

    let mut results = Vec::with_capacity(repetitions);
    for repetition in 1..=repetitions {
        print!("repetition {repetition}/{repetitions}... ");
        std::io::stdout().flush().unwrap();
        let result = run_once(validator_count, transaction_count);
        println!(
            "{:.2}s ({:.1} tx/s), {} block(s)",
            result.end_to_end_secs,
            transaction_count as f64 / result.end_to_end_secs,
            result.blocks.len()
        );
        results.push(result);
    }

    println!();
    report(&results, transaction_count);
}

#[allow(clippy::too_many_lines)] // Keep the process-spawn/measure/parse timeline auditable.
fn run_once(validator_count: u8, transaction_count: usize) -> RepetitionResult {
    let directory = TempDir::new().unwrap();
    let senders = (0..transaction_count)
        .map(|index| {
            let key = *Hash::digest(index.to_le_bytes()).as_bytes();
            let public_key = Ed25519Scheme.public_key(&key).unwrap();
            let address = Ed25519Scheme.address(&public_key).unwrap();
            let object = Object {
                id: Hash::digest(format!("profile-object-{index}").as_bytes()),
                owner: Owner::Single(address),
                type_tag: "profile_pipeline::Object".to_owned(),
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
    // Every validator's own log is needed: each height is proposed by
    // whichever validator leads it (round-robin), so a block's transaction
    // count only ever appears in that one leader's log, never in the
    // submitting node's log for heights it didn't lead.
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
            .env("RUST_LOG", "node=trace")
            .stdout(Stdio::from(fs::File::create(&log_paths[index]).unwrap()))
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        children.push(ProcessNode {
            rpc: entry.rpc_address.parse().unwrap(),
            child,
        });
    }

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

    // The chain produces blocks (possibly empty ones) continuously from
    // genesis, independent of whether anything has been submitted yet.
    // Baselining against whatever height already exists right before
    // submission -- rather than 0 -- keeps any such pre-submission backlog
    // out of both the block count and the timing of the first newly
    // observed block.
    let baseline_height = rpc_call(children[0].rpc, "kestrel_getStatus", &Value::Null)
        .ok()
        .and_then(|status| status["result"]["finalizedHeight"].as_u64())
        .unwrap_or(0);

    let submit_start = Instant::now();
    for sender in &senders {
        let transaction = signed_mutation(sender, 0, 1);
        let encoded = hex::encode(bcs::to_bytes(&transaction).unwrap());
        rpc_call(
            children[0].rpc,
            "kestrel_submitTransaction",
            &serde_json::json!({ "transaction": encoded }),
        )
        .unwrap();
    }

    let mut height_timings = Vec::new();
    let mut last_height = baseline_height;
    let mut last_commit_wall_time = submit_start;
    let commit_deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let committed = senders.iter().all(|sender| {
            rpc_call(
                children[0].rpc,
                "kestrel_getObject",
                &serde_json::json!({ "id": sender.object.id.to_string() }),
            )
            .is_ok_and(|response| response["result"]["data"] == "01")
        });
        if let Ok(status) = rpc_call(children[0].rpc, "kestrel_getStatus", &Value::Null) {
            let result = &status["result"];
            if let (Some(height), Some(latency)) = (
                result["finalizedHeight"].as_u64(),
                result["finalityLatencyMs"].as_u64(),
            ) && height > last_height
            {
                let now = Instant::now();
                let wall_gap_ms = now.duration_since(last_commit_wall_time).as_secs_f64() * 1000.0;
                // latency is a consensus-round millisecond count, far below f64's exact-integer range.
                #[allow(clippy::cast_precision_loss)]
                let overhead_ms = wall_gap_ms - latency as f64;
                height_timings.push((height, latency, overhead_ms));
                last_height = height;
                last_commit_wall_time = now;
            }
        }
        if committed {
            break;
        }
        assert!(
            Instant::now() < commit_deadline,
            "not all transactions committed before the deadline"
        );
        thread::sleep(Duration::from_millis(3));
    }
    let end_to_end_secs = submit_start.elapsed().as_secs_f64();
    let final_view_changes = rpc_call(children[0].rpc, "kestrel_getStatus", &Value::Null)
        .ok()
        .and_then(|status| status["result"]["viewChanges"].as_u64());

    drop(children); // Drop impl kills every child process.

    // Merge every validator's own log: whichever one led a given height is
    // the only one that logged a "built new block proposal" for it. Only
    // heights strictly after the pre-submission baseline are this run's own
    // blocks -- earlier ones are background empty blocks from before
    // anything was submitted.
    let mut by_height = BTreeMap::new();
    // height -> (leader node index, wall-clock build timestamp in micros).
    let mut block_builds: BTreeMap<u64, (usize, f64)> = BTreeMap::new();
    for (node_index, log_path) in log_paths.iter().enumerate() {
        for line in fs::read_to_string(log_path).unwrap_or_default().lines() {
            let Ok(parsed) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if parsed["fields"]["message"] == "built new block proposal" {
                let height = parsed["fields"]["height"].as_u64().unwrap();
                let count =
                    usize::try_from(parsed["fields"]["transaction_count"].as_u64().unwrap())
                        .unwrap();
                if height > baseline_height && height <= last_height {
                    by_height.insert(height, count);
                    if let Some(timestamp) = parsed["timestamp"].as_str()
                        && let Some(micros) = parse_timestamp_micros(timestamp)
                    {
                        block_builds.insert(height, (node_index, micros));
                    }
                }
            }
        }
    }
    let blocks = by_height.into_iter().collect::<Vec<_>>();

    // For each transaction, the wall-clock timestamp (microseconds since
    // midnight UTC -- fine for within-run deltas) each node's own log
    // recorded admitting it. Node 0 is where every transaction was
    // submitted directly, so its timestamp for a given ID is the admission
    // origin; the other three nodes' timestamps for the same ID are how
    // long gossip actually took to reach them.
    let mut admissions: BTreeMap<String, BTreeMap<usize, f64>> = BTreeMap::new();
    for (node_index, log_path) in log_paths.iter().enumerate() {
        for line in fs::read_to_string(log_path).unwrap_or_default().lines() {
            let Ok(parsed) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if parsed["fields"]["message"] == "admitted transaction"
                && let Some(id) = parsed["fields"]["transaction_id"].as_str()
                && let Some(timestamp) = parsed["timestamp"].as_str()
                && let Some(micros) = parse_timestamp_micros(timestamp)
            {
                admissions
                    .entry(id.to_owned())
                    .or_default()
                    .insert(node_index, micros);
            }
        }
    }
    let propagation_latencies_ms = admissions
        .values()
        .filter_map(|by_node| {
            let origin = *by_node.get(&0)?;
            Some(
                by_node
                    .iter()
                    .filter(|&(&node_index, _)| node_index != 0)
                    .map(move |(_, &timestamp)| (timestamp - origin) / 1000.0),
            )
        })
        .flatten()
        .collect::<Vec<_>>();

    // The reliably-empty height is the first one after baseline_height. Find
    // when its leader built its proposal, and the earliest moment any
    // transaction of this batch had reached that same leader's own mempool.
    let first_block_leader_lead_ms =
        block_builds
            .get(&(baseline_height + 1))
            .and_then(|&(leader_node, build_micros)| {
                let earliest_admission_micros = admissions
                    .values()
                    .filter_map(|by_node| by_node.get(&leader_node).copied())
                    .fold(f64::INFINITY, f64::min);
                earliest_admission_micros
                    .is_finite()
                    .then(|| (build_micros - earliest_admission_micros) / 1000.0)
            });

    eprintln!(
        "    [debug] baseline_height={baseline_height} final_height={last_height} view_changes={final_view_changes:?} blocks={blocks:?} first_block_leader_lead_ms={first_block_leader_lead_ms:?}"
    );

    RepetitionResult {
        end_to_end_secs,
        blocks,
        height_timings,
        propagation_latencies_ms,
        first_block_leader_lead_ms,
    }
}

#[allow(clippy::cast_precision_loss)] // transaction/block counts are small test-run sizes, not chain data.
fn report(results: &[RepetitionResult], transaction_count: usize) {
    let tps_values = results
        .iter()
        .map(|result| transaction_count as f64 / result.end_to_end_secs)
        .collect::<Vec<_>>();
    let (tps_mean, tps_ci) = mean_and_95_ci(&tps_values);

    let block_counts = results
        .iter()
        .map(|result| result.blocks.len() as f64)
        .collect::<Vec<_>>();
    let (blocks_mean, blocks_ci) = mean_and_95_ci(&block_counts);

    let tx_per_block = results
        .iter()
        .flat_map(|result| result.blocks.iter().map(|(_, count)| *count as f64))
        .collect::<Vec<_>>();
    let (tx_per_block_mean, tx_per_block_ci) = mean_and_95_ci(&tx_per_block);
    let tx_per_block_min = tx_per_block.iter().copied().fold(f64::INFINITY, f64::min);
    let tx_per_block_max = tx_per_block
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);

    let consensus_latencies = results
        .iter()
        .flat_map(|result| {
            result
                .height_timings
                .iter()
                .map(|(_, latency, _)| *latency as f64)
        })
        .collect::<Vec<_>>();
    let (consensus_mean, consensus_ci) = mean_and_95_ci(&consensus_latencies);

    let overheads = results
        .iter()
        .flat_map(|result| {
            result
                .height_timings
                .iter()
                .map(|(_, _, overhead)| *overhead)
        })
        .collect::<Vec<_>>();
    let (overhead_mean, overhead_ci) = mean_and_95_ci(&overheads);

    println!(
        "=== {} repetitions, {transaction_count} transactions each ===",
        results.len()
    );
    println!(
        "end-to-end throughput:      {tps_mean:.1} tx/s  (95% CI +/- {tps_ci:.1}, n={})",
        tps_values.len()
    );
    println!("blocks needed per run:      {blocks_mean:.1}  (95% CI +/- {blocks_ci:.1})");
    println!(
        "transactions per block:     mean {tx_per_block_mean:.1} (95% CI +/- {tx_per_block_ci:.1}), \
         min {tx_per_block_min:.0}, max {tx_per_block_max:.0}, n={} blocks observed",
        tx_per_block.len()
    );
    println!(
        "consensus round cost/block: {consensus_mean:.1}ms (95% CI +/- {consensus_ci:.1}ms) \
         -- this is `finality_latency_ms`, the pure BFT voting round"
    );
    println!(
        "non-consensus overhead/block: {overhead_mean:.1}ms (95% CI +/- {overhead_ci:.1}ms) \
         -- wall-clock time per block MINUS its consensus round cost; conflates admission/gossip \
         wait, execution, durable commit, and this tool's own polling granularity -- NOT a clean \
         single-stage measurement"
    );

    let propagation = results
        .iter()
        .flat_map(|result| result.propagation_latencies_ms.iter().copied())
        .collect::<Vec<_>>();
    let (propagation_mean, propagation_ci) = mean_and_95_ci(&propagation);
    let propagation_min = propagation.iter().copied().fold(f64::INFINITY, f64::min);
    let propagation_max = propagation
        .iter()
        .copied()
        .fold(f64::NEG_INFINITY, f64::max);
    println!(
        "admission-to-remote-mempool latency: {propagation_mean:.1}ms (95% CI +/- {propagation_ci:.1}ms), \
         min {propagation_min:.1}ms, max {propagation_max:.1}ms, n={} (sender x remote-validator pairs) \
         -- measured directly from each process's own tracing log timestamp for the same transaction ID, \
         not inferred from block timing",
        propagation.len()
    );

    let leader_lead = results
        .iter()
        .filter_map(|result| result.first_block_leader_lead_ms)
        .collect::<Vec<_>>();
    let (leader_lead_mean, leader_lead_ci) = mean_and_95_ci(&leader_lead);
    let built_before_arrival = leader_lead.iter().filter(|&&gap| gap < 0.0).count();
    println!(
        "first-block leader build vs. earliest local arrival: {leader_lead_mean:.1}ms (95% CI +/- \
         {leader_lead_ci:.1}ms), n={} repetitions -- build_timestamp minus earliest transaction \
         admission timestamp on that same leader; negative means the leader built its (empty) \
         proposal before any transaction of the batch could possibly have reached it; \
         {built_before_arrival}/{} repetitions were negative",
        leader_lead.len(),
        leader_lead.len()
    );
}

/// Sample mean and half-width of a 95% CI (normal approximation; fine for the
/// sample sizes here, not claiming small-sample exactness).
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

/// Parses the time-of-day portion of an RFC3339 timestamp like
/// "2026-07-23T16:24:55.663065Z" into microseconds since midnight UTC. Only
/// used for within-run deltas over a fraction of a second, so day-boundary
/// wraparound is not a concern.
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
            chain_id: "kestrel-profile-pipeline".to_owned(),
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
