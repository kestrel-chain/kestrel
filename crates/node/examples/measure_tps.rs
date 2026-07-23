//! Measures sustained transaction throughput using real `node` binary
//! processes: `TRANSACTION_COUNT` independent senders, each owning its own
//! object, submit one transaction each as fast as this process can send
//! them, and TPS is computed from first submission to the last transaction
//! committing durably on every validator.
//!
//! Independent senders (rather than one sender issuing many transactions)
//! are deliberate: the admission path only allows one outstanding
//! uncommitted transaction per sender at a time, and per-sender/per-object
//! fee scopes are exactly what lets the mempool schedule unrelated senders
//! in parallel (see `docs/mempool-spec.md`) -- a single-sender loop would
//! measure nonce-ordering latency, not throughput.
//!
//! Same caveat as `measure_finality.rs`: a genuine measurement of the real
//! production binary, but one machine over loopback -- not the real-geography
//! exercise Stage 2 requires (see `docs/TECH_DEBT.md` TD-003).
//!
//! Usage: `cargo run --release -p node --example measure_tps [VALIDATOR_COUNT] [TRANSACTION_COUNT]`

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

#[allow(clippy::too_many_lines)] // Keep the process-spawn/measure/report timeline auditable.
#[allow(clippy::cast_precision_loss)] // transaction_count is a small test-run size, not chain data.
fn main() {
    let arguments = env::args().collect::<Vec<_>>();
    let validator_count: u8 = arguments
        .get(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let transaction_count: usize = arguments
        .get(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(500);

    println!("generating {transaction_count} independent sender/object pairs...");
    let senders = (0..transaction_count)
        .map(|index| {
            let key = *Hash::digest(index.to_le_bytes()).as_bytes();
            let public_key = Ed25519Scheme.public_key(&key).unwrap();
            let address = Ed25519Scheme.address(&public_key).unwrap();
            let object = Object {
                id: Hash::digest(format!("tps-object-{index}").as_bytes()),
                owner: Owner::Single(address),
                type_tag: "measure_tps::Object".to_owned(),
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

    let directory = TempDir::new().unwrap();
    let (genesis, keys, gossip_identities) = fixture_genesis(
        validator_count,
        senders.iter().map(|sender| sender.object.clone()).collect(),
    );
    let genesis_path = directory.path().join("genesis.json");
    genesis.write_json(&genesis_path).unwrap();

    let node_binary = node_binary_path();
    println!(
        "spawning {validator_count} real node processes on loopback ({})...",
        node_binary.display()
    );
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
        let log_path = directory.path().join(format!("node-{index}.log"));
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
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(fs::File::create(&log_path).unwrap()))
            .stderr(Stdio::from(
                fs::File::create(directory.path().join(format!("node-{index}.err.log"))).unwrap(),
            ))
            .spawn()
            .unwrap();
        children.push(ProcessNode {
            rpc: entry.rpc_address.parse().unwrap(),
            child,
        });
    }
    println!("node logs: {}", directory.path().display());
    // Keep the temp directory alive past this point for post-run inspection.
    let directory = directory.keep();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let wait =
        u64::try_from(u128::from(genesis.genesis_unix_ms).saturating_sub(now)).unwrap_or_default();
    thread::sleep(Duration::from_millis(wait.saturating_add(500)));

    println!("waiting for all nodes to report ready...");
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
        if Instant::now() >= ready_deadline {
            eprintln!("not all node processes became ready in time; shutting down");
            shutdown(&mut children);
            std::process::exit(1);
        }
        thread::sleep(Duration::from_millis(50));
    }

    println!("submitting {transaction_count} independent transactions as fast as possible...");
    let submit_start = Instant::now();
    for (index, sender) in senders.iter().enumerate() {
        let transaction = signed_mutation(sender, 0, 1);
        let encoded = hex::encode(bcs::to_bytes(&transaction).unwrap());
        let target_validator = &children[index % children.len()];
        if let Err(error) = rpc_call(
            target_validator.rpc,
            "kestrel_submitTransaction",
            &serde_json::json!({ "transaction": encoded }),
        ) {
            eprintln!("submission {index} failed: {error}");
        }
    }
    let submit_elapsed = submit_start.elapsed();
    println!(
        "all {transaction_count} submissions sent in {:.2}s ({:.0} submit/s); now waiting for durable commit on every validator...",
        submit_elapsed.as_secs_f64(),
        transaction_count as f64 / submit_elapsed.as_secs_f64()
    );

    let commit_deadline = Instant::now() + Duration::from_secs(25);
    let mut last_report = Instant::now();
    loop {
        let committed = senders
            .iter()
            .filter(|sender| {
                children.iter().all(|node| {
                    rpc_call(
                        node.rpc,
                        "kestrel_getObject",
                        &serde_json::json!({ "id": sender.object.id.to_string() }),
                    )
                    .is_ok_and(|response| response["result"]["data"] == "01")
                })
            })
            .count();
        if committed == transaction_count {
            break;
        }
        if last_report.elapsed() >= Duration::from_secs(2) {
            println!(
                "  {committed}/{transaction_count} committed on every validator ({:.1}s elapsed)",
                submit_start.elapsed().as_secs_f64()
            );
            last_report = Instant::now();
        }
        assert!(
            Instant::now() < commit_deadline,
            "only {committed}/{transaction_count} committed on every validator before the deadline"
        );
        thread::sleep(Duration::from_millis(50));
    }
    let total_elapsed = submit_start.elapsed();

    shutdown(&mut children);

    println!();
    println!(
        "=== TPS over {transaction_count} transactions ({validator_count} validators, real node binary, loopback) ==="
    );
    println!(
        "submit-to-fully-committed: {:.2}s -> {:.1} tx/s",
        total_elapsed.as_secs_f64(),
        transaction_count as f64 / total_elapsed.as_secs_f64()
    );
    println!(
        "NOTE: one machine over loopback, not real geography -- see docs/TECH_DEBT.md TD-003 \
         before treating this as a Stage 2 result. Node logs kept at {}",
        directory.display()
    );
}

fn node_binary_path() -> std::path::PathBuf {
    let mut path = env::current_exe().unwrap();
    path.pop(); // examples/
    path.pop(); // <profile>/
    path.push(if cfg!(windows) { "node.exe" } else { "node" });
    assert!(
        path.is_file(),
        "expected a built node binary at {}; run `cargo build -p node` first",
        path.display()
    );
    path
}

fn shutdown(children: &mut [ProcessNode]) {
    for node in children {
        if node.child.try_wait().ok().flatten().is_none() {
            let _ = node.child.kill();
            let _ = node.child.wait();
        }
    }
}

struct ProcessNode {
    rpc: SocketAddr,
    child: Child,
}

// A failed `assert!` (the commit-deadline check, or a mismatch) unwinds past
// any explicit `shutdown()` call. Without this, every failed run leaks four
// real node processes that keep running indefinitely, competing for CPU/ports
// with every subsequent run -- exactly the kind of self-inflicted resource
// contention that can masquerade as a protocol bug in a later run.
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
            chain_id: "kestrel-measure-tps".to_owned(),
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
