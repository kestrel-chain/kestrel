//! Runs real `node` binary processes on loopback and reports the finality
//! latency the production consensus coordinator itself measures for every
//! block it finalizes (`NodeStatus::finality_latency_ms`, the same number
//! `/metrics` and `kestrel_getStatus` expose to an operator).
//!
//! This is a genuine end-to-end measurement of the shipped binary -- not a
//! `ConsensusSimulator` estimate -- but it is still one machine over
//! loopback, not the real-geography, real-latency exercise Stage 2 requires
//! (see `docs/TECH_DEBT.md` TD-003). Point the same technique at real separate
//! hosts for that; this is meant as a first, honest local data point and a
//! template for the real one.
//!
//! Usage: `cargo run --release -p node --example measure_finality [VALIDATOR_COUNT] [DURATION_SECS]`

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

#[allow(clippy::too_many_lines)] // Keep the process-spawn/measure/report timeline auditable.
fn main() {
    let arguments = env::args().collect::<Vec<_>>();
    let validator_count: u8 = arguments
        .get(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(4);
    let duration_secs: u64 = arguments
        .get(2)
        .and_then(|value| value.parse().ok())
        .unwrap_or(30);

    let directory = TempDir::new().unwrap();
    let account_key = [77_u8; 32];
    let account_public_key = Ed25519Scheme.public_key(&account_key).unwrap();
    let owner = Ed25519Scheme.address(&account_public_key).unwrap();
    let target = Object {
        id: Hash::digest([1_u8, 2, 3]),
        owner: Owner::Single(owner),
        type_tag: "measure_finality::Object".to_owned(),
        version: 0,
        data: vec![0],
        rent_balance: 1_000,
    };

    let (genesis, keys, gossip_identities) = fixture_genesis(validator_count, target.clone());
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
            .stdout(Stdio::null())
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

    println!(
        "all nodes ready; submitting one real signed transaction to confirm the pipeline, \
         then measuring steady-state block time for {duration_secs}s..."
    );
    let transaction = signed_mutation(&account_key, &account_public_key, owner, 0, &target, 0, 9);
    let encoded = hex::encode(bcs::to_bytes(&transaction).unwrap());
    let submit = rpc_call(
        children[0].rpc,
        "kestrel_submitTransaction",
        &serde_json::json!({ "transaction": encoded }),
    )
    .unwrap();
    if submit.get("error").is_some() {
        eprintln!("warning: transaction submission was rejected: {submit:?}");
    }

    let mut samples = Vec::<(u64, u64)>::new();
    let mut last_height = 0_u64;
    let run_deadline = Instant::now() + Duration::from_secs(duration_secs);
    while Instant::now() < run_deadline {
        if let Ok(status) = rpc_call(children[0].rpc, "kestrel_getStatus", &Value::Null) {
            let result = &status["result"];
            if let (Some(height), Some(latency)) = (
                result["finalizedHeight"].as_u64(),
                result["finalityLatencyMs"].as_u64(),
            ) && height > last_height
            {
                println!("  height {height}: {latency} ms");
                samples.push((height, latency));
                last_height = height;
            }
        }
        thread::sleep(Duration::from_millis(20));
    }

    shutdown(&mut children);

    if samples.is_empty() {
        println!("no blocks finalized in the measurement window");
        return;
    }
    let mut latencies = samples
        .iter()
        .map(|(_, latency)| *latency)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let count = latencies.len();
    let sum = latencies.iter().sum::<u64>();
    let mean = sum / count as u64;
    let p50 = latencies[count / 2];
    let p90 = latencies[(count * 9 / 10).min(count - 1)];
    println!();
    println!(
        "=== finality latency over {count} blocks ({validator_count} validators, real node binary, loopback) ==="
    );
    println!(
        "min={}ms p50={p50}ms mean={mean}ms p90={p90}ms max={}ms",
        latencies[0],
        latencies[count - 1]
    );
    println!(
        "NOTE: one machine over loopback, not real geography -- see docs/TECH_DEBT.md TD-003 \
         before treating this as a Stage 2 result."
    );
}

/// Locates the sibling `node` binary from this example's own executable path
/// (`target/<profile>/examples/measure_finality` -> `target/<profile>/node`),
/// since `CARGO_BIN_EXE_node` is only injected for integration tests/benches,
/// not examples.
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

fn signed_mutation(
    private_key: &[u8],
    public_key: &[u8],
    sender: Address,
    nonce: u64,
    object: &Object,
    expected_version: u64,
    data: u8,
) -> Transaction {
    let executable = ExecutableTransaction {
        operation: MoveOperation::MutateObject {
            sender,
            id: object.id,
            expected_version,
            replacement: Object {
                version: expected_version,
                data: vec![data],
                ..object.clone()
            },
        },
        object_references: vec![DeclaredObjectRef {
            id: object.id,
            owner: Owner::Single(sender),
            access: AccessMode::Write,
        }],
        compute_limit: 1_000,
    };
    let mut transaction = Transaction {
        sender,
        nonce,
        payload: bcs::to_bytes(&executable).unwrap(),
        scheme_id: 1,
        public_key: public_key.to_vec(),
        signature: Vec::new(),
    };
    transaction.signature = Ed25519Scheme
        .sign(private_key, &transaction.signing_message())
        .unwrap();
    transaction
}

fn fixture_genesis(
    validator_count: u8,
    initial_object: Object,
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
            chain_id: "kestrel-measure-finality".to_owned(),
            genesis_unix_ms: u64::try_from(now).unwrap() + 3_000,
            blocks_per_epoch: 100,
            state_config: state::StateConfig::default(),
            active_signature_schemes: vec![1, 2],
            equivocation_slash_basis_points: 5_000,
            validators,
            initial_objects: vec![initial_object],
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
