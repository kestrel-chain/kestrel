#![cfg(unix)]
//! Proves the shipped `node` binary itself — not library code — runs the real
//! Stage 2 production pipeline end to end across separate OS processes: a
//! transaction submitted through the public `kestrel_submitTransaction` JSON-RPC
//! method on one validator process is admitted, gossiped over real libp2p
//! sockets, ordered by real consensus, executed, and durably committed, and
//! every other process's `kestrel_getObject` reflects the identical result.
//! `tests/stage_2_processes.rs` proves the same binary's Byzantine-fault
//! safety/liveness; this test proves its real transaction-processing path.

use std::{
    fs,
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

const VALIDATOR_COUNT: u8 = 4;

#[test]
#[allow(clippy::too_many_lines)] // Keep the full process-spawn/submit/verify timeline auditable.
fn node_binary_commits_an_rpc_submitted_transaction_across_all_processes() {
    let directory = TempDir::new().unwrap();
    let account_key = [42_u8; 32];
    let account_public_key = Ed25519Scheme.public_key(&account_key).unwrap();
    let owner = Ed25519Scheme.address(&account_public_key).unwrap();
    let target = Object {
        id: Hash::digest([5_u8, 5, 5]),
        owner: Owner::Single(owner),
        type_tag: "stage2::RpcObject".to_owned(),
        version: 0,
        data: vec![0],
        rent_balance: 1_000,
    };

    let (genesis, keys, gossip_identities) = fixture_genesis(target.clone());
    genesis
        .write_json(directory.path().join("genesis.json"))
        .unwrap();
    let genesis_path = directory.path().join("genesis.json");

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
        let child = Command::new(env!("CARGO_BIN_EXE_node"))
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

    // Generous under CI/full-workspace parallel load: each process still has to
    // finish tokio/libp2p/RocksDB startup, form the gossip mesh, and finalize
    // and commit its first (possibly empty) block before it reports ready.
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

    let transaction = signed_mutation(&account_key, &account_public_key, owner, 0, &target, 0, 77);
    let encoded = hex::encode(bcs::to_bytes(&transaction).unwrap());
    let submit = rpc_call(
        children[0].rpc,
        "kestrel_submitTransaction",
        &serde_json::json!({ "transaction": encoded }),
    )
    .unwrap();
    assert!(
        submit.get("error").is_none(),
        "transaction submission was rejected: {submit:?}"
    );

    let commit_deadline = Instant::now() + Duration::from_secs(45);
    loop {
        let committed = children
            .iter()
            .filter_map(|node| {
                rpc_call(
                    node.rpc,
                    "kestrel_getObject",
                    &serde_json::json!({ "id": target.id.to_string() }),
                )
                .ok()
            })
            .filter(|response| response["result"]["data"] == "4d")
            .count();
        if committed == children.len() {
            break;
        }
        assert!(
            Instant::now() < commit_deadline,
            "the RPC-submitted transaction did not commit on every process in time"
        );
        thread::sleep(Duration::from_millis(100));
    }

    for node in &mut children {
        if node.child.try_wait().unwrap().is_none() {
            node.child.kill().unwrap();
            node.child.wait().unwrap();
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
    initial_object: Object,
) -> (GenesisDocument, Vec<Vec<u8>>, Vec<identity::Keypair>) {
    let scheme = Bls12381Scheme;
    let mut keys = Vec::new();
    let mut gossip_identities = Vec::new();
    let validators = (1..=VALIDATOR_COUNT)
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
            chain_id: "kestrel-stage2-node-rpc-test".to_owned(),
            genesis_unix_ms: u64::try_from(now).unwrap() + 3_000,
            blocks_per_epoch: 100,
            state_config: state::StateConfig::default(),
            active_signature_schemes: vec![1, 2],
            equivocation_slash_basis_points: 5_000,
            validators,
            initial_objects: vec![initial_object],
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
