#![cfg(unix)]

use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use consensus::Validator;
use crypto::{Bls12381Scheme, SignatureScheme};
use node::{GENESIS_FORMAT_VERSION, GenesisDocument, GenesisValidator};
use serde_json::Value;
use tempfile::TempDir;
use types::Hash;

#[test]
fn stage_two_processes_survive_killed_and_partitioned_leaders() {
    run_scenario(LeaderFault::Healthy, VoterFault::Honest);
    run_scenario(LeaderFault::Kill, VoterFault::Corrupt);
    run_scenario(LeaderFault::Partition, VoterFault::Withhold);
    run_scenario(LeaderFault::Equivocate, VoterFault::Honest);
}

#[derive(Clone, Copy, Debug)]
enum LeaderFault {
    Healthy,
    Kill,
    Partition,
    Equivocate,
}

#[derive(Clone, Copy, Debug)]
enum VoterFault {
    Honest,
    Corrupt,
    Withhold,
}

#[allow(clippy::too_many_lines)] // Keep each multi-process fault timeline auditable end to end.
fn run_scenario(leader_fault: LeaderFault, voter_fault: VoterFault) {
    let directory = TempDir::new().unwrap();
    let (mut genesis, keys, gossip_identities) = fixture_genesis();
    let initial = genesis.validate().unwrap();
    let leader = initial.validators.leader(1, 0).id;
    let byzantine = genesis
        .validators
        .iter()
        .map(|entry| entry.validator.id)
        .find(|id| *id != leader)
        .unwrap();
    genesis
        .validators
        .iter_mut()
        .find(|entry| entry.validator.id == byzantine)
        .unwrap()
        .validator
        .stake = 10;
    if matches!(leader_fault, LeaderFault::Equivocate) {
        genesis
            .validators
            .iter_mut()
            .find(|entry| entry.validator.id == leader)
            .unwrap()
            .validator
            .stake = 10;
    }
    genesis
        .validators
        .iter_mut()
        .find(|entry| entry.validator.id != leader && entry.validator.id != byzantine)
        .unwrap()
        .validator
        .stake = if matches!(leader_fault, LeaderFault::Equivocate) {
        40
    } else {
        30
    };
    let validated = genesis.validate().unwrap();
    assert_eq!(
        validated.validators.validator(leader).unwrap().stake,
        if matches!(leader_fault, LeaderFault::Equivocate) {
            10
        } else {
            20
        }
    );
    assert_eq!(validated.validators.validator(byzantine).unwrap().stake, 10);
    let genesis_path = directory.path().join("genesis.json");
    genesis.write_json(&genesis_path).unwrap();

    let all_ids = genesis
        .validators
        .iter()
        .map(|entry| entry.validator.id.to_string())
        .collect::<Vec<_>>()
        .join(",");
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
        let mut command = Command::new(env!("CARGO_BIN_EXE_node"));
        command
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
                "--stop-after-height",
                "1",
                "--delay-ms",
                "5",
                "--drop-bps",
                "5",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        if entry.validator.id == leader {
            if !matches!(leader_fault, LeaderFault::Healthy) {
                command.args(["--proposal-delay-ms", "500"]);
            }
            if matches!(leader_fault, LeaderFault::Partition) {
                command.args(["--blocked-peers", &all_ids]);
            }
            if matches!(leader_fault, LeaderFault::Equivocate) {
                command.arg("--equivocate");
            }
        }
        if entry.validator.id == byzantine {
            match voter_fault {
                VoterFault::Honest => {}
                VoterFault::Corrupt => {
                    command.arg("--corrupt-votes");
                }
                VoterFault::Withhold => {
                    command.arg("--withhold-votes");
                }
            }
        }
        children.push(ProcessNode {
            id: entry.validator.id,
            rpc: entry.rpc_address.parse().unwrap(),
            child: command.spawn().unwrap(),
        });
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let wait =
        u64::try_from(u128::from(genesis.genesis_unix_ms).saturating_sub(now)).unwrap_or_default();
    thread::sleep(Duration::from_millis(wait.saturating_add(100)));
    if matches!(leader_fault, LeaderFault::Kill) {
        let leader_process = children.iter_mut().find(|node| node.id == leader).unwrap();
        leader_process.child.kill().unwrap();
        leader_process.child.wait().unwrap();
    }

    // Generous on purpose: five real OS processes running BLS consensus in an
    // unoptimized build finish this in about a second when the machine is idle,
    // so a tight bound only ever fires on a slow or contended one, for reasons
    // unrelated to the fault being tested. The assertions after the wait still
    // verify the real behaviour.
    let deadline = Instant::now() + Duration::from_secs(60);
    let statuses = loop {
        let statuses = children
            .iter()
            .filter(|node| node.id != leader || matches!(leader_fault, LeaderFault::Healthy))
            .filter_map(|node| rpc_status(node.rpc).ok())
            .collect::<Vec<_>>();
        let expected = if matches!(leader_fault, LeaderFault::Healthy) {
            5
        } else {
            4
        };
        if statuses.len() == expected
            && statuses
                .iter()
                .all(|status| status["result"]["finalizedHeight"] == 1)
        {
            break statuses;
        }
        assert!(
            Instant::now() < deadline,
            "nodes did not finalize: {statuses:?}"
        );
        thread::sleep(Duration::from_millis(50));
    };
    let block = statuses[0]["result"]["finalizedBlock"].clone();
    assert!(
        statuses
            .iter()
            .all(|status| status["result"]["finalizedBlock"] == block)
    );
    let expected_view_changes = u64::from(!matches!(leader_fault, LeaderFault::Healthy));
    assert!(
        statuses
            .iter()
            .all(|status| status["result"]["viewChanges"] == expected_view_changes)
    );
    assert!(statuses.iter().all(|status| {
        status["result"]["finalityLatencyMs"]
            .as_u64()
            .is_some_and(|latency| latency < 2_000)
    }));
    let maximum_latency_ms = statuses
        .iter()
        .filter_map(|status| status["result"]["finalityLatencyMs"].as_u64())
        .max()
        .unwrap();
    println!(
        "leader_fault={leader_fault:?} voter_fault={voter_fault:?} nodes=5 finalized_nodes={} view_changes={expected_view_changes} max_finality_ms={maximum_latency_ms}",
        statuses.len()
    );

    for node in &mut children {
        if node.child.try_wait().unwrap().is_none() {
            node.child.kill().unwrap();
            node.child.wait().unwrap();
        }
    }
}

struct ProcessNode {
    id: Hash,
    rpc: SocketAddr,
    child: Child,
}

fn rpc_status(address: SocketAddr) -> Result<Value, std::io::Error> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_millis(100))?;
    stream.set_read_timeout(Some(Duration::from_millis(250)))?;
    let body = r#"{"jsonrpc":"2.0","method":"kestrel_getStatus","id":1}"#;
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

fn fixture_genesis() -> (
    GenesisDocument,
    Vec<Vec<u8>>,
    Vec<libp2p::identity::Keypair>,
) {
    let scheme = Bls12381Scheme;
    let mut keys = Vec::new();
    let mut gossip_identities = Vec::new();
    let validators = (1_u8..=5)
        .map(|index| {
            let key = vec![index; 32];
            let public_key = scheme.public_key(&key).unwrap();
            keys.push(key.clone());
            let gossip_identity =
                libp2p::identity::Keypair::ed25519_from_bytes([index; 32]).unwrap();
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
            chain_id: "kestrel-stage-2-process-test".to_owned(),
            genesis_unix_ms: u64::try_from(now).unwrap() + 1_500,
            blocks_per_epoch: 100,
            state_config: state::StateConfig::default(),
            active_signature_schemes: vec![1, 2],
            equivocation_slash_basis_points: 5_000,
            validators,
            initial_objects: Vec::new(),
            initial_fee_balances: BTreeMap::new(),
        },
        keys,
        gossip_identities,
    )
}

fn reserve_address() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}
