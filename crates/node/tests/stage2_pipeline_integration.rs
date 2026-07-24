//! Exercises the full Stage 2 production composition in-process: real libp2p
//! transaction gossip and `KestrelCast` shred exchange, real consensus ordering
//! sourced from each node's own mempool/relay state (no synthetic proposals),
//! deferred execution, and an atomic durable commit, converging to the same
//! state root on every node. This closes the "wiring live finality output to
//! payloads arriving over libp2p" gap left open after Phase 6 (see
//! `docs/TECH_DEBT.md` TD-002/TD-003): unlike `tests/stage_2_processes.rs`
//! (real sockets, synthetic per-height transaction IDs) and
//! `tests/block_lifecycle.rs` (a hand-fed `FinalizedOrder`/`PropagatedBlock`),
//! this test submits one real signed transaction through gossip and lets the
//! production `Stage2Pipeline` discover, propagate, order, execute, and
//! commit it end to end.

use std::{
    collections::BTreeMap,
    net::TcpListener,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use consensus::Validator;
use crypto::{Bls12381Scheme, Ed25519Scheme, SignatureScheme};
use execution::{AccessMode, DeclaredObjectRef, ExecutableTransaction, MoveOperation};
use libp2p::{Multiaddr, PeerId, identity};
use network::{ConfiguredPeer, GossipConfig, NetworkNode};
use node::{
    BlockLifecycle, ConsensusCoordinator, CoordinatorConfig, CoordinatorFaults,
    GENESIS_FORMAT_VERSION, GenesisDocument, GenesisValidator, Stage2Pipeline,
    Stage2PipelineConfig,
};
use rpc::NodeStatus;
use state::{StateConfig, StateTree};
use tempfile::TempDir;
use tokio::sync::mpsc;
use types::{Address, Hash, Object, Owner, Transaction};

const VALIDATOR_COUNT: usize = 4;

/// Records why a validator task ended, tolerating a poisoned mutex so one
/// panicking task cannot bury the original failure behind `PoisonError`.
fn record_crash(crashes: &Arc<std::sync::Mutex<Vec<String>>>, reason: String) {
    match crashes.lock() {
        Ok(mut guard) => guard.push(reason),
        Err(poisoned) => poisoned.into_inner().push(reason),
    }
}

struct Sender {
    key: [u8; 32],
    public_key: Vec<u8>,
    address: Address,
    object: Object,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)] // Keep the full multi-node wiring/assertion timeline auditable.
async fn stage2_pipeline_commits_a_gossiped_transaction_across_all_nodes() {
    let directory = TempDir::new().unwrap();
    let account_key = [7_u8; 32];
    let account_public_key = Ed25519Scheme.public_key(&account_key).unwrap();
    let owner = Ed25519Scheme.address(&account_public_key).unwrap();
    let target = Object {
        id: Hash::digest([9_u8, 9, 9]),
        owner: Owner::Single(owner),
        type_tag: "stage2::Object".to_owned(),
        version: 0,
        data: vec![0],
        rent_balance: 1_000,
    };

    let bls = Bls12381Scheme;
    let mut bls_keys = Vec::new();
    let mut libp2p_identities = Vec::new();
    let mut validators = Vec::new();
    let gossip_ports = (0..VALIDATOR_COUNT)
        .map(|_| reserve_port())
        .collect::<Vec<_>>();
    for index in 1..=VALIDATOR_COUNT {
        let key = vec![u8::try_from(index).unwrap(); 32];
        let public_key = bls.public_key(&key).unwrap();
        bls_keys.push(key.clone());
        let gossip_identity = identity::Keypair::generate_ed25519();
        let gossip_peer_id = gossip_identity.public().to_peer_id().to_string();
        libp2p_identities.push(gossip_identity);
        validators.push(GenesisValidator {
            name: format!("validator-{index}"),
            validator: Validator {
                id: Hash::digest([u8::try_from(index).unwrap()]),
                stake: 20,
                public_key,
                proof_of_possession: bls.proof_of_possession(&key).unwrap(),
            },
            network_address: reserve_socket_address(),
            rpc_address: reserve_socket_address(),
            gossip_peer_id,
            gossip_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[index - 1]),
        });
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let genesis = GenesisDocument {
        format_version: GENESIS_FORMAT_VERSION,
        chain_id: "kestrel-stage2-pipeline-test".to_owned(),
        genesis_unix_ms: u64::try_from(now).unwrap() + 1_500,
        blocks_per_epoch: 100,
        state_config: StateConfig::default(),
        active_signature_schemes: vec![1, 2],
        equivocation_slash_basis_points: 5_000,
        validators: validators.clone(),
        initial_objects: vec![target.clone()],
        initial_fee_balances: BTreeMap::new(),
    };
    let validated = genesis.validate().unwrap();

    let peer_ids = libp2p_identities
        .iter()
        .map(identity::Keypair::public)
        .map(|key| key.to_peer_id())
        .collect::<Vec<_>>();
    let validator_peers: BTreeMap<Hash, PeerId> = validators
        .iter()
        .zip(peer_ids.iter())
        .map(|(entry, peer_id)| (entry.validator.id, *peer_id))
        .collect();

    let mut handles = Vec::new();
    let mut object_states = Vec::new();
    let mut statuses = Vec::new();
    let mut tasks = Vec::new();

    for index in 0..VALIDATOR_COUNT {
        let status = Arc::new(RwLock::new(NodeStatus {
            chain_id: genesis.chain_id.clone(),
            genesis_hash: validated.genesis_hash,
            finalized_height: 0,
            finalized_block: validated.genesis_hash,
            state_root: validated.state_root,
            peer_count: 0,
            ready: false,
            finality_latency_ms: None,
            view_changes: 0,
        }));
        let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

        let lifecycle = BlockLifecycle::open(
            &genesis,
            directory.path().join(format!("app-{index}")),
            Arc::clone(&status),
            Arc::clone(&shared_state),
            100,
            4,
        )
        .unwrap();

        let configured_peers = (0..VALIDATOR_COUNT)
            .filter(|other| *other != index)
            .map(|other| ConfiguredPeer {
                peer_id: peer_ids[other],
                address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[other])
                    .parse::<Multiaddr>()
                    .unwrap(),
            })
            .collect();
        let network_node = NetworkNode::spawn(
            libp2p_identities[index].clone(),
            GossipConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[index])
                    .parse()
                    .unwrap(),
                configured_peers,
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();

        let (finalized_order_sender, finalized_order_receiver) = mpsc::unbounded_channel();
        let (pipeline, handle) = Stage2Pipeline::new(
            &genesis,
            network_node,
            lifecycle,
            validator_peers.clone(),
            finalized_order_receiver,
            Stage2PipelineConfig::default(),
        )
        .unwrap();
        let proposal_source = pipeline.proposal_source();

        let coordinator = ConsensusCoordinator::bind_with_pipeline(
            &genesis,
            validators[index].validator.id,
            bls_keys[index].clone(),
            directory.path().join(format!("consensus-{index}")),
            Arc::clone(&status),
            CoordinatorConfig::default(),
            CoordinatorFaults::default(),
            proposal_source,
            finalized_order_sender,
        )
        .await
        .unwrap();

        let genesis_time = genesis.genesis_unix_ms;
        tasks.push(tokio::spawn(async move {
            let _ = coordinator.run(genesis_time).await;
        }));
        tasks.push(tokio::spawn(async move {
            let _ = pipeline.run().await;
        }));

        handles.push(handle);
        object_states.push(shared_state);
        statuses.push(status);
    }

    // Let the gossip mesh dial and stabilize before genesis time arrives.
    tokio::time::sleep(Duration::from_millis(500)).await;

    let transaction = signed_mutation(&account_key, &account_public_key, owner, 0, &target, 0, 42);
    handles[0].submit_transaction(transaction).unwrap();

    // 45s, not 20s: this fixture runs four full tokio multi-threaded
    // runtimes with real libp2p/BLS, and a busy shared CI runner has been
    // observed to need far longer than the ~1.6s this takes locally.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        let all_committed = object_states.iter().all(|state| {
            state
                .read()
                .unwrap()
                .object(&target.id)
                .is_some_and(|object| object.data == vec![42])
        });
        if all_committed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "transaction did not commit on all nodes in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let roots = statuses
        .iter()
        .map(|status| status.read().unwrap().state_root)
        .collect::<Vec<_>>();
    assert!(roots.iter().all(|root| *root == roots[0]));
    let heights = statuses
        .iter()
        .map(|status| status.read().unwrap().finalized_height)
        .collect::<Vec<_>>();
    assert!(heights.iter().all(|height| *height >= 1));

    for task in tasks {
        task.abort();
    }
}

/// Reproduces a real crash found by throughput-testing this pipeline with
/// many concurrent, independent senders: the consensus coordinator's own
/// notion of "current height" advances the instant a certificate forms, on
/// its own task, independently of when the pipeline task gets around to
/// draining the matching `FinalizedOrder` off its channel and retiring that
/// height's transactions from local mempool/nonce bookkeeping. A validator
/// that never led an earlier height (so its own `select_block` never popped
/// that height's transactions out) could race ahead, get asked to lead a
/// later height before the earlier one was retired, and re-select an
/// already-certified transaction — which every honest validator then
/// deterministically rejects as a nonce reuse on the trusted finalized-order
/// path, fatally tearing down both the pipeline and consensus coordinator
/// tasks. Fixed by tracking a `retired_height` in `PipelineState` and
/// refusing to build a proposal more than one height ahead of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)] // Keep the full multi-node wiring/assertion timeline auditable.
async fn stage2_pipeline_commits_many_independent_transactions_concurrently() {
    const TRANSACTION_COUNT: usize = 30;
    let directory = TempDir::new().unwrap();

    let senders = (0..TRANSACTION_COUNT)
        .map(|index| {
            let key = *Hash::digest(index.to_le_bytes()).as_bytes();
            let public_key = Ed25519Scheme.public_key(&key).unwrap();
            let address = Ed25519Scheme.address(&public_key).unwrap();
            let object = Object {
                id: Hash::digest(format!("concurrent-object-{index}").as_bytes()),
                owner: Owner::Single(address),
                type_tag: "stage2::ConcurrentObject".to_owned(),
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

    let bls = Bls12381Scheme;
    let mut bls_keys = Vec::new();
    let mut libp2p_identities = Vec::new();
    let mut validators = Vec::new();
    let gossip_ports = (0..VALIDATOR_COUNT)
        .map(|_| reserve_port())
        .collect::<Vec<_>>();
    for index in 1..=VALIDATOR_COUNT {
        let key = vec![u8::try_from(index).unwrap(); 32];
        let public_key = bls.public_key(&key).unwrap();
        bls_keys.push(key.clone());
        let gossip_identity = identity::Keypair::generate_ed25519();
        let gossip_peer_id = gossip_identity.public().to_peer_id().to_string();
        libp2p_identities.push(gossip_identity);
        validators.push(GenesisValidator {
            name: format!("validator-{index}"),
            validator: Validator {
                id: Hash::digest([u8::try_from(index).unwrap()]),
                stake: 20,
                public_key,
                proof_of_possession: bls.proof_of_possession(&key).unwrap(),
            },
            network_address: reserve_socket_address(),
            rpc_address: reserve_socket_address(),
            gossip_peer_id,
            gossip_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[index - 1]),
        });
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let genesis = GenesisDocument {
        format_version: GENESIS_FORMAT_VERSION,
        chain_id: "kestrel-stage2-concurrent-test".to_owned(),
        genesis_unix_ms: u64::try_from(now).unwrap() + 1_500,
        blocks_per_epoch: 100,
        state_config: StateConfig::default(),
        active_signature_schemes: vec![1, 2],
        equivocation_slash_basis_points: 5_000,
        validators: validators.clone(),
        initial_objects: senders.iter().map(|sender| sender.object.clone()).collect(),
        initial_fee_balances: BTreeMap::new(),
    };
    let validated = genesis.validate().unwrap();

    let peer_ids = libp2p_identities
        .iter()
        .map(identity::Keypair::public)
        .map(|key| key.to_peer_id())
        .collect::<Vec<_>>();
    let validator_peers: BTreeMap<Hash, PeerId> = validators
        .iter()
        .zip(peer_ids.iter())
        .map(|(entry, peer_id)| (entry.validator.id, *peer_id))
        .collect();

    let mut handles = Vec::new();
    let mut object_states = Vec::new();
    let mut tasks = Vec::new();
    let mut statuses = Vec::new();
    let crashes: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

    for index in 0..VALIDATOR_COUNT {
        let status = Arc::new(RwLock::new(NodeStatus {
            chain_id: genesis.chain_id.clone(),
            genesis_hash: validated.genesis_hash,
            finalized_height: 0,
            finalized_block: validated.genesis_hash,
            state_root: validated.state_root,
            peer_count: 0,
            ready: false,
            finality_latency_ms: None,
            view_changes: 0,
        }));
        let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

        let lifecycle = BlockLifecycle::open(
            &genesis,
            directory.path().join(format!("app-{index}")),
            Arc::clone(&status),
            Arc::clone(&shared_state),
            100,
            4,
        )
        .unwrap();

        let configured_peers = (0..VALIDATOR_COUNT)
            .filter(|other| *other != index)
            .map(|other| ConfiguredPeer {
                peer_id: peer_ids[other],
                address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[other])
                    .parse::<Multiaddr>()
                    .unwrap(),
            })
            .collect();
        let network_node = NetworkNode::spawn(
            libp2p_identities[index].clone(),
            GossipConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[index])
                    .parse()
                    .unwrap(),
                configured_peers,
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();

        let (finalized_order_sender, finalized_order_receiver) = mpsc::unbounded_channel();
        let (pipeline, handle) = Stage2Pipeline::new(
            &genesis,
            network_node,
            lifecycle,
            validator_peers.clone(),
            finalized_order_receiver,
            Stage2PipelineConfig::default(),
        )
        .unwrap();
        let proposal_source = pipeline.proposal_source();

        let coordinator = ConsensusCoordinator::bind_with_pipeline(
            &genesis,
            validators[index].validator.id,
            bls_keys[index].clone(),
            directory.path().join(format!("consensus-{index}")),
            Arc::clone(&status),
            CoordinatorConfig::default(),
            CoordinatorFaults::default(),
            proposal_source,
            finalized_order_sender,
        )
        .await
        .unwrap();

        let genesis_time = genesis.genesis_unix_ms;
        // Capture *why* a task ended. Without this the assertion below can only
        // report "something crashed", which is useless on a CI runner where the
        // race reproduces but the machine isn't available to debug.
        let coordinator_crashes = Arc::clone(&crashes);
        tasks.push(tokio::spawn(async move {
            let reason = match coordinator.run(genesis_time).await {
                Ok(outcome) => format!(
                    "validator {index} coordinator exited unexpectedly at height {}",
                    outcome.finalized_height
                ),
                Err(error) => format!("validator {index} coordinator failed: {error}"),
            };
            record_crash(&coordinator_crashes, reason);
        }));
        let pipeline_crashes = Arc::clone(&crashes);
        tasks.push(tokio::spawn(async move {
            if let Err(error) = pipeline.run().await {
                record_crash(
                    &pipeline_crashes,
                    format!("validator {index} pipeline failed: {error}"),
                );
            }
        }));

        handles.push(handle);
        object_states.push(shared_state);
        statuses.push(status);
    }

    // Let the gossip mesh dial and stabilize before genesis time arrives.
    tokio::time::sleep(Duration::from_millis(500)).await;

    for (index, sender) in senders.iter().enumerate() {
        let transaction = signed_mutation(
            &sender.key,
            &sender.public_key,
            sender.address,
            0,
            &sender.object,
            0,
            1,
        );
        handles[index % handles.len()]
            .submit_transaction(transaction)
            .unwrap();
    }

    // Generous because CI is the worst case for this test: an unoptimized debug
    // build (BLS aggregate verification is enormously slower without
    // optimisation) driving four full validators plus thirty transactions on a
    // small, contended runner. The same work takes a couple of seconds on a
    // fast multi-core dev machine, so a deadline tuned there is not a
    // meaningful bound on CI. The diagnostic below distinguishes "slow" from
    // "stalled" so a real hang still fails loudly rather than hiding here.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(180);
    loop {
        let all_committed = object_states.iter().all(|state| {
            let state = state.read().unwrap();
            senders.iter().all(|sender| {
                state
                    .object(&sender.object.id)
                    .is_some_and(|o| o.data == vec![1])
            })
        });
        if all_committed {
            break;
        }
        // Snapshot rather than holding the guard across the assert: panicking
        // while holding it poisons the mutex, which then panics every still-
        // running task and buries the real error under PoisonError noise.
        let reported = crashes.lock().map_or_else(
            |poisoned| poisoned.into_inner().clone(),
            |guard| guard.clone(),
        );
        assert!(
            tasks.iter().all(|task| !task.is_finished()),
            "a validator's pipeline or coordinator task crashed under concurrent independent-sender load: {reported:?}"
        );
        // Report what the chain was actually doing. A stall with a huge
        // view-change count is a very different failure from steady-but-slow
        // progress, and the bare message could not tell them apart.
        if tokio::time::Instant::now() >= deadline {
            let progress = statuses
                .iter()
                .zip(object_states.iter())
                .enumerate()
                .map(|(node, (status, state))| {
                    let status = status.read().unwrap();
                    let state = state.read().unwrap();
                    let committed = senders
                        .iter()
                        .filter(|sender| {
                            state
                                .object(&sender.object.id)
                                .is_some_and(|object| object.data == vec![1])
                        })
                        .count();
                    // `height` distinguishes a stall from mere slowness, and
                    // `state_root` distinguishes slowness from divergence: the
                    // same height with differing roots means nodes executed
                    // the same certified order into different state, which
                    // deferred execution cannot catch on its own because state
                    // roots never feed back into the ordering vote.
                    format!(
                        "node {node}: height={} state_root={} committed={}/{}",
                        status.finalized_height,
                        status.state_root,
                        committed,
                        senders.len()
                    )
                })
                .collect::<Vec<_>>();
            panic!(
                "not all independent transactions committed on every node in time: {progress:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let reported = crashes.lock().map_or_else(
        |poisoned| poisoned.into_inner().clone(),
        |guard| guard.clone(),
    );
    assert!(
        tasks.iter().all(|task| !task.is_finished()),
        "a validator's pipeline or coordinator task crashed under concurrent independent-sender load: {reported:?}"
    );

    for task in tasks {
        task.abort();
    }
}

/// An adversarial gossip peer needs no stake or admission to publish on the
/// transaction topic. Before this test's fix, a single malformed message (or
/// one that fails signature/nonce/mempool validation) would propagate a
/// `PipelineError` through the `?` operator in `Stage2Pipeline::run` and
/// silently kill the whole task — a one-message denial of service against any
/// validator. This proves a garbage message is rejected and logged instead,
/// and that the pipeline keeps committing real transactions afterward.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[allow(clippy::too_many_lines)]
async fn malformed_gossiped_transaction_does_not_kill_the_pipeline() {
    let directory = TempDir::new().unwrap();
    let account_key = [11_u8; 32];
    let account_public_key = Ed25519Scheme.public_key(&account_key).unwrap();
    let owner = Ed25519Scheme.address(&account_public_key).unwrap();
    let target = Object {
        id: Hash::digest([4_u8, 5, 6]),
        owner: Owner::Single(owner),
        type_tag: "stage2::Object".to_owned(),
        version: 0,
        data: vec![0],
        rent_balance: 1_000,
    };

    let bls = Bls12381Scheme;
    let mut bls_keys = Vec::new();
    let mut libp2p_identities = Vec::new();
    let mut validators = Vec::new();
    let gossip_ports = (0..VALIDATOR_COUNT)
        .map(|_| reserve_port())
        .collect::<Vec<_>>();
    for index in 1..=VALIDATOR_COUNT {
        let key = vec![u8::try_from(index).unwrap(); 32];
        let public_key = bls.public_key(&key).unwrap();
        bls_keys.push(key.clone());
        let gossip_identity = identity::Keypair::generate_ed25519();
        let gossip_peer_id = gossip_identity.public().to_peer_id().to_string();
        libp2p_identities.push(gossip_identity);
        validators.push(GenesisValidator {
            name: format!("validator-{index}"),
            validator: Validator {
                id: Hash::digest([u8::try_from(index).unwrap()]),
                stake: 20,
                public_key,
                proof_of_possession: bls.proof_of_possession(&key).unwrap(),
            },
            network_address: reserve_socket_address(),
            rpc_address: reserve_socket_address(),
            gossip_peer_id,
            gossip_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[index - 1]),
        });
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let genesis = GenesisDocument {
        format_version: GENESIS_FORMAT_VERSION,
        chain_id: "kestrel-stage2-dos-test".to_owned(),
        genesis_unix_ms: u64::try_from(now).unwrap() + 1_500,
        blocks_per_epoch: 100,
        state_config: StateConfig::default(),
        active_signature_schemes: vec![1, 2],
        equivocation_slash_basis_points: 5_000,
        validators: validators.clone(),
        initial_objects: vec![target.clone()],
        initial_fee_balances: BTreeMap::new(),
    };
    let validated = genesis.validate().unwrap();

    let peer_ids = libp2p_identities
        .iter()
        .map(identity::Keypair::public)
        .map(|key| key.to_peer_id())
        .collect::<Vec<_>>();
    let validator_peers: BTreeMap<Hash, PeerId> = validators
        .iter()
        .zip(peer_ids.iter())
        .map(|(entry, peer_id)| (entry.validator.id, *peer_id))
        .collect();

    let mut handles = Vec::new();
    let mut object_states = Vec::new();
    let mut statuses = Vec::new();
    let mut tasks = Vec::new();

    for index in 0..VALIDATOR_COUNT {
        let status = Arc::new(RwLock::new(NodeStatus {
            chain_id: genesis.chain_id.clone(),
            genesis_hash: validated.genesis_hash,
            finalized_height: 0,
            finalized_block: validated.genesis_hash,
            state_root: validated.state_root,
            peer_count: 0,
            ready: false,
            finality_latency_ms: None,
            view_changes: 0,
        }));
        let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

        let lifecycle = BlockLifecycle::open(
            &genesis,
            directory.path().join(format!("app-{index}")),
            Arc::clone(&status),
            Arc::clone(&shared_state),
            100,
            4,
        )
        .unwrap();

        let configured_peers = (0..VALIDATOR_COUNT)
            .filter(|other| *other != index)
            .map(|other| ConfiguredPeer {
                peer_id: peer_ids[other],
                address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[other])
                    .parse::<Multiaddr>()
                    .unwrap(),
            })
            .collect();
        let network_node = NetworkNode::spawn(
            libp2p_identities[index].clone(),
            GossipConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[index])
                    .parse()
                    .unwrap(),
                configured_peers,
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();

        let (finalized_order_sender, finalized_order_receiver) = mpsc::unbounded_channel();
        let (pipeline, handle) = Stage2Pipeline::new(
            &genesis,
            network_node,
            lifecycle,
            validator_peers.clone(),
            finalized_order_receiver,
            Stage2PipelineConfig::default(),
        )
        .unwrap();
        let proposal_source = pipeline.proposal_source();

        let coordinator = ConsensusCoordinator::bind_with_pipeline(
            &genesis,
            validators[index].validator.id,
            bls_keys[index].clone(),
            directory.path().join(format!("consensus-{index}")),
            Arc::clone(&status),
            CoordinatorConfig::default(),
            CoordinatorFaults::default(),
            proposal_source,
            finalized_order_sender,
        )
        .await
        .unwrap();

        let genesis_time = genesis.genesis_unix_ms;
        tasks.push(tokio::spawn(async move {
            let _ = coordinator.run(genesis_time).await;
        }));
        let pipeline_task = tokio::spawn(async move {
            let _ = pipeline.run().await;
        });
        tasks.push(pipeline_task);

        handles.push(handle);
        object_states.push(shared_state);
        statuses.push(status);
    }

    // An unauthenticated fifth peer joins the same gossip mesh — no stake,
    // no genesis entry, no admission boundary of its own.
    let attacker_identity = identity::Keypair::generate_ed25519();
    let attacker_port = reserve_port();
    let attacker_configured_peers = peer_ids
        .iter()
        .zip(&gossip_ports)
        .map(|(peer_id, port)| ConfiguredPeer {
            peer_id: *peer_id,
            address: format!("/ip4/127.0.0.1/tcp/{port}").parse().unwrap(),
        })
        .collect();
    let attacker = NetworkNode::spawn(
        attacker_identity,
        GossipConfig {
            listen_address: format!("/ip4/127.0.0.1/tcp/{attacker_port}")
                .parse()
                .unwrap(),
            configured_peers: attacker_configured_peers,
            heartbeat_interval: Duration::from_millis(25),
            ..GossipConfig::default()
        },
    )
    .unwrap();

    // Let the gossip mesh (including the attacker) dial and stabilize before
    // genesis time arrives.
    tokio::time::sleep(Duration::from_millis(500)).await;

    attacker
        .handle
        .try_publish_transaction(b"not a valid bcs-encoded transaction envelope".to_vec())
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        tasks.iter().all(|task| !task.is_finished()),
        "a malformed gossiped transaction must not terminate any node's pipeline or coordinator task"
    );

    let transaction = signed_mutation(&account_key, &account_public_key, owner, 0, &target, 0, 77);
    handles[0].submit_transaction(transaction).unwrap();

    // See the matching comment in the sibling test above for why 45s.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    loop {
        let all_committed = object_states.iter().all(|state| {
            state
                .read()
                .unwrap()
                .object(&target.id)
                .is_some_and(|object| object.data == vec![77])
        });
        if all_committed {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "transaction did not commit on all nodes after the malformed-message attack"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    for task in tasks {
        task.abort();
    }
}

/// An admitted transaction that has not yet been finalized lives only in the
/// pipeline's mempool -- unless it is durably logged. This proves it survives
/// a full drop-and-reopen of the pipeline (standing in for a process crash
/// and restart): submit a transaction, tear down every component holding the
/// node's `RocksDB` store without ever running the pipeline (so the
/// transaction is never proposed or finalized), reopen fresh components at
/// the same data directory, and confirm the transaction is available to
/// propose again with no resubmission (see `docs/TECH_DEBT.md` TD-015).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(clippy::too_many_lines)] // Keep the full admit/drop/reopen/verify timeline auditable.
async fn admitted_transaction_survives_a_restart_before_finalization() {
    let directory = TempDir::new().unwrap();
    let account_key = [21_u8; 32];
    let account_public_key = Ed25519Scheme.public_key(&account_key).unwrap();
    let owner = Ed25519Scheme.address(&account_public_key).unwrap();
    let target = Object {
        id: Hash::digest([1_u8, 2, 3]),
        owner: Owner::Single(owner),
        type_tag: "stage2::Object".to_owned(),
        version: 0,
        data: vec![0],
        rent_balance: 1_000,
    };

    let bls = Bls12381Scheme;
    let mut bls_keys = Vec::new();
    let mut libp2p_identities = Vec::new();
    let mut validators = Vec::new();
    let gossip_ports = (0..VALIDATOR_COUNT)
        .map(|_| reserve_port())
        .collect::<Vec<_>>();
    for index in 1..=VALIDATOR_COUNT {
        let key = vec![u8::try_from(index).unwrap(); 32];
        let public_key = bls.public_key(&key).unwrap();
        bls_keys.push(key.clone());
        let gossip_identity = identity::Keypair::generate_ed25519();
        let gossip_peer_id = gossip_identity.public().to_peer_id().to_string();
        libp2p_identities.push(gossip_identity);
        validators.push(GenesisValidator {
            name: format!("validator-{index}"),
            validator: Validator {
                id: Hash::digest([u8::try_from(index).unwrap()]),
                stake: 20,
                public_key,
                proof_of_possession: bls.proof_of_possession(&key).unwrap(),
            },
            network_address: reserve_socket_address(),
            rpc_address: reserve_socket_address(),
            gossip_peer_id,
            gossip_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[index - 1]),
        });
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let genesis = GenesisDocument {
        format_version: GENESIS_FORMAT_VERSION,
        chain_id: "kestrel-stage2-restart-test".to_owned(),
        genesis_unix_ms: u64::try_from(now).unwrap() + 1_500,
        blocks_per_epoch: 100,
        state_config: StateConfig::default(),
        active_signature_schemes: vec![1, 2],
        equivocation_slash_basis_points: 5_000,
        validators: validators.clone(),
        initial_objects: vec![target.clone()],
        initial_fee_balances: BTreeMap::new(),
    };
    let validated = genesis.validate().unwrap();

    let peer_ids = libp2p_identities
        .iter()
        .map(identity::Keypair::public)
        .map(|key| key.to_peer_id())
        .collect::<Vec<_>>();
    let validator_peers: BTreeMap<Hash, PeerId> = validators
        .iter()
        .zip(peer_ids.iter())
        .map(|(entry, peer_id)| (entry.validator.id, *peer_id))
        .collect();
    let data_directory = directory.path().join("app-0");
    let transaction = signed_mutation(&account_key, &account_public_key, owner, 0, &target, 0, 42);

    // First "process": admit the transaction, then tear down without ever
    // calling `run()` -- nothing gets proposed, ordered, or finalized.
    {
        let status = Arc::new(RwLock::new(NodeStatus {
            chain_id: genesis.chain_id.clone(),
            genesis_hash: validated.genesis_hash,
            finalized_height: 0,
            finalized_block: validated.genesis_hash,
            state_root: validated.state_root,
            peer_count: 0,
            ready: false,
            finality_latency_ms: None,
            view_changes: 0,
        }));
        let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));
        let lifecycle = BlockLifecycle::open(
            &genesis,
            &data_directory,
            Arc::clone(&status),
            Arc::clone(&shared_state),
            100,
            4,
        )
        .unwrap();
        let network_node = NetworkNode::spawn(
            libp2p_identities[0].clone(),
            GossipConfig {
                listen_address: format!("/ip4/127.0.0.1/tcp/{}", gossip_ports[0])
                    .parse()
                    .unwrap(),
                heartbeat_interval: Duration::from_millis(25),
                ..GossipConfig::default()
            },
        )
        .unwrap();
        let (_finalized_order_sender, finalized_order_receiver) = mpsc::unbounded_channel();
        let (pipeline, handle) = Stage2Pipeline::new(
            &genesis,
            network_node,
            lifecycle,
            validator_peers.clone(),
            finalized_order_receiver,
            Stage2PipelineConfig::default(),
        )
        .unwrap();
        handle.submit_transaction(transaction.clone()).unwrap();
        // Drop the pipeline (and, transitively, the lifecycle and its
        // `Arc<RocksDbStore>`) and the handle without ever running either,
        // releasing the RocksDB lock so the same path can be reopened below.
        drop(pipeline);
        drop(handle);
    }

    // Second "process": reopen at the same data directory. No resubmission.
    let status = Arc::new(RwLock::new(NodeStatus {
        chain_id: genesis.chain_id.clone(),
        genesis_hash: validated.genesis_hash,
        finalized_height: 0,
        finalized_block: validated.genesis_hash,
        state_root: validated.state_root,
        peer_count: 0,
        ready: false,
        finality_latency_ms: None,
        view_changes: 0,
    }));
    let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));
    let lifecycle = BlockLifecycle::open(
        &genesis,
        &data_directory,
        Arc::clone(&status),
        Arc::clone(&shared_state),
        100,
        4,
    )
    .unwrap();
    let network_node = NetworkNode::spawn(
        libp2p_identities[0].clone(),
        GossipConfig {
            listen_address: format!("/ip4/127.0.0.1/tcp/{}", reserve_port())
                .parse()
                .unwrap(),
            heartbeat_interval: Duration::from_millis(25),
            ..GossipConfig::default()
        },
    )
    .unwrap();
    let (_finalized_order_sender, finalized_order_receiver) = mpsc::unbounded_channel();
    let (pipeline, _handle) = Stage2Pipeline::new(
        &genesis,
        network_node,
        lifecycle,
        validator_peers,
        finalized_order_receiver,
        Stage2PipelineConfig::default(),
    )
    .unwrap();

    let (restored_ids, _fee_commitment) = pipeline
        .proposal_source()
        .transaction_ids(1, validated.genesis_hash)
        .expect("the durably re-admitted transaction is available to propose");
    assert_eq!(
        restored_ids.len(),
        1,
        "exactly the one restored transaction should be proposable, with no resubmission"
    );
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

fn reserve_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn reserve_socket_address() -> String {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .to_string()
}
