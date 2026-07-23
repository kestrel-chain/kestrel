use std::{
    collections::BTreeMap,
    sync::{Arc, RwLock},
    thread,
    time::{Duration, Instant},
};

use consensus::{
    CertificateKind, FinalizedOrder, Proposal, Validator, Vote, VoteCollector, VotePhase,
};
use crypto::{AggregateSignatureScheme, Bls12381Scheme, Ed25519Scheme, SignatureScheme};
use execution::{
    AccessMode, DeclaredObjectRef, ExecutableTransaction, MoveOperation,
    NATIVE_OPERATION_COMPUTE_COST,
};
use network::KestrelCastConfig;
use node::{
    BlockLifecycle, GENESIS_FORMAT_VERSION, GenesisDocument, GenesisValidator, LifecycleError,
    PropagatedBlock, SignedExecutionPayload,
};
use rpc::NodeStatus;
use state::{StateConfig, StateTree};
use tempfile::TempDir;
use types::{Address, Hash, Object, Owner, Transaction};

#[test]
#[allow(clippy::too_many_lines)] // Keep the commit/restart/continuation timeline auditable.
fn reconstructed_signed_block_executes_commits_and_restores_after_restart() {
    let directory = TempDir::new().unwrap();
    let account_key = [99_u8; 32];
    let account_scheme = Ed25519Scheme;
    let account_public_key = account_scheme.public_key(&account_key).unwrap();
    let owner = account_scheme.address(&account_public_key).unwrap();
    let first = object(1, owner);
    let second = object(2, owner);
    let (genesis, validator_keys) = genesis(vec![first.clone(), second.clone()]);
    let validated = genesis.validate().unwrap();
    let status = status(&genesis, validated.genesis_hash, validated.state_root);
    let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

    let first_payload = PropagatedBlock {
        height: 1,
        parent_id: validated.genesis_hash,
        transactions: vec![
            signed_mutation(&account_key, &account_public_key, owner, 0, &first, 0, 11),
            signed_mutation(&account_key, &account_public_key, owner, 1, &second, 0, 12),
        ],
        base_fees: vec![0, 0],
    };
    let first_order = finalized_order(&genesis, &validator_keys, &first_payload);
    let shreds = first_payload.shreds(KestrelCastConfig::default()).unwrap();
    let subset = shreds.into_iter().step_by(2).take(10).collect::<Vec<_>>();

    let first_record = {
        let mut lifecycle = BlockLifecycle::open(
            &genesis,
            directory.path(),
            Arc::clone(&status),
            Arc::clone(&shared_state),
            100,
            4,
        )
        .unwrap();
        let invalid_payload = PropagatedBlock {
            height: 1,
            parent_id: validated.genesis_hash,
            transactions: vec![signed_mutation(
                &account_key,
                &account_public_key,
                owner,
                7,
                &first,
                0,
                99,
            )],
            base_fees: vec![0],
        };
        let invalid_order = finalized_order(&genesis, &validator_keys, &invalid_payload);
        assert!(matches!(
            lifecycle.submit_payload(invalid_order, &invalid_payload),
            Err(LifecycleError::NonceMismatch {
                expected: 0,
                received: 7
            })
        ));
        assert_eq!(lifecycle.committed_height(), 0);
        lifecycle.submit_shreds(first_order, &subset).unwrap();
        let record = wait_for_commit(&mut lifecycle);
        assert_eq!(lifecycle.block(1).unwrap(), Some(record.clone()));
        assert_eq!(lifecycle.committed_height(), 1);
        let state = shared_state.read().unwrap();
        assert_eq!(state.object(&first.id).unwrap().data, vec![11]);
        assert_eq!(state.object(&second.id).unwrap().data, vec![12]);
        record
    };

    let mut lifecycle = BlockLifecycle::open(
        &genesis,
        directory.path(),
        Arc::clone(&status),
        Arc::clone(&shared_state),
        100,
        4,
    )
    .unwrap();
    assert_eq!(lifecycle.committed_height(), 1);
    assert_eq!(lifecycle.committed_block(), first_record.consensus_block_id);
    assert_eq!(status.read().unwrap().state_root, first_record.state_root);

    let first_after = shared_state
        .read()
        .unwrap()
        .object(&first.id)
        .unwrap()
        .clone();
    let second_payload = PropagatedBlock {
        height: 2,
        parent_id: first_record.consensus_block_id,
        transactions: vec![signed_mutation(
            &account_key,
            &account_public_key,
            owner,
            2,
            &first_after,
            1,
            21,
        )],
        base_fees: vec![0],
    };
    let second_order = finalized_order(&genesis, &validator_keys, &second_payload);
    lifecycle
        .submit_payload(second_order, &second_payload)
        .unwrap();
    let second_record = wait_for_commit(&mut lifecycle);
    assert_eq!(second_record.height, 2);
    assert_eq!(status.read().unwrap().finalized_height, 2);
    assert_eq!(
        shared_state.read().unwrap().object(&first.id).unwrap().data,
        vec![21]
    );
}

#[test]
fn rent_epoch_advances_automatically_and_survives_restart() {
    let directory = TempDir::new().unwrap();
    let owner = Address::from_bytes([0x2a; 32]);
    let rented = Object {
        rent_balance: 5,
        ..object(1, owner)
    };
    let (mut genesis, validator_keys) = genesis(vec![rented.clone()]);
    genesis.blocks_per_epoch = 1;
    let validated = genesis.validate().unwrap();
    let node_status = status(&genesis, validated.genesis_hash, validated.state_root);
    let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

    let mut parent_id = validated.genesis_hash;
    {
        let mut lifecycle = BlockLifecycle::open(
            &genesis,
            directory.path(),
            Arc::clone(&node_status),
            Arc::clone(&shared_state),
            100,
            4,
        )
        .unwrap();
        for height in 1_u64..=3 {
            let payload = PropagatedBlock {
                height,
                parent_id,
                transactions: Vec::new(),
                base_fees: Vec::new(),
            };
            let order = finalized_order(&genesis, &validator_keys, &payload);
            lifecycle.submit_payload(order, &payload).unwrap();
            let record = wait_for_commit(&mut lifecycle);
            parent_id = record.consensus_block_id;
        }
    }
    // blocks_per_epoch=1 means each of the three committed heights is its own
    // epoch; the default rent_per_object_per_epoch=1 must have been charged
    // three times purely by committing blocks, with no transaction touching
    // the object at all.
    assert_eq!(
        shared_state
            .read()
            .unwrap()
            .object(&rented.id)
            .unwrap()
            .rent_balance,
        rented.rent_balance - 3
    );

    // Reopening from the same directory must restore the advanced epoch and
    // its rent accounting rather than resetting to genesis.
    let restored_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));
    let restored_status = status(&genesis, validated.genesis_hash, validated.state_root);
    let mut lifecycle = BlockLifecycle::open(
        &genesis,
        directory.path(),
        Arc::clone(&restored_status),
        Arc::clone(&restored_state),
        100,
        4,
    )
    .unwrap();
    assert_eq!(lifecycle.committed_height(), 3);
    assert_eq!(
        restored_state
            .read()
            .unwrap()
            .object(&rented.id)
            .unwrap()
            .rent_balance,
        rented.rent_balance - 3
    );

    // Continuing to commit blocks keeps charging rent until the object
    // exhausts its balance and is moved to expired state.
    for height in 4_u64..=rented.rent_balance {
        let payload = PropagatedBlock {
            height,
            parent_id,
            transactions: Vec::new(),
            base_fees: Vec::new(),
        };
        let order = finalized_order(&genesis, &validator_keys, &payload);
        lifecycle.submit_payload(order, &payload).unwrap();
        let record = wait_for_commit(&mut lifecycle);
        parent_id = record.consensus_block_id;
    }
    let final_state = restored_state.read().unwrap();
    assert!(final_state.object(&rented.id).is_none());
    assert!(final_state.expired_object(&rented.id).is_some());
}

#[test]
fn finalized_block_submitted_before_a_crash_still_commits_after_restart() {
    let directory = TempDir::new().unwrap();
    let account_key = [7_u8; 32];
    let account_scheme = Ed25519Scheme;
    let account_public_key = account_scheme.public_key(&account_key).unwrap();
    let owner = account_scheme.address(&account_public_key).unwrap();
    let first = object(1, owner);
    let (genesis, validator_keys) = genesis(vec![first.clone()]);
    let validated = genesis.validate().unwrap();
    let status = status(&genesis, validated.genesis_hash, validated.state_root);
    let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

    let payload = PropagatedBlock {
        height: 1,
        parent_id: validated.genesis_hash,
        transactions: vec![signed_mutation(
            &account_key,
            &account_public_key,
            owner,
            0,
            &first,
            0,
            42,
        )],
        base_fees: vec![0],
    };
    let order = finalized_order(&genesis, &validator_keys, &payload);

    {
        let mut lifecycle = BlockLifecycle::open(
            &genesis,
            directory.path(),
            Arc::clone(&status),
            Arc::clone(&shared_state),
            100,
            4,
        )
        .unwrap();
        lifecycle.submit_payload(order, &payload).unwrap();
        // The process ends here: the block was validated and handed to the
        // executor, but `poll_commit` never ran, so the durable checkpoint
        // still shows height 0. Only the pending-block record persisted by
        // `submit_payload` can recover it.
    }

    let mut lifecycle = BlockLifecycle::open(
        &genesis,
        directory.path(),
        Arc::clone(&status),
        Arc::clone(&shared_state),
        100,
        4,
    )
    .unwrap();
    assert_eq!(lifecycle.committed_height(), 0);
    let record = wait_for_commit(&mut lifecycle);
    assert_eq!(record.height, 1);
    assert_eq!(lifecycle.committed_height(), 1);
    assert_eq!(
        shared_state.read().unwrap().object(&first.id).unwrap().data,
        vec![42]
    );
}

#[test]
fn committed_transaction_fee_is_debited_from_payer_and_credited_to_the_leader() {
    let directory = TempDir::new().unwrap();
    let account_key = [11_u8; 32];
    let account_scheme = Ed25519Scheme;
    let account_public_key = account_scheme.public_key(&account_key).unwrap();
    let owner = account_scheme.address(&account_public_key).unwrap();
    let first = object(1, owner);
    let (mut genesis, validator_keys) = genesis(vec![first.clone()]);
    genesis.initial_fee_balances.insert(owner, 10_000);
    let validated = genesis.validate().unwrap();
    let leader = validated.validators.leader(1, 0);
    let leader_address = Bls12381Scheme.address(&leader.public_key).unwrap();
    let status = status(&genesis, validated.genesis_hash, validated.state_root);
    let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

    // priority fee 2 + leader-declared base fee 3 = unit price 5; native
    // object primitives always charge exactly `NATIVE_OPERATION_COMPUTE_COST`.
    let payload = PropagatedBlock {
        height: 1,
        parent_id: validated.genesis_hash,
        transactions: vec![signed_mutation_with_fee_bid(
            &account_key,
            &account_public_key,
            owner,
            0,
            &first,
            0,
            42,
            10,
            2,
        )],
        base_fees: vec![3],
    };
    let order = finalized_order(&genesis, &validator_keys, &payload);

    let mut lifecycle = BlockLifecycle::open(
        &genesis,
        directory.path(),
        Arc::clone(&status),
        Arc::clone(&shared_state),
        100,
        4,
    )
    .unwrap();
    lifecycle.submit_payload(order, &payload).unwrap();
    wait_for_commit(&mut lifecycle);

    let expected_charge = 5_u128 * u128::from(NATIVE_OPERATION_COMPUTE_COST);
    assert_eq!(lifecycle.fee_balance(owner), 10_000 - expected_charge);
    assert_eq!(lifecycle.fee_balance(leader_address), expected_charge);
}

#[test]
fn base_fee_that_does_not_match_the_certified_commitment_is_rejected() {
    let directory = TempDir::new().unwrap();
    let account_key = [12_u8; 32];
    let account_scheme = Ed25519Scheme;
    let account_public_key = account_scheme.public_key(&account_key).unwrap();
    let owner = account_scheme.address(&account_public_key).unwrap();
    let first = object(1, owner);
    let (mut genesis, validator_keys) = genesis(vec![first.clone()]);
    genesis.initial_fee_balances.insert(owner, 10_000);
    let validated = genesis.validate().unwrap();
    let status = status(&genesis, validated.genesis_hash, validated.state_root);
    let shared_state = Arc::new(RwLock::new(StateTree::new(StateConfig::default()).unwrap()));

    let payload = PropagatedBlock {
        height: 1,
        parent_id: validated.genesis_hash,
        transactions: vec![signed_mutation_with_fee_bid(
            &account_key,
            &account_public_key,
            owner,
            0,
            &first,
            0,
            42,
            10,
            2,
        )],
        base_fees: vec![3],
    };
    // Certify against the real base fee, then propagate a payload claiming a
    // different one for the same certified block. A leader (or a corrupt
    // shred reconstruction) must not be able to silently change the settled
    // amount after the certificate is formed.
    let order = finalized_order(&genesis, &validator_keys, &payload);
    // 1 (instead of the certified 3) still clears the signed fee cap of 10
    // (1 + priority 2 = 3), so this exercises the commitment mismatch itself
    // rather than tripping the separate fee-cap check first.
    let tampered_payload = PropagatedBlock {
        base_fees: vec![1],
        ..payload
    };

    let mut lifecycle = BlockLifecycle::open(
        &genesis,
        directory.path(),
        Arc::clone(&status),
        Arc::clone(&shared_state),
        100,
        4,
    )
    .unwrap();
    assert!(matches!(
        lifecycle.submit_payload(order, &tampered_payload),
        Err(LifecycleError::OrderMismatch)
    ));
}

#[allow(clippy::too_many_arguments)] // Every argument is a distinct signed-payload/fee-bid field.
fn signed_mutation_with_fee_bid(
    private_key: &[u8],
    public_key: &[u8],
    sender: Address,
    nonce: u64,
    object: &Object,
    expected_version: u64,
    data: u8,
    max_fee_per_compute: u128,
    priority_fee_per_compute: u128,
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
    let payload = SignedExecutionPayload::new(
        executable,
        max_fee_per_compute,
        priority_fee_per_compute,
        Vec::new(),
    );
    let mut transaction = Transaction {
        sender,
        nonce,
        payload: bcs::to_bytes(&payload).unwrap(),
        scheme_id: 1,
        public_key: public_key.to_vec(),
        signature: Vec::new(),
    };
    transaction.signature = Ed25519Scheme
        .sign(private_key, &transaction.signing_message())
        .unwrap();
    transaction
}

fn wait_for_commit(lifecycle: &mut BlockLifecycle) -> node::DurableBlockRecord {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(record) = lifecycle.poll_commit().unwrap() {
            return record;
        }
        assert!(Instant::now() < deadline, "execution did not complete");
        thread::yield_now();
    }
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

fn finalized_order(
    genesis: &GenesisDocument,
    validator_keys: &BTreeMap<Hash, Vec<u8>>,
    payload: &PropagatedBlock,
) -> FinalizedOrder {
    let validated = genesis.validate().unwrap();
    let transaction_ids = payload.transaction_ids().unwrap();
    let proposal = Proposal::new(
        payload.height,
        0,
        payload.parent_id,
        validated.validators.leader(payload.height, 0).id,
        transaction_ids.clone(),
        payload.fee_commitment(),
        None,
    );
    let scheme: Arc<dyn AggregateSignatureScheme> = Arc::new(Bls12381Scheme);
    let mut collector = VoteCollector::new(
        &validated.validators,
        Arc::clone(&scheme),
        CertificateKind::Fast,
        payload.height,
        0,
        proposal.block_id,
    );
    let mut certificate = None;
    for validator in validated.validators.validators() {
        let key = &validator_keys[&validator.id];
        certificate = collector
            .add_vote(
                Vote::sign(
                    validator.id,
                    key,
                    payload.height,
                    0,
                    proposal.block_id,
                    VotePhase::Order,
                    scheme.as_ref(),
                )
                .unwrap(),
            )
            .unwrap()
            .or(certificate);
    }
    FinalizedOrder {
        height: payload.height,
        block_id: proposal.block_id,
        transaction_ids,
        fee_commitment: proposal.fee_commitment,
        certificate: certificate.unwrap(),
    }
}

fn genesis(initial_objects: Vec<Object>) -> (GenesisDocument, BTreeMap<Hash, Vec<u8>>) {
    let scheme = Bls12381Scheme;
    let mut keys = BTreeMap::new();
    let validators = (1_u8..=4)
        .map(|index| {
            let key = vec![index; 32];
            let public_key = scheme.public_key(&key).unwrap();
            let id = Hash::digest([index]);
            keys.insert(id, key.clone());
            let gossip_identity =
                libp2p::identity::Keypair::ed25519_from_bytes([index; 32]).unwrap();
            GenesisValidator {
                name: format!("validator-{index}"),
                validator: Validator {
                    id,
                    stake: 25,
                    public_key,
                    proof_of_possession: scheme.proof_of_possession(&key).unwrap(),
                },
                network_address: format!("127.0.0.1:{}", 9_000 + u16::from(index)),
                rpc_address: format!("127.0.0.1:{}", 10_000 + u16::from(index)),
                gossip_peer_id: gossip_identity.public().to_peer_id().to_string(),
                gossip_address: format!("/ip4/127.0.0.1/tcp/{}", 11_000 + u16::from(index)),
            }
        })
        .collect();
    (
        GenesisDocument {
            format_version: GENESIS_FORMAT_VERSION,
            chain_id: "kestrel-lifecycle-test".to_owned(),
            genesis_unix_ms: 1,
            blocks_per_epoch: 100,
            state_config: StateConfig::default(),
            active_signature_schemes: vec![1, 2],
            equivocation_slash_basis_points: 5_000,
            validators,
            initial_objects,
            initial_fee_balances: BTreeMap::new(),
        },
        keys,
    )
}

fn status(
    genesis: &GenesisDocument,
    genesis_hash: Hash,
    state_root: Hash,
) -> Arc<RwLock<NodeStatus>> {
    Arc::new(RwLock::new(NodeStatus {
        chain_id: genesis.chain_id.clone(),
        genesis_hash,
        finalized_height: 0,
        finalized_block: genesis_hash,
        state_root,
        peer_count: 0,
        ready: false,
        finality_latency_ms: None,
        view_changes: 0,
    }))
}

fn object(seed: u8, owner: Address) -> Object {
    Object {
        id: Hash::digest([seed]),
        owner: Owner::Single(owner),
        type_tag: "lifecycle::Object".to_owned(),
        version: 0,
        data: vec![seed],
        rent_balance: 100,
    }
}
