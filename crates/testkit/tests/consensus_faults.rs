use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use execution::{
    AccessMode, DeclaredObjectRef, DeferredExecutionError, DeferredExecutor, ExecutableTransaction,
    MoveOperation, OrderedExecutionBlock,
};
use state::{StateConfig, StateTree};
use testkit::{
    ConsensusFaults, ConsensusSimConfig, ConsensusSimulator, FinalityPath, consensus_lan_validators,
};
use types::{Address, Hash, Object, Owner};

#[test]
fn realistic_lan_finality_is_measured() {
    let validators = consensus_lan_validators(20);
    let report = ConsensusSimulator::finalize_height(
        &validators,
        Hash::digest(b"parent"),
        &[Hash::digest(b"transaction")],
        &ConsensusFaults::default(),
        ConsensusSimConfig::default(),
    )
    .unwrap();
    assert_eq!(report.path, FinalityPath::Fast);
    assert_eq!(report.view_changes, 0);
    assert!(report.finality_latency_ms < 500);
}

#[test]
fn killed_leader_recovers_within_one_view_change() {
    let validators = consensus_lan_validators(20);
    let config = ConsensusSimConfig::default();
    let mut ordered_ids = validators
        .iter()
        .map(|validator| validator.id)
        .collect::<Vec<_>>();
    ordered_ids.sort_unstable();
    let leader =
        ordered_ids[usize::try_from(config.height).unwrap_or_default() % ordered_ids.len()];
    let faults = ConsensusFaults {
        offline: BTreeSet::from([leader]),
        ..ConsensusFaults::default()
    };
    let report = ConsensusSimulator::finalize_height(
        &validators,
        Hash::digest(b"parent"),
        &[Hash::digest(b"transaction")],
        &faults,
        config,
    )
    .unwrap();
    assert_eq!(report.view_changes, 1);
    assert_eq!(report.finalized_view, 1);
}

#[test]
fn isolated_leader_recovers_within_one_view_change() {
    let validators = consensus_lan_validators(20);
    let config = ConsensusSimConfig::default();
    let mut ordered_ids = validators
        .iter()
        .map(|validator| validator.id)
        .collect::<Vec<_>>();
    ordered_ids.sort_unstable();
    let leader =
        ordered_ids[usize::try_from(config.height).unwrap_or_default() % ordered_ids.len()];
    let faults = ConsensusFaults {
        partitioned_links: validators
            .iter()
            .filter(|validator| validator.id != leader)
            .map(|validator| (leader, validator.id))
            .collect(),
        ..ConsensusFaults::default()
    };
    let report = ConsensusSimulator::finalize_height(
        &validators,
        Hash::digest(b"parent"),
        &[Hash::digest(b"transaction")],
        &faults,
        config,
    )
    .unwrap();
    assert_eq!(report.view_changes, 1);
    assert_eq!(report.finalized_view, 1);
}

#[test]
fn equivocation_withholding_partition_delay_and_loss_preserve_progress() {
    let validators = consensus_lan_validators(20);
    let config = ConsensusSimConfig::default();
    let mut ordered_ids = validators
        .iter()
        .map(|validator| validator.id)
        .collect::<Vec<_>>();
    ordered_ids.sort_unstable();
    let first_leader =
        ordered_ids[usize::try_from(config.height).unwrap_or_default() % ordered_ids.len()];
    let byzantine = validators
        .iter()
        .map(|validator| validator.id)
        .filter(|id| *id != first_leader)
        .take(3)
        .collect::<BTreeSet<_>>();
    let withholding = BTreeSet::from([*byzantine.first().unwrap()]);
    let mut partitioned_links = BTreeSet::new();
    for validator in validators.iter().take(2) {
        partitioned_links.insert((first_leader, validator.id));
    }
    let faults = ConsensusFaults {
        byzantine: byzantine.clone(),
        withholding,
        equivocating_leaders: BTreeSet::from([first_leader]),
        partitioned_links,
        additional_delay_ms: validators
            .iter()
            .take(4)
            .map(|validator| (validator.id, 15))
            .collect(),
        drop_basis_points: 100,
        ..ConsensusFaults::default()
    };
    // Include the equivocating leader in the Byzantine budget while staying at 4/20 = 20%
    // would be out of model, so replace one nonleader identity with it.
    let mut faults = faults;
    let removed = *faults.byzantine.last().unwrap();
    faults.byzantine.remove(&removed);
    faults.byzantine.insert(first_leader);
    faults
        .withholding
        .retain(|id| faults.byzantine.contains(id));

    let report = ConsensusSimulator::finalize_height(
        &validators,
        Hash::digest(b"parent"),
        &[Hash::digest(b"transaction")],
        &faults,
        config,
    )
    .unwrap();
    assert!(report.view_changes >= 1);
    assert!(report.view_changes <= config.max_view_changes);
}

#[test]
fn fallback_remains_live_with_separate_fifteen_plus_twenty_nonresponsive_stake() {
    let validators = consensus_lan_validators(20);
    let byzantine = validators
        .iter()
        .take(3)
        .map(|validator| validator.id)
        .collect::<BTreeSet<_>>();
    let offline = validators
        .iter()
        .skip(3)
        .take(4)
        .map(|validator| validator.id)
        .collect::<BTreeSet<_>>();
    let faults = ConsensusFaults {
        withholding: byzantine.clone(),
        byzantine,
        offline,
        ..ConsensusFaults::default()
    };
    let report = ConsensusSimulator::finalize_height(
        &validators,
        Hash::digest(b"parent"),
        &[Hash::digest(b"transaction")],
        &faults,
        ConsensusSimConfig::default(),
    )
    .unwrap();
    assert_eq!(report.path, FinalityPath::Fallback);
    assert!(report.signed_stake >= 1_200);
}

#[test]
fn consensus_orders_next_height_before_prior_execution_finishes() {
    let validators = consensus_lan_validators(20);
    let first = ConsensusSimulator::finalize_height(
        &validators,
        Hash::digest(b"genesis"),
        &[Hash::digest(b"tx-1")],
        &ConsensusFaults::default(),
        ConsensusSimConfig::default(),
    )
    .unwrap();

    let owner = Address::from_bytes([9_u8; 32]);
    let object = Object {
        id: Hash::digest(b"deferred-object"),
        owner: Owner::Single(owner),
        type_tag: "counter".to_owned(),
        version: 0,
        data: vec![0],
        rent_balance: 100,
    };
    let mut state = StateTree::new(StateConfig::default()).unwrap();
    state.create_object(object.clone()).unwrap();
    let executor = DeferredExecutor::new(state, 100, 2, 100).unwrap();
    executor
        .submit(execution_block(1, first.block_id, &object, owner, 0))
        .unwrap();

    // Height 2 ordering completes without polling or awaiting height 1 execution.
    let second = ConsensusSimulator::finalize_height(
        &validators,
        first.block_id,
        &[Hash::digest(b"tx-2")],
        &ConsensusFaults::default(),
        ConsensusSimConfig {
            height: 2,
            ..ConsensusSimConfig::default()
        },
    )
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match executor.submit(execution_block(2, second.block_id, &object, owner, 1)) {
            Ok(()) => break,
            Err(DeferredExecutionError::LagLimitReached) if Instant::now() < deadline => {
                std::thread::yield_now();
            }
            result => panic!("height 2 execution submission failed: {result:?}"),
        }
    }

    let mut heights = Vec::new();
    while heights.len() < 2 && Instant::now() < deadline {
        if let Some(result) = executor.try_result().unwrap() {
            result.result.unwrap();
            heights.push(result.height);
        } else {
            std::thread::yield_now();
        }
    }
    assert_eq!(heights, vec![1, 2]);
}

fn execution_block(
    height: u64,
    block_id: Hash,
    object: &Object,
    owner: Address,
    expected_version: u64,
) -> OrderedExecutionBlock {
    OrderedExecutionBlock {
        height,
        consensus_block_id: block_id,
        transaction_ids: vec![Hash::digest(format!("tx-{height}"))],
        transactions: vec![ExecutableTransaction {
            operation: MoveOperation::MutateObject {
                sender: owner,
                id: object.id,
                expected_version,
                replacement: Object {
                    version: expected_version,
                    data: vec![u8::try_from(height).unwrap()],
                    ..object.clone()
                },
            },
            object_references: vec![DeclaredObjectRef {
                id: object.id,
                owner: Owner::Single(owner),
                access: AccessMode::Write,
            }],
            compute_limit: 1_000_000,
        }],
    }
}
