use execution::{
    AccessMode, DeclaredObjectRef, ExecutableTransaction, ExecutionPath, MoveOperation,
    ParallelExecutor, SequentialExecutor,
};
use proptest::prelude::*;
use state::{StateConfig, StateTree};
use types::{Address, Hash, Object, Owner};

fn address(seed: u8) -> Address {
    Address::from_bytes([seed; 32])
}

fn object(slot: u8, owner: Owner, value: u8) -> Object {
    Object {
        id: Hash::digest([slot]),
        owner,
        type_tag: "test::Counter".to_owned(),
        version: 0,
        data: vec![value],
        rent_balance: 1_000,
    }
}

fn transaction(operation: MoveOperation, id: Hash, owner: Owner) -> ExecutableTransaction {
    ExecutableTransaction {
        operation,
        object_references: vec![DeclaredObjectRef {
            id,
            owner,
            access: AccessMode::Write,
        }],
        compute_limit: 1_000_000,
    }
}

fn assert_equivalent(initial: &StateTree, transactions: &[ExecutableTransaction]) {
    let operations: Vec<_> = transactions
        .iter()
        .map(|transaction| transaction.operation.clone())
        .collect();
    let mut sequential_state = initial.clone();
    let mut parallel_state = initial.clone();
    let sequential = SequentialExecutor::new(1_000)
        .unwrap()
        .execute_block(&mut sequential_state, &operations)
        .unwrap();
    let parallel = ParallelExecutor::new(1_000, 4)
        .unwrap()
        .execute_block(&mut parallel_state, transactions)
        .unwrap();

    assert_eq!(parallel.state_root, sequential.state_root);
    assert_eq!(
        parallel_state.root().unwrap(),
        sequential_state.root().unwrap()
    );
    assert!(
        parallel
            .receipts
            .iter()
            .all(|receipt| receipt.state_root == parallel.state_root)
    );
    assert!(
        sequential
            .receipts
            .iter()
            .all(|receipt| receipt.state_root == sequential.state_root)
    );
    assert_eq!(
        parallel
            .receipts
            .iter()
            .map(|receipt| receipt.state_root)
            .collect::<Vec<_>>(),
        sequential
            .receipts
            .iter()
            .map(|receipt| receipt.state_root)
            .collect::<Vec<_>>()
    );
}

proptest! {
    #[test]
    fn low_contention_matches_sequential(values in prop::collection::vec(any::<u8>(), 1..40)) {
        let mut initial = StateTree::new(StateConfig::default()).unwrap();
        let transactions = values
            .into_iter()
            .enumerate()
            .map(|(index, value)| {
                let slot = u8::try_from(index).unwrap();
                let owner = address(slot.wrapping_add(1));
                let original = object(slot, Owner::Single(owner), 0);
                initial.create_object(original.clone()).unwrap();
                let mut replacement = original.clone();
                replacement.data[0] = value;
                transaction(
                    MoveOperation::MutateObject {
                        sender: owner,
                        id: original.id,
                        expected_version: 0,
                        replacement,
                    },
                    original.id,
                    Owner::Single(owner),
                )
            })
            .collect::<Vec<_>>();
        assert_equivalent(&initial, &transactions);
    }

    #[test]
    fn shared_object_contention_matches_sequential(values in prop::collection::vec(any::<u8>(), 1..40)) {
        let mut initial = StateTree::new(StateConfig::default()).unwrap();
        let original = object(200, Owner::Shared, 0);
        initial.create_object(original.clone()).unwrap();
        let transactions = values
            .into_iter()
            .enumerate()
            .map(|(version, value)| {
                let mut replacement = original.clone();
                replacement.data[0] = value;
                transaction(
                    MoveOperation::MutateObject {
                        sender: address(99),
                        id: original.id,
                        expected_version: version as u64,
                        replacement,
                    },
                    original.id,
                    Owner::Shared,
                )
            })
            .collect::<Vec<_>>();
        assert_equivalent(&initial, &transactions);
    }

    #[test]
    fn adversarial_mixes_match_sequential(actions in prop::collection::vec(any::<u8>(), 1..60)) {
        let mut initial = StateTree::new(StateConfig::default()).unwrap();
        let owners = [Owner::Shared, Owner::Single(address(11)), Owner::Single(address(12)), Owner::Shared];
        let mut model = owners
            .iter()
            .enumerate()
            .map(|(slot, owner)| {
                let value = object(u8::try_from(slot).unwrap(), owner.clone(), 0);
                initial.create_object(value.clone()).unwrap();
                Some(value)
            })
            .collect::<Vec<_>>();
        let mut transactions = Vec::with_capacity(actions.len());

        for (step, action) in actions.into_iter().enumerate() {
            let slot_index = usize::from(action) % model.len();
            let owner = owners[slot_index].clone();
            let sender = match &owner {
                Owner::Single(owner) => *owner,
                Owner::Shared => address(90),
            };
            let slot = u8::try_from(slot_index).unwrap();
            let id = Hash::digest([slot]);
            let operation = if let Some(current) = &model[slot_index] {
                if action & 0x80 != 0 && step + 1 < 60 {
                    let operation = MoveOperation::DeleteObject {
                        sender,
                        id,
                        expected_version: current.version,
                    };
                    model[slot_index] = None;
                    operation
                } else {
                    let mut replacement = current.clone();
                    replacement.data[0] = action;
                    let operation = MoveOperation::MutateObject {
                        sender,
                        id,
                        expected_version: current.version,
                        replacement: replacement.clone(),
                    };
                    replacement.version += 1;
                    model[slot_index] = Some(replacement);
                    operation
                }
            } else {
                let created = object(slot, owner.clone(), action);
                model[slot_index] = Some(created.clone());
                MoveOperation::CreateObject { sender, object: created }
            };
            transactions.push(transaction(operation, id, owner));
        }
        assert_equivalent(&initial, &transactions);
    }
}

#[test]
fn scheduler_reports_fast_path_and_conflict_reexecution() {
    let mut initial = StateTree::new(StateConfig::default()).unwrap();
    let fast_owner = address(1);
    let fast = object(1, Owner::Single(fast_owner), 0);
    let shared = object(2, Owner::Shared, 0);
    initial.create_object(fast.clone()).unwrap();
    initial.create_object(shared.clone()).unwrap();

    let mut fast_replacement = fast.clone();
    fast_replacement.data[0] = 1;
    let mut shared_first = shared.clone();
    shared_first.data[0] = 1;
    let mut shared_second = shared.clone();
    shared_second.data[0] = 2;
    let transactions = vec![
        transaction(
            MoveOperation::MutateObject {
                sender: fast_owner,
                id: fast.id,
                expected_version: 0,
                replacement: fast_replacement,
            },
            fast.id,
            Owner::Single(fast_owner),
        ),
        transaction(
            MoveOperation::MutateObject {
                sender: address(9),
                id: shared.id,
                expected_version: 0,
                replacement: shared_first,
            },
            shared.id,
            Owner::Shared,
        ),
        transaction(
            MoveOperation::MutateObject {
                sender: address(9),
                id: shared.id,
                expected_version: 1,
                replacement: shared_second,
            },
            shared.id,
            Owner::Shared,
        ),
    ];
    let result = ParallelExecutor::new(1_000, 2)
        .unwrap()
        .execute_block(&mut initial, &transactions)
        .unwrap();
    assert_eq!(result.receipts[0].path, ExecutionPath::StructuralFastPath);
    assert_eq!(result.receipts[2].path, ExecutionPath::Reexecuted);
    assert_eq!(result.scheduler.structural_fast_path, 1);
    assert!(result.scheduler.aborts >= 1);
}

#[test]
fn duplicate_absent_id_across_owner_lanes_matches_sequential_failure() {
    let initial = StateTree::new(StateConfig::default()).unwrap();
    let id = Hash::digest(b"duplicate-fast-create");
    let first_owner = address(31);
    let second_owner = address(32);
    let first = Object {
        id,
        owner: Owner::Single(first_owner),
        type_tag: "test::First".to_owned(),
        version: 0,
        data: vec![1],
        rent_balance: 10,
    };
    let second = Object {
        owner: Owner::Single(second_owner),
        type_tag: "test::Second".to_owned(),
        data: vec![2],
        ..first.clone()
    };
    let transactions = vec![
        transaction(
            MoveOperation::CreateObject {
                sender: first_owner,
                object: first,
            },
            id,
            Owner::Single(first_owner),
        ),
        transaction(
            MoveOperation::CreateObject {
                sender: second_owner,
                object: second,
            },
            id,
            Owner::Single(second_owner),
        ),
    ];
    let operations = transactions
        .iter()
        .map(|transaction| transaction.operation.clone())
        .collect::<Vec<_>>();
    let mut sequential_state = initial.clone();
    let sequential_error = SequentialExecutor::new(1_000)
        .unwrap()
        .execute_block(&mut sequential_state, &operations)
        .unwrap_err();
    let mut parallel_state = initial;
    let parallel_error = ParallelExecutor::new(1_000, 2)
        .unwrap()
        .execute_block(&mut parallel_state, &transactions)
        .unwrap_err();

    assert!(
        sequential_error
            .to_string()
            .starts_with("transaction 1 failed")
    );
    assert!(
        parallel_error
            .to_string()
            .starts_with("transaction 1 failed")
    );
    assert_eq!(
        parallel_state.root().unwrap(),
        sequential_state.root().unwrap()
    );
    assert_eq!(parallel_state.object(&id), sequential_state.object(&id));
}
