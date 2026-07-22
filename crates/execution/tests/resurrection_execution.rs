use execution::{
    AccessMode, DeclaredObjectRef, ExecutableTransaction, MoveOperation, ParallelExecutor,
    SequentialExecutor,
};
use state::{StateConfig, StateTree};
use types::{Address, Epoch, Hash, Object, Owner};

#[test]
fn resurrection_and_followup_mutation_match_sequential_execution() {
    let owner = Address::from_bytes([0x55; 32]);
    let object = Object {
        id: Hash::digest(b"resurrection-execution"),
        owner: Owner::Single(owner),
        type_tag: "resurrectable".to_owned(),
        version: 0,
        data: b"expired".to_vec(),
        rent_balance: 1,
    };
    let mut initial = StateTree::new(StateConfig::default()).unwrap();
    initial.create_object(object.clone()).unwrap();
    initial.advance_to_epoch(Epoch(1)).unwrap();
    let witness = initial.resurrection_witness(&object.id).unwrap();
    let mut replacement = object.clone();
    replacement.version = 1;
    replacement.rent_balance = 10;
    replacement.data = b"mutated".to_vec();
    let operations = vec![
        MoveOperation::ResurrectObject {
            sender: owner,
            witness: witness.clone(),
            rent_credit: 10,
        },
        MoveOperation::MutateObject {
            sender: owner,
            id: object.id,
            expected_version: 1,
            replacement,
        },
    ];
    let transactions = operations
        .iter()
        .cloned()
        .map(|operation| ExecutableTransaction {
            operation,
            object_references: vec![DeclaredObjectRef {
                id: object.id,
                owner: Owner::Single(owner),
                access: AccessMode::Write,
            }],
            compute_limit: 1_000_000,
        })
        .collect::<Vec<_>>();

    let mut sequential_state = initial.clone();
    let sequential = SequentialExecutor::new(10)
        .unwrap()
        .execute_block(&mut sequential_state, &operations)
        .unwrap();
    let mut parallel_state = initial;
    let parallel = ParallelExecutor::new(10, 2)
        .unwrap()
        .execute_block(&mut parallel_state, &transactions)
        .unwrap();

    assert_eq!(parallel.state_root, sequential.state_root);
    assert_eq!(
        parallel_state.root().unwrap(),
        sequential_state.root().unwrap()
    );
    assert_eq!(parallel_state.object(&object.id).unwrap().version, 2);
}
