use std::time::{Duration, Instant};

use execution::{
    AccessMode, DeclaredObjectRef, DeferredExecutionError, DeferredExecutor, ExecutableTransaction,
    MoveOperation, OrderedExecutionBlock,
};
use state::{StateConfig, StateTree};
use types::{Address, Hash, Object, Owner};

#[test]
fn finalized_order_runs_in_a_bounded_deferred_stage() {
    let owner = Address::from_bytes([7_u8; 32]);
    let mut state = StateTree::new(StateConfig::default()).unwrap();
    let objects = (0_u64..64)
        .map(|index| Object {
            id: Hash::digest(index.to_be_bytes()),
            owner: Owner::Single(owner),
            type_tag: "deferred".to_owned(),
            version: 0,
            data: vec![0],
            rent_balance: 100,
        })
        .collect::<Vec<_>>();
    for object in &objects {
        state.create_object(object.clone()).unwrap();
    }
    let pipeline = DeferredExecutor::new(state, 100, 4, 100).unwrap();
    pipeline.submit(block(1, &objects, owner, 0)).unwrap();

    // Ordering can advance one height without waiting for height 1 execution.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match pipeline.submit(block(2, &objects, owner, 1)) {
            Ok(()) => break,
            Err(DeferredExecutionError::LagLimitReached) if Instant::now() < deadline => {
                std::thread::yield_now();
            }
            result => panic!("failed to pipeline second block: {result:?}"),
        }
    }

    let mut results = Vec::new();
    while results.len() < 2 && Instant::now() < deadline {
        if let Some(result) = pipeline.try_result().unwrap() {
            results.push(result);
        } else {
            std::thread::yield_now();
        }
    }
    assert_eq!(
        results
            .iter()
            .map(|result| result.height)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert!(results.into_iter().all(|result| result.result.is_ok()));
}

fn block(
    height: u64,
    objects: &[Object],
    owner: Address,
    expected_version: u64,
) -> OrderedExecutionBlock {
    let transactions = objects
        .iter()
        .map(|object| ExecutableTransaction {
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
        })
        .collect::<Vec<_>>();
    OrderedExecutionBlock {
        height,
        consensus_block_id: Hash::digest([b"block".as_slice(), &height.to_be_bytes()].concat()),
        transaction_ids: (0..transactions.len())
            .map(|index| {
                Hash::digest(
                    [
                        height.to_be_bytes().as_slice(),
                        index.to_be_bytes().as_slice(),
                    ]
                    .concat(),
                )
            })
            .collect(),
        transactions,
    }
}
