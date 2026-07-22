use std::{collections::BTreeMap, fs};

use execution::{
    AccessMode, DeclaredObjectRef, ExecutableTransaction, MoveOperation, ParallelExecutor,
    SequentialExecutor,
};
use move_binary_format::file_format::CompiledModule;
use move_compiler::{Compiler, compiled_unit::AnnotatedCompiledUnit};
use state::{StateConfig, StateTree};
use tempfile::TempDir;
use types::{Address, Owner};
use vm_move::{MoveArgument, MoveCall, MoveModuleId, MoveVmHost};

#[test]
fn realistic_move_transfers_have_hard_parallel_root_equivalence_gate() {
    let publisher = Address::from_bytes([0x77; 32]);
    let module_bytes = compile_module(&token_source(publisher));
    let (initial, transactions) = workload(64, publisher, &module_bytes);
    let operations = transactions
        .iter()
        .map(|transaction| transaction.operation.clone())
        .collect::<Vec<_>>();
    let mut sequential_state = initial.clone();
    let sequential = SequentialExecutor::new(1_000)
        .unwrap()
        .execute_block(&mut sequential_state, &operations)
        .unwrap();
    let mut parallel_state = initial;
    let parallel = ParallelExecutor::new(1_000, 8)
        .unwrap()
        .execute_block(&mut parallel_state, &transactions)
        .unwrap();

    assert_eq!(parallel.state_root, sequential.state_root);
    assert_eq!(
        parallel_state.root().unwrap(),
        sequential_state.root().unwrap()
    );
    assert_eq!(parallel.receipts.len(), sequential.receipts.len());
    assert!(
        parallel
            .receipts
            .iter()
            .all(|receipt| receipt.state_root == parallel.state_root)
    );
}

fn workload(
    transaction_count: usize,
    publisher: Address,
    module_bytes: &[u8],
) -> (StateTree, Vec<ExecutableTransaction>) {
    let mut state = StateTree::new(StateConfig::default()).unwrap();
    let host = MoveVmHost::new(1_000).unwrap();
    host.publish_module(&mut state, publisher, module_bytes.to_vec(), 1_000_000)
        .unwrap();
    let module = MoveModuleId {
        address: publisher,
        name: "Token".to_owned(),
    };
    let mut pairs = Vec::with_capacity(transaction_count);
    for index in 0..transaction_count {
        let source = benchmark_address(0x51, index);
        let destination = benchmark_address(0x52, index);
        for (sender, balance) in [(source, 100_u64), (destination, 0_u64)] {
            host.execute_entry_function(
                &mut state,
                &MoveCall {
                    sender,
                    module: module.clone(),
                    function: "mint".to_owned(),
                    arguments: vec![MoveArgument::Signer, MoveArgument::U64(balance)],
                },
                1_000_000,
            )
            .unwrap();
        }
        pairs.push((source, destination));
    }

    let module_id = state
        .active_objects()
        .find(|object| matches!(object.owner, Owner::Shared))
        .unwrap()
        .id;
    let resources = state
        .active_objects()
        .filter_map(|object| match object.owner {
            Owner::Single(owner) => Some((owner, object.id)),
            Owner::Shared => None,
        })
        .collect::<BTreeMap<_, _>>();
    let transactions = pairs
        .into_iter()
        .map(|(source, destination)| ExecutableTransaction {
            operation: MoveOperation::EntryFunction(MoveCall {
                sender: source,
                module: module.clone(),
                function: "transfer".to_owned(),
                arguments: vec![
                    MoveArgument::Address(source),
                    MoveArgument::Address(destination),
                    MoveArgument::U64(1),
                ],
            }),
            object_references: vec![
                DeclaredObjectRef {
                    id: module_id,
                    owner: Owner::Shared,
                    access: AccessMode::Read,
                },
                DeclaredObjectRef {
                    id: resources[&source],
                    owner: Owner::Single(source),
                    access: AccessMode::Write,
                },
                DeclaredObjectRef {
                    id: resources[&destination],
                    owner: Owner::Single(destination),
                    access: AccessMode::Write,
                },
            ],
            compute_limit: 1_000_000,
        })
        .collect();
    (state.speculative_snapshot(), transactions)
}

fn benchmark_address(domain: u8, index: usize) -> Address {
    let mut bytes = [0_u8; 32];
    bytes[0] = domain;
    bytes[24..].copy_from_slice(&u64::try_from(index).unwrap().to_be_bytes());
    Address::from_bytes(bytes)
}

fn token_source(address: Address) -> String {
    include_str!("../../vm-move/tests/fixtures/token.move")
        .replace("__KESTREL_PUBLISHER__", &address.to_string())
}

fn compile_module(source: &str) -> Vec<u8> {
    let directory = TempDir::new().unwrap();
    let source_path = directory.path().join("root-equivalence.move");
    fs::write(&source_path, source).unwrap();
    let (_, units) = Compiler::from_files(
        vec![source_path.to_string_lossy().into_owned()],
        Vec::new(),
        BTreeMap::<String, move_compiler::shared::NumericalAddress>::new(),
    )
    .build_and_report()
    .unwrap();
    let module = units
        .into_iter()
        .find_map(|unit| match unit {
            AnnotatedCompiledUnit::Module(module) => Some(module.named_module.module),
            AnnotatedCompiledUnit::Script(_) => None,
        })
        .unwrap();
    let mut bytes = Vec::new();
    CompiledModule::serialize(&module, &mut bytes).unwrap();
    bytes
}
