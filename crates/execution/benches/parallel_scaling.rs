use std::{collections::BTreeMap, fs};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use execution::{
    AccessMode, DeclaredObjectRef, ExecutableTransaction, MoveOperation, ParallelExecutor,
    SequentialExecutor,
};
use move_binary_format::file_format::CompiledModule;
use move_compiler::{Compiler, compiled_unit::AnnotatedCompiledUnit};
use state::{StateConfig, StateTree};
use tempfile::TempDir;
use types::{Address, Hash, Object, Owner};
use vm_move::{MoveArgument, MoveCall, MoveModuleId, MoveVmHost};

const BLOCK_SIZES: [usize; 3] = [256, 1_024, 4_096];
const HIGH_CONTENTION_TRANSACTIONS: usize = 256;

fn object(index: usize, owner: Owner) -> Object {
    Object {
        id: Hash::digest(index.to_be_bytes()),
        owner,
        type_tag: "bench::Counter".to_owned(),
        version: 0,
        data: vec![0; 256],
        rent_balance: 1_000,
    }
}

fn low_contention(transaction_count: usize) -> (StateTree, Vec<ExecutableTransaction>) {
    let mut state = StateTree::new(StateConfig::default()).unwrap();
    let mut transactions = Vec::with_capacity(transaction_count);
    for index in 0..transaction_count {
        let owner = Address::from_bytes(Hash::digest(index.to_be_bytes()).as_bytes().to_owned());
        let original = object(index, Owner::Single(owner));
        state.create_object(original.clone()).unwrap();
        let mut replacement = original.clone();
        replacement.data[0] = 1;
        transactions.push(ExecutableTransaction {
            operation: MoveOperation::MutateObject {
                sender: owner,
                id: original.id,
                expected_version: 0,
                replacement,
            },
            object_references: vec![DeclaredObjectRef {
                id: original.id,
                owner: Owner::Single(owner),
                access: AccessMode::Write,
            }],
            compute_limit: 1_000_000,
        });
    }
    (state.speculative_snapshot(), transactions)
}

fn high_contention() -> (StateTree, Vec<ExecutableTransaction>) {
    let mut state = StateTree::new(StateConfig::default()).unwrap();
    let owner = Address::from_bytes([7; 32]);
    let original = object(0, Owner::Shared);
    state.create_object(original.clone()).unwrap();
    let transactions = (0..HIGH_CONTENTION_TRANSACTIONS)
        .map(|index| {
            let mut replacement = original.clone();
            replacement.version = index as u64;
            replacement.data[0] = u8::try_from(index).unwrap();
            ExecutableTransaction {
                operation: MoveOperation::MutateObject {
                    sender: owner,
                    id: original.id,
                    expected_version: index as u64,
                    replacement,
                },
                object_references: vec![DeclaredObjectRef {
                    id: original.id,
                    owner: Owner::Shared,
                    access: AccessMode::Write,
                }],
                compute_limit: 1_000_000,
            }
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

fn realistic_move(
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

fn token_source(address: Address) -> String {
    include_str!("../../vm-move/tests/fixtures/token.move")
        .replace("__KESTREL_PUBLISHER__", &address.to_string())
}

fn compile_module(source: &str) -> Vec<u8> {
    let directory = TempDir::new().unwrap();
    let source_path = directory.path().join("benchmark.move");
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

fn scaling(c: &mut Criterion) {
    for transaction_count in BLOCK_SIZES {
        let (initial, transactions) = low_contention(transaction_count);
        let name = format!("low_contention/{transaction_count}");
        let mut group = c.benchmark_group(name);
        group.throughput(Throughput::Elements(transactions.len() as u64));
        let operations: Vec<_> = transactions
            .iter()
            .map(|transaction| transaction.operation.clone())
            .collect();
        group.bench_function("sequential", |b| {
            let executor = SequentialExecutor::new(1_000).unwrap();
            b.iter(|| {
                let mut state = initial.clone();
                executor.execute_block(&mut state, &operations).unwrap()
            });
        });
        let maximum = std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .min(8);
        for workers in [1, 2, 4, 8]
            .into_iter()
            .filter(|workers| *workers <= maximum)
        {
            let executor = ParallelExecutor::new(1_000, workers).unwrap();
            group.bench_with_input(BenchmarkId::new("parallel", workers), &workers, |b, _| {
                b.iter(|| {
                    let mut state = initial.clone();
                    executor.execute_block(&mut state, &transactions).unwrap()
                });
            });
        }
        group.finish();
    }

    let (initial, transactions) = high_contention();
    let mut group = c.benchmark_group("high_contention/256");
    group.throughput(Throughput::Elements(transactions.len() as u64));
    let operations: Vec<_> = transactions
        .iter()
        .map(|transaction| transaction.operation.clone())
        .collect();
    group.bench_function("sequential", |b| {
        let executor = SequentialExecutor::new(1_000).unwrap();
        b.iter(|| {
            let mut state = initial.clone();
            executor.execute_block(&mut state, &operations).unwrap()
        });
    });
    let maximum = std::thread::available_parallelism()
        .map_or(1, std::num::NonZero::get)
        .min(8);
    for workers in [1, 2, 4, 8]
        .into_iter()
        .filter(|workers| *workers <= maximum)
    {
        let executor = ParallelExecutor::new(1_000, workers).unwrap();
        group.bench_with_input(BenchmarkId::new("parallel", workers), &workers, |b, _| {
            b.iter(|| {
                let mut state = initial.clone();
                executor.execute_block(&mut state, &transactions).unwrap()
            });
        });
    }
    group.finish();
}

fn realistic_move_scaling(c: &mut Criterion) {
    let publisher = Address::from_bytes([0x77; 32]);
    let module_bytes = compile_module(&token_source(publisher));
    for transaction_count in BLOCK_SIZES {
        let (initial, transactions) = realistic_move(transaction_count, publisher, &module_bytes);
        let name = format!("realistic_move/{transaction_count}");
        let mut group = c.benchmark_group(name);
        group.throughput(Throughput::Elements(transactions.len() as u64));
        let operations = transactions
            .iter()
            .map(|transaction| transaction.operation.clone())
            .collect::<Vec<_>>();
        let maximum = std::thread::available_parallelism()
            .map_or(1, std::num::NonZero::get)
            .min(8);
        let mut reference_state = initial.clone();
        let reference = SequentialExecutor::new(1_000)
            .unwrap()
            .execute_block(&mut reference_state, &operations)
            .unwrap();
        let mut parallel_state = initial.clone();
        let parallel = ParallelExecutor::new(1_000, maximum)
            .unwrap()
            .execute_block(&mut parallel_state, &transactions)
            .unwrap();
        assert_eq!(parallel.state_root, reference.state_root);
        assert_eq!(
            parallel_state.root().unwrap(),
            reference_state.root().unwrap()
        );
        group.bench_function("sequential", |b| {
            let executor = SequentialExecutor::new(1_000).unwrap();
            b.iter(|| {
                let mut state = initial.clone();
                executor.execute_block(&mut state, &operations).unwrap()
            });
        });
        for workers in [1, 2, 4, 8]
            .into_iter()
            .filter(|workers| *workers <= maximum)
        {
            let executor = ParallelExecutor::new(1_000, workers).unwrap();
            group.bench_with_input(BenchmarkId::new("parallel", workers), &workers, |b, _| {
                b.iter(|| {
                    let mut state = initial.clone();
                    executor.execute_block(&mut state, &transactions).unwrap()
                });
            });
        }
        group.finish();
    }
}

criterion_group!(benches, scaling, realistic_move_scaling);
criterion_main!(benches);
