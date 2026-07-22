use std::{collections::BTreeSet, hint::black_box};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use mempool::{FeeScope, LocalizedMempool, SubmittedTransaction};
use types::{Address, Hash};

fn selection_benchmark(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("localized_fee_selection");
    for hot_depth in [0_u64, 1_000, 10_000] {
        let mut pool = LocalizedMempool::new(1, 0, 64).unwrap();
        let hot = Hash::digest(b"hot");
        let cold = Hash::digest(b"cold");
        for index in 0..hot_depth {
            pool.submit(transaction(index, hot)).unwrap();
        }
        pool.submit(transaction(hot_depth + 1, cold)).unwrap();
        group.bench_with_input(
            BenchmarkId::new("cold_scope_preview", hot_depth),
            &hot_depth,
            |bencher, _| bencher.iter(|| black_box(pool.preview_block(2))),
        );
    }
    group.finish();
}

fn transaction(index: u64, object: Hash) -> SubmittedTransaction {
    SubmittedTransaction {
        id: Hash::digest([object.as_bytes().as_slice(), &index.to_be_bytes()].concat()),
        sender: Address::from_bytes([1; 32]),
        scope: FeeScope::Object(object),
        touched_objects: BTreeSet::from([object]),
        compute_limit: 100,
        max_fee_per_compute: 1_000,
        priority_fee_per_compute: 1,
        arrival_sequence: index,
        policy_data: Vec::new(),
    }
}

criterion_group!(benches, selection_benchmark);
criterion_main!(benches);
