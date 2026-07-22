use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use testkit::{PropagationConfig, PropagationSimulator, lan_validators};

fn propagation(criterion: &mut Criterion) {
    let block = vec![0x42; 512 * 1024];
    let config = PropagationConfig::default();
    let mut group = criterion.benchmark_group("kestrel_cast_simulation");
    group.throughput(Throughput::Bytes(block.len() as u64));
    for validator_count in [20, 50, 100] {
        let validators = lan_validators(validator_count);
        group.bench_with_input(
            BenchmarkId::from_parameter(validator_count),
            &validator_count,
            |bencher, _| {
                bencher.iter(|| {
                    PropagationSimulator::propagate(&block, &validators, &config).unwrap()
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, propagation);
criterion_main!(benches);
