use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use testkit::{ConsensusFaults, ConsensusSimConfig, ConsensusSimulator, consensus_lan_validators};
use types::Hash;

fn finality_benchmark(criterion: &mut Criterion) {
    let validators = consensus_lan_validators(20);
    let transactions = (0_u64..256)
        .map(|index| Hash::digest(index.to_be_bytes()))
        .collect::<Vec<_>>();
    criterion.bench_function("consensus/finality/20_validators", |bencher| {
        bencher.iter(|| {
            ConsensusSimulator::finalize_height(
                black_box(&validators),
                Hash::digest(b"parent"),
                black_box(&transactions),
                &ConsensusFaults::default(),
                ConsensusSimConfig::default(),
            )
            .unwrap()
        });
    });
}

criterion_group!(benches, finality_benchmark);
criterion_main!(benches);
