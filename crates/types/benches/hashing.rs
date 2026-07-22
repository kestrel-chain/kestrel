use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use types::{Address, Hash};

fn hashing(criterion: &mut Criterion) {
    let bytes = vec![0x42_u8; 4096];
    let mut group = criterion.benchmark_group("blake3");
    group.throughput(Throughput::Bytes(bytes.len() as u64));
    group.bench_function("hash_4k", |bencher| {
        bencher.iter(|| Hash::digest(&bytes));
    });
    group.finish();

    criterion.bench_function("derive_ed25519_address", |bencher| {
        bencher.iter(|| Address::derive(1, &[0x24; 32]));
    });
}

criterion_group!(benches, hashing);
criterion_main!(benches);
