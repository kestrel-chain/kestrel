use criterion::{Criterion, criterion_group, criterion_main};
use crypto::{Ed25519Scheme, SignatureScheme};

fn signatures(criterion: &mut Criterion) {
    let scheme = Ed25519Scheme;
    let private_key = [0x42_u8; Ed25519Scheme::PRIVATE_KEY_SIZE];
    let public_key = scheme.public_key(&private_key).unwrap();
    let message = b"Kestrel Phase 0 signature verification benchmark";
    let signature = scheme.sign(&private_key, message).unwrap();

    criterion.bench_function("ed25519_sign", |bencher| {
        bencher.iter(|| scheme.sign(&private_key, message).unwrap());
    });
    criterion.bench_function("ed25519_verify", |bencher| {
        bencher.iter(|| scheme.verify(&public_key, message, &signature).unwrap());
    });
}

criterion_group!(benches, signatures);
criterion_main!(benches);
