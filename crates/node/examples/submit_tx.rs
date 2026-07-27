//! Build and sign a `CreateObject` transaction and print the hex blob that the
//! `kestrel_submitTransaction` RPC method expects.
//!
//! Usage:
//!
//! ```text
//! cargo run -p node --example submit_tx -- [ACCOUNT_SEED] [NONCE] [DATA_BYTE]
//! ```
//!
//! The hex transaction is printed on stdout (nothing else), so it can be piped
//! straight into a JSON-RPC body. Human-readable fields go to stderr.
//!
//! The account key is a fixed devnet demo key — never use it for anything real.

use crypto::{Ed25519Scheme, SignatureScheme};
use execution::{AccessMode, DeclaredObjectRef, ExecutableTransaction, MoveOperation};
use types::{Hash, Object, Owner, Transaction};

fn main() {
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    // ACCOUNT_SEED lets a load generator mint unlimited *independent* senders:
    // a fresh seed each call means a fresh account, so every transaction uses
    // nonce 0 and needs no cross-transaction nonce coordination.
    let account_seed: u64 = arguments.first().and_then(|a| a.parse().ok()).unwrap_or(0);
    let nonce: u64 = arguments.get(1).and_then(|a| a.parse().ok()).unwrap_or(0);
    let data: u8 = arguments.get(2).and_then(|a| a.parse().ok()).unwrap_or(1);

    // Deterministic demo account per seed (scheme 1 = Ed25519). Never use these
    // keys for anything real.
    let account_key = *Hash::digest(format!("kestrel-soak-account/{account_seed}")).as_bytes();
    let public_key = Ed25519Scheme.public_key(&account_key).unwrap();
    let sender = Ed25519Scheme.address(&public_key).unwrap();

    // A fresh, unique object id per (sender, nonce) so repeated runs don't
    // collide with an already-created object.
    let mut seed = sender.as_bytes().to_vec();
    seed.extend_from_slice(&nonce.to_be_bytes());
    let object_id = Hash::digest(seed);

    let object = Object {
        id: object_id,
        owner: Owner::Single(sender),
        type_tag: "0x1::demo::Item".to_string(),
        version: 0,
        data: vec![data],
        rent_balance: 1_000,
    };

    let executable = ExecutableTransaction {
        operation: MoveOperation::CreateObject {
            sender,
            object: object.clone(),
        },
        object_references: vec![DeclaredObjectRef {
            id: object_id,
            owner: Owner::Single(sender),
            access: AccessMode::Write,
        }],
        compute_limit: 1_000,
    };

    let mut transaction = Transaction {
        sender,
        nonce,
        payload: bcs::to_bytes(&executable).unwrap(),
        scheme_id: 1,
        public_key,
        signature: Vec::new(),
    };
    transaction.signature = Ed25519Scheme
        .sign(&account_key, &transaction.signing_message())
        .unwrap();

    eprintln!("sender    = {sender}");
    eprintln!("object_id = {object_id}");
    eprintln!("nonce     = {nonce}  data = {data}");
    println!("{}", hex::encode(bcs::to_bytes(&transaction).unwrap()));
}
