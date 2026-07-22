//! Canonical protocol value types shared across Kestrel.

use core::fmt;
use serde::{Deserialize, Serialize};

/// Byte width of Kestrel hashes and addresses.
pub const DIGEST_LENGTH: usize = 32;

/// Domain separator for protocol addresses.
const ADDRESS_DOMAIN: &[u8] = b"kestrel/address/v1";

/// Numeric identifier assigned to a signature scheme.
pub type SchemeId = u16;

/// A BLAKE3 digest.
#[derive(Clone, Copy, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Hash([u8; DIGEST_LENGTH]);

impl Hash {
    /// Hashes arbitrary bytes with BLAKE3.
    #[must_use]
    pub fn digest(bytes: impl AsRef<[u8]>) -> Self {
        Self(*blake3::hash(bytes.as_ref()).as_bytes())
    }

    /// Constructs a digest from its exact byte representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; DIGEST_LENGTH]) -> Self {
        Self(bytes)
    }

    /// Returns the exact byte representation.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_LENGTH] {
        &self.0
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Hash({self})")
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

/// A crypto-agile account address.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Address([u8; DIGEST_LENGTH]);

impl Address {
    /// Derives an address from a scheme identifier and encoded public key.
    ///
    /// Length framing and domain separation make this mapping unambiguous and
    /// prevent addresses from being interpreted as legacy raw public keys.
    #[must_use]
    pub fn derive(scheme_id: SchemeId, public_key: &[u8]) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(ADDRESS_DOMAIN);
        hasher.update(&scheme_id.to_be_bytes());
        hasher.update(&(public_key.len() as u64).to_be_bytes());
        hasher.update(public_key);
        Self(*hasher.finalize().as_bytes())
    }

    /// Constructs an address from an already-derived byte representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; DIGEST_LENGTH]) -> Self {
        Self(bytes)
    }

    /// Returns the exact byte representation.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_LENGTH] {
        &self.0
    }
}

impl fmt::Debug for Address {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Address({self})")
    }
}

impl fmt::Display for Address {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&hex::encode(self.0))
    }
}

/// Consensus slot number.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Slot(pub u64);

/// Protocol epoch number.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Epoch(pub u64);

/// Globally unique object identifier.
pub type ObjectId = Hash;

/// Ownership controls the execution conflict model.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Owner {
    Single(Address),
    Shared,
}

/// Object/resource state carried by the protocol.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Object {
    pub id: ObjectId,
    pub owner: Owner,
    pub type_tag: String,
    pub version: u64,
    pub data: Vec<u8>,
    pub rent_balance: u64,
}

/// Minimal account state used by native account operations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AccountState {
    pub address: Address,
    pub nonce: u64,
    pub balance: u128,
}

/// A signed transaction envelope independent of any concrete signature scheme.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    pub sender: Address,
    pub nonce: u64,
    pub payload: Vec<u8>,
    pub scheme_id: SchemeId,
    pub public_key: Vec<u8>,
    pub signature: Vec<u8>,
}

impl Transaction {
    /// Canonical payload committed to by a transaction signature.
    #[must_use]
    pub fn signing_message(&self) -> Vec<u8> {
        let mut message = Vec::with_capacity(DIGEST_LENGTH + 16 + self.payload.len());
        message.extend_from_slice(self.sender.as_bytes());
        message.extend_from_slice(&self.nonce.to_be_bytes());
        message.extend_from_slice(&(self.payload.len() as u64).to_be_bytes());
        message.extend_from_slice(&self.payload);
        message
    }
}

/// Ordered transaction block.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Block {
    pub parent_hash: Hash,
    pub slot: Slot,
    pub epoch: Epoch,
    pub proposer: Address,
    pub transactions: Vec<Transaction>,
}

#[cfg(test)]
mod tests {
    use super::{Address, Hash};
    use rand::{RngCore, SeedableRng, rngs::StdRng};

    #[test]
    fn address_derivation_is_deterministic_and_scheme_separated() {
        let key = [7_u8; 32];
        assert_eq!(Address::derive(1, &key), Address::derive(1, &key));
        assert_ne!(Address::derive(1, &key), Address::derive(2, &key));
    }

    #[test]
    fn derived_ed25519_addresses_do_not_alias_raw_public_keys() {
        let mut rng = StdRng::seed_from_u64(0x0054_4852_594c_4f53);
        for _ in 0..10_000 {
            let mut public_key = [0_u8; 32];
            rng.fill_bytes(&mut public_key);
            assert_ne!(Address::derive(1, &public_key).as_bytes(), &public_key);
        }
    }

    #[test]
    fn hash_changes_with_input() {
        assert_ne!(Hash::digest(b"a"), Hash::digest(b"b"));
    }
}
