use std::collections::{BTreeMap, BTreeSet};

use rand::{
    SeedableRng,
    distributions::{Distribution, WeightedIndex},
    rngs::StdRng,
};
use reed_solomon_erasure::galois_8::ReedSolomon;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use types::Hash;

const MAX_SHARDS: usize = 256;

/// Erasure-coding parameters for one `KestrelCast` block.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KestrelCastConfig {
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl Default for KestrelCastConfig {
    fn default() -> Self {
        Self {
            data_shards: 10,
            parity_shards: 10,
        }
    }
}

/// One independently gossipable erasure-coded block fragment.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Shred {
    pub block_id: Hash,
    pub index: u16,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub original_len: u64,
    pub payload: Vec<u8>,
}

/// Validator eligible to relay shreds, identified independently of transport.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelayCandidate {
    pub id: Hash,
    pub stake: u64,
}

/// Deterministic single-layer relay assignment for one block.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RelayPlan {
    pub relays: Vec<Hash>,
    pub assignments: BTreeMap<Hash, Vec<Shred>>,
}

/// `KestrelCast` codec and stake-weighted relay planner.
#[derive(Clone, Debug)]
pub struct KestrelCast {
    config: KestrelCastConfig,
    codec: ReedSolomon,
}

/// Encoding, reconstruction, and relay-selection failures.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum KestrelCastError {
    #[error("data and parity shard counts must both be greater than zero")]
    EmptyShardClass,
    #[error("total shard count exceeds the GF(2^8) maximum of {MAX_SHARDS}")]
    TooManyShards,
    #[error("block length cannot be represented on this platform")]
    BlockTooLarge,
    #[error("shred metadata is inconsistent")]
    InconsistentMetadata,
    #[error("shred {0} is duplicated")]
    DuplicateShred(u16),
    #[error("shred index {index} is outside total shard count {total}")]
    InvalidShredIndex { index: u16, total: usize },
    #[error("only {available} distinct shreds are available; {required} are required")]
    InsufficientShreds { available: usize, required: usize },
    #[error("Reed-Solomon operation failed: {0}")]
    ReedSolomon(String),
    #[error("reconstructed block failed its integrity check")]
    IntegrityMismatch,
    #[error("relay count and replication factor must both be greater than zero")]
    InvalidRelayPlan,
    #[error("relay candidates must have unique IDs and nonzero stake")]
    InvalidRelayCandidate,
}

impl KestrelCast {
    /// Creates a codec with an approximately 50% reconstruction threshold by
    /// default (10 data plus 10 parity shreds).
    ///
    /// # Errors
    ///
    /// Rejects zero-sized shard classes and more than 256 total shreds.
    pub fn new(config: KestrelCastConfig) -> Result<Self, KestrelCastError> {
        if config.data_shards == 0 || config.parity_shards == 0 {
            return Err(KestrelCastError::EmptyShardClass);
        }
        if config
            .data_shards
            .checked_add(config.parity_shards)
            .is_none_or(|total| total > MAX_SHARDS)
        {
            return Err(KestrelCastError::TooManyShards);
        }
        let codec = ReedSolomon::new(config.data_shards, config.parity_shards)
            .map_err(|error| KestrelCastError::ReedSolomon(error.to_string()))?;
        Ok(Self { config, codec })
    }

    #[must_use]
    pub const fn config(&self) -> KestrelCastConfig {
        self.config
    }

    /// Splits a block into equal-sized data and parity shreds.
    ///
    /// # Errors
    ///
    /// Returns an error if the block length is not representable or encoding fails.
    pub fn encode(&self, block: &[u8]) -> Result<Vec<Shred>, KestrelCastError> {
        let original_len =
            u64::try_from(block.len()).map_err(|_| KestrelCastError::BlockTooLarge)?;
        let shard_len = block.len().div_ceil(self.config.data_shards).max(1);
        let mut shards = vec![vec![0_u8; shard_len]; self.codec.total_shard_count()];
        for (chunk, shard) in block.chunks(shard_len).zip(&mut shards) {
            shard[..chunk.len()].copy_from_slice(chunk);
        }
        self.codec
            .encode(&mut shards)
            .map_err(|error| KestrelCastError::ReedSolomon(error.to_string()))?;
        let block_id = Hash::digest(block);
        shards
            .into_iter()
            .enumerate()
            .map(|(index, payload)| {
                Ok(Shred {
                    block_id,
                    index: u16::try_from(index).map_err(|_| KestrelCastError::TooManyShards)?,
                    data_shards: u16::try_from(self.config.data_shards)
                        .map_err(|_| KestrelCastError::TooManyShards)?,
                    parity_shards: u16::try_from(self.config.parity_shards)
                        .map_err(|_| KestrelCastError::TooManyShards)?,
                    original_len,
                    payload,
                })
            })
            .collect()
    }

    /// Reconstructs and integrity-checks a block from any sufficient subset.
    ///
    /// # Errors
    ///
    /// Rejects inconsistent, duplicate, malformed, or insufficient shreds.
    pub fn reconstruct(shreds: &[Shred]) -> Result<Vec<u8>, KestrelCastError> {
        let first = shreds.first().ok_or(KestrelCastError::InsufficientShreds {
            available: 0,
            required: 1,
        })?;
        let data_shards = usize::from(first.data_shards);
        let parity_shards = usize::from(first.parity_shards);
        let total = data_shards + parity_shards;
        let codec = Self::new(KestrelCastConfig {
            data_shards,
            parity_shards,
        })?;
        let shard_len = first.payload.len();
        let mut available = BTreeSet::new();
        let mut slots = vec![None; total];
        for shred in shreds {
            if shred.block_id != first.block_id
                || shred.data_shards != first.data_shards
                || shred.parity_shards != first.parity_shards
                || shred.original_len != first.original_len
                || shred.payload.len() != shard_len
            {
                return Err(KestrelCastError::InconsistentMetadata);
            }
            let index = usize::from(shred.index);
            if index >= total {
                return Err(KestrelCastError::InvalidShredIndex {
                    index: shred.index,
                    total,
                });
            }
            if !available.insert(shred.index) {
                return Err(KestrelCastError::DuplicateShred(shred.index));
            }
            slots[index] = Some(shred.payload.clone());
        }
        if available.len() < data_shards {
            return Err(KestrelCastError::InsufficientShreds {
                available: available.len(),
                required: data_shards,
            });
        }
        codec
            .codec
            .reconstruct(&mut slots)
            .map_err(|error| KestrelCastError::ReedSolomon(error.to_string()))?;
        let mut block = Vec::with_capacity(data_shards.saturating_mul(shard_len));
        for shard in slots.into_iter().take(data_shards) {
            block.extend(shard.ok_or_else(|| {
                KestrelCastError::ReedSolomon("data shard was not reconstructed".to_owned())
            })?);
        }
        let original_len =
            usize::try_from(first.original_len).map_err(|_| KestrelCastError::BlockTooLarge)?;
        if original_len > block.len() {
            return Err(KestrelCastError::InconsistentMetadata);
        }
        block.truncate(original_len);
        if Hash::digest(&block) != first.block_id {
            return Err(KestrelCastError::IntegrityMismatch);
        }
        Ok(block)
    }

    /// Selects stake-weighted relays deterministically from the block ID and
    /// assigns every shred to `replication_factor` relays. Relays form one layer:
    /// they fan out directly to validators and never forward through other relays.
    ///
    /// # Errors
    ///
    /// Rejects empty/duplicate candidates, zero stake, and zero sizing inputs.
    pub fn relay_plan(
        block_id: Hash,
        shreds: &[Shred],
        candidates: &[RelayCandidate],
        relay_count: usize,
        replication_factor: usize,
    ) -> Result<RelayPlan, KestrelCastError> {
        if relay_count == 0 || replication_factor == 0 {
            return Err(KestrelCastError::InvalidRelayPlan);
        }
        let mut ids = BTreeSet::new();
        if candidates.is_empty()
            || candidates
                .iter()
                .any(|candidate| candidate.stake == 0 || !ids.insert(candidate.id))
        {
            return Err(KestrelCastError::InvalidRelayCandidate);
        }
        let mut remaining = candidates.to_vec();
        remaining.sort_unstable_by_key(|candidate| candidate.id);
        let mut random = StdRng::from_seed(*block_id.as_bytes());
        let mut relays = Vec::with_capacity(relay_count.min(remaining.len()));
        while relays.len() < relay_count && !remaining.is_empty() {
            let distribution =
                WeightedIndex::new(remaining.iter().map(|candidate| candidate.stake))
                    .map_err(|_| KestrelCastError::InvalidRelayCandidate)?;
            let selected = distribution.sample(&mut random);
            relays.push(remaining.swap_remove(selected).id);
        }
        let copies = replication_factor.min(relays.len());
        let mut assignments: BTreeMap<_, Vec<_>> = relays
            .iter()
            .copied()
            .map(|relay| (relay, Vec::new()))
            .collect();
        for (offset, shred) in shreds.iter().enumerate() {
            for copy in 0..copies {
                let relay = relays[(offset + copy) % relays.len()];
                assignments.entry(relay).or_default().push(shred.clone());
            }
        }
        Ok(RelayPlan {
            relays,
            assignments,
        })
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use rand::{SeedableRng, rngs::StdRng, seq::SliceRandom};

    use super::{KestrelCast, KestrelCastConfig, KestrelCastError, RelayCandidate};
    use types::Hash;

    proptest! {
        #[test]
        fn any_half_of_shreds_reconstructs(payload in prop::collection::vec(any::<u8>(), 0..32_768), seed in any::<u64>()) {
            let codec = KestrelCast::new(KestrelCastConfig::default()).unwrap();
            let mut shreds = codec.encode(&payload).unwrap();
            shreds.shuffle(&mut StdRng::seed_from_u64(seed));
            shreds.truncate(codec.config().data_shards);
            prop_assert_eq!(KestrelCast::reconstruct(&shreds).unwrap(), payload);
        }
    }

    #[test]
    fn corruption_is_detected_after_reconstruction() {
        let codec = KestrelCast::new(KestrelCastConfig::default()).unwrap();
        let mut shreds = codec.encode(b"integrity matters").unwrap();
        shreds.truncate(codec.config().data_shards);
        shreds[0].payload[0] ^= 0xff;
        assert_eq!(
            KestrelCast::reconstruct(&shreds),
            Err(KestrelCastError::IntegrityMismatch)
        );
    }

    #[test]
    fn relay_plan_is_deterministic_and_replicated() {
        let codec = KestrelCast::new(KestrelCastConfig::default()).unwrap();
        let shreds = codec.encode(b"block").unwrap();
        let candidates: Vec<_> = (0_u8..20)
            .map(|seed| RelayCandidate {
                id: Hash::digest([seed]),
                stake: u64::from(seed) + 1,
            })
            .collect();
        let first =
            KestrelCast::relay_plan(shreds[0].block_id, &shreds, &candidates, 12, 2).unwrap();
        let second =
            KestrelCast::relay_plan(shreds[0].block_id, &shreds, &candidates, 12, 2).unwrap();
        let mut reversed = candidates.clone();
        reversed.reverse();
        let reordered =
            KestrelCast::relay_plan(shreds[0].block_id, &shreds, &reversed, 12, 2).unwrap();
        assert_eq!(first, second);
        assert_eq!(first, reordered);
        assert_eq!(first.relays.len(), 12);
        assert_eq!(
            first.assignments.values().map(Vec::len).sum::<usize>(),
            shreds.len() * 2
        );
    }
}
