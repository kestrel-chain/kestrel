//! Validator configuration, deterministic genesis, and operational readiness helpers.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use consensus::{ConsensusError, SlashingPolicy, Validator, ValidatorSet};
use crypto::{BLS12381_SCHEME_ID, Bls12381Scheme, CryptoError, ED25519_SCHEME_ID};
use serde::{Deserialize, Serialize};
use state::{StateConfig, StateError, StateTree};
use thiserror::Error;
use types::{Address, Hash, Object, SchemeId};

mod coordinator;
mod lifecycle;
mod pipeline;

pub use coordinator::{
    ConsensusCoordinator, CoordinatorConfig, CoordinatorError, CoordinatorFaults,
    CoordinatorOutcome, ProposalTransactionSource,
};
pub use lifecycle::{
    BlockLifecycle, DurableBlockRecord, LifecycleError, PropagatedBlock, SignedExecutionPayload,
    TransactionValidator, signed_transaction_id,
};
pub use pipeline::{
    PipelineError, ShredStats, Stage2Pipeline, Stage2PipelineConfig, Stage2PipelineHandle,
};

pub const GENESIS_FORMAT_VERSION: u16 = 1;
pub const MIN_VALIDATORS: usize = 4;
pub const MAX_VALIDATORS: usize = 500;

/// Public validator identity and advertised endpoints committed at genesis.
///
/// `network_address` carries the raw-TCP consensus transport
/// (`ConsensusCoordinator`); `gossip_peer_id`/`gossip_address` carry the
/// separate libp2p transport (`network::NetworkNode`) used for transaction
/// gossip and `KestrelCast` shred relay. The two transports are a deliberate,
/// documented split (see `docs/TECH_DEBT.md` TD-003), not an oversight.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenesisValidator {
    pub name: String,
    pub validator: Validator,
    pub network_address: String,
    pub rpc_address: String,
    pub gossip_peer_id: String,
    pub gossip_address: String,
}

/// Canonical public-testnet genesis input. It never contains private keys.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GenesisDocument {
    pub format_version: u16,
    pub chain_id: String,
    pub genesis_unix_ms: u64,
    pub blocks_per_epoch: u64,
    pub state_config: StateConfig,
    pub active_signature_schemes: Vec<SchemeId>,
    pub equivocation_slash_basis_points: u16,
    pub validators: Vec<GenesisValidator>,
    pub initial_objects: Vec<Object>,
    /// Fee-ledger balances seeded at genesis, keyed by sender address. Absent
    /// senders start at a zero balance and must be funded before any of their
    /// transactions can settle (see `docs/TECH_DEBT.md` TD-011).
    #[serde(default)]
    pub initial_fee_balances: BTreeMap<Address, u128>,
}

/// Validated roots and stake table derived solely from canonical genesis.
#[derive(Clone, Debug)]
pub struct ValidatedGenesis {
    pub genesis_hash: Hash,
    pub state_root: Hash,
    pub validators: ValidatorSet,
}

impl GenesisDocument {
    /// Validates every consensus/state parameter and derives deterministic roots.
    ///
    /// # Errors
    ///
    /// Rejects malformed identifiers, endpoint collisions, unsupported schemes,
    /// invalid validator keys/stake, invalid rent, or duplicate objects.
    pub fn validate(&self) -> Result<ValidatedGenesis, GenesisError> {
        if self.format_version != GENESIS_FORMAT_VERSION {
            return Err(GenesisError::UnsupportedFormat(self.format_version));
        }
        if self.chain_id.is_empty()
            || self.chain_id.len() > 64
            || !self
                .chain_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(GenesisError::InvalidChainId);
        }
        if self.genesis_unix_ms == 0 {
            return Err(GenesisError::InvalidGenesisTime);
        }
        if self.blocks_per_epoch == 0 {
            return Err(GenesisError::InvalidBlocksPerEpoch);
        }
        if !(MIN_VALIDATORS..=MAX_VALIDATORS).contains(&self.validators.len()) {
            return Err(GenesisError::ValidatorCount(self.validators.len()));
        }
        let schemes = self
            .active_signature_schemes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if schemes.len() != self.active_signature_schemes.len()
            || !schemes.contains(&ED25519_SCHEME_ID)
            || !schemes.contains(&BLS12381_SCHEME_ID)
            || schemes.iter().any(|scheme| !matches!(*scheme, 1 | 2))
        {
            return Err(GenesisError::InvalidSignatureSchemes);
        }
        SlashingPolicy::new(self.equivocation_slash_basis_points)?;

        let mut names = BTreeSet::new();
        let mut network_addresses = BTreeSet::new();
        let mut rpc_addresses = BTreeSet::new();
        let mut gossip_peer_ids = BTreeSet::new();
        let mut gossip_addresses = BTreeSet::new();
        for validator in &self.validators {
            if validator.name.trim().is_empty()
                || validator.network_address.trim().is_empty()
                || validator.rpc_address.trim().is_empty()
                || !names.insert(validator.name.clone())
                || !network_addresses.insert(validator.network_address.clone())
                || !rpc_addresses.insert(validator.rpc_address.clone())
                || validator.gossip_peer_id.parse::<libp2p::PeerId>().is_err()
                || validator
                    .gossip_address
                    .parse::<libp2p::Multiaddr>()
                    .is_err()
                || !gossip_peer_ids.insert(validator.gossip_peer_id.clone())
                || !gossip_addresses.insert(validator.gossip_address.clone())
            {
                return Err(GenesisError::InvalidValidatorMetadata);
            }
        }
        // Genesis's own `Validator.proof_of_possession`/`public_key` fields are
        // already BLS-specific by construction, so this one-time bootstrap
        // step is not part of the "swap the consensus signature scheme
        // without touching consensus logic" concern that motivates injecting
        // `AggregateSignatureScheme` everywhere else (see docs/TECH_DEBT.md).
        let validators = ValidatorSet::new(
            self.validators
                .iter()
                .map(|entry| entry.validator.clone())
                .collect(),
            &Bls12381Scheme,
        )?;
        let mut state = StateTree::new(self.state_config)?;
        let mut objects = self.initial_objects.clone();
        objects.sort_unstable_by_key(|object| object.id);
        for object in objects {
            state.create_object(object)?;
        }
        let state_root = state.root()?;
        let genesis_hash = Hash::digest(self.canonical_bytes()?);
        Ok(ValidatedGenesis {
            genesis_hash,
            state_root,
            validators,
        })
    }

    /// Returns a stable encoding independent of input validator/object order.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical serialization fails.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, GenesisError> {
        let mut normalized = self.clone();
        normalized
            .validators
            .sort_unstable_by_key(|entry| entry.validator.id);
        normalized
            .initial_objects
            .sort_unstable_by_key(|object| object.id);
        normalized.active_signature_schemes.sort_unstable();
        bcs::to_bytes(&normalized).map_err(|error| GenesisError::Encoding(error.to_string()))
    }

    /// Loads and validates a JSON genesis document.
    ///
    /// # Errors
    ///
    /// Returns I/O, JSON, or protocol validation errors.
    pub fn load_json(path: impl AsRef<Path>) -> Result<Self, GenesisError> {
        let bytes = fs::read(path)?;
        let document = serde_json::from_slice::<Self>(&bytes)?;
        document.validate()?;
        Ok(document)
    }

    /// Writes stable, human-readable JSON after validation.
    ///
    /// # Errors
    ///
    /// Returns validation, serialization, or I/O errors.
    pub fn write_json(&self, path: impl AsRef<Path>) -> Result<(), GenesisError> {
        self.validate()?;
        let mut normalized = self.clone();
        normalized
            .validators
            .sort_unstable_by_key(|entry| entry.validator.id);
        normalized
            .initial_objects
            .sort_unstable_by_key(|object| object.id);
        normalized.active_signature_schemes.sort_unstable();
        let bytes = serde_json::to_vec_pretty(&normalized)?;
        fs::write(path, bytes)?;
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum GenesisError {
    #[error("unsupported genesis format version {0}")]
    UnsupportedFormat(u16),
    #[error("chain ID must be 1-64 ASCII letters, digits, '-' or '_'")]
    InvalidChainId,
    #[error("genesis timestamp must be nonzero")]
    InvalidGenesisTime,
    #[error("blocks per epoch must be nonzero")]
    InvalidBlocksPerEpoch,
    #[error("validator count {0} is outside the supported 4-500 range")]
    ValidatorCount(usize),
    #[error("active schemes must uniquely contain the supported Ed25519 and BLS IDs")]
    InvalidSignatureSchemes,
    #[error("validator names and endpoints must be nonempty and unique")]
    InvalidValidatorMetadata,
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error("canonical genesis encoding failed: {0}")]
    Encoding(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use consensus::Validator;
    use crypto::{Bls12381Scheme, SignatureScheme};
    use tempfile::TempDir;
    use types::{Address, Owner};

    use super::{GENESIS_FORMAT_VERSION, GenesisDocument, GenesisValidator};

    #[test]
    fn genesis_is_order_independent_and_round_trips_json() {
        let first = fixture(false);
        let second = fixture(true);
        let first_validated = first.validate().unwrap();
        let second_validated = second.validate().unwrap();
        assert_eq!(first_validated.genesis_hash, second_validated.genesis_hash);
        assert_eq!(first_validated.state_root, second_validated.state_root);

        let directory = TempDir::new().unwrap();
        let path = directory.path().join("genesis.json");
        first.write_json(&path).unwrap();
        assert_eq!(
            GenesisDocument::load_json(path)
                .unwrap()
                .canonical_bytes()
                .unwrap(),
            first.canonical_bytes().unwrap()
        );
    }

    fn fixture(reverse: bool) -> GenesisDocument {
        let scheme = Bls12381Scheme;
        let mut validators = (1_u8..=4)
            .map(|index| {
                let private_key = [index; 32];
                let gossip_identity =
                    libp2p::identity::Keypair::ed25519_from_bytes([index; 32]).unwrap();
                GenesisValidator {
                    name: format!("validator-{index}"),
                    validator: Validator {
                        id: types::Hash::digest([index]),
                        stake: 25,
                        public_key: scheme.public_key(&private_key).unwrap(),
                        proof_of_possession: scheme.proof_of_possession(&private_key).unwrap(),
                    },
                    network_address: format!("127.0.0.1:{}", 9_000 + u16::from(index)),
                    rpc_address: format!("127.0.0.1:{}", 10_000 + u16::from(index)),
                    gossip_peer_id: gossip_identity.public().to_peer_id().to_string(),
                    gossip_address: format!("/ip4/127.0.0.1/tcp/{}", 11_000 + u16::from(index)),
                }
            })
            .collect::<Vec<_>>();
        let mut objects = (1_u8..=2)
            .map(|index| types::Object {
                id: types::Hash::digest([index, 9]),
                owner: Owner::Single(Address::from_bytes([index; 32])),
                type_tag: "genesis::Balance".to_owned(),
                version: 0,
                data: vec![index],
                rent_balance: 100,
            })
            .collect::<Vec<_>>();
        if reverse {
            validators.reverse();
            objects.reverse();
        }
        GenesisDocument {
            format_version: GENESIS_FORMAT_VERSION,
            chain_id: "kestrel-testnet-1".to_owned(),
            genesis_unix_ms: 1_800_000_000_000,
            blocks_per_epoch: 100,
            state_config: state::StateConfig::default(),
            active_signature_schemes: vec![1, 2],
            equivocation_slash_basis_points: 5_000,
            validators,
            initial_objects: objects,
            initial_fee_balances: BTreeMap::new(),
        }
    }
}
