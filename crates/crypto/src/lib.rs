//! Crypto-agile signature primitives for Kestrel.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use blst::{BLST_ERROR, min_pk};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use rand::{CryptoRng, RngCore, rngs::OsRng};
use thiserror::Error;
use types::{Address, SchemeId};

/// Stable identifier for Ed25519 signatures.
pub const ED25519_SCHEME_ID: SchemeId = 1;
/// Stable identifier for BLS12-381 validator vote signatures.
pub const BLS12381_SCHEME_ID: SchemeId = 2;

const BLS_SIGNATURE_DST: &[u8] = b"KESTREL_BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_V1";
const BLS_POP_DST: &[u8] = b"KESTREL_BLS_POP_BLS12381G2_XMD:SHA-256_SSWU_RO_V1";

/// Errors returned by signature schemes and their registry.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum CryptoError {
    #[error("invalid private key length: expected {expected}, received {actual}")]
    InvalidPrivateKeyLength { expected: usize, actual: usize },
    #[error("invalid private key material")]
    InvalidPrivateKey,
    #[error("invalid public key encoding")]
    InvalidPublicKey,
    #[error("invalid signature encoding")]
    InvalidSignature,
    #[error("signature verification failed")]
    VerificationFailed,
    #[error("aggregate signature requires at least one signer")]
    EmptyAggregate,
    #[error("BLS proof of possession verification failed")]
    InvalidProofOfPossession,
    #[error("signature scheme {0} is not registered")]
    UnknownScheme(SchemeId),
    #[error("signature scheme {0} is registered but inactive")]
    InactiveScheme(SchemeId),
    #[error("signature scheme {0} was registered more than once")]
    DuplicateScheme(SchemeId),
    #[error("genesis activates unregistered signature scheme {0}")]
    ActivatedSchemeMissing(SchemeId),
}

/// Object-safe interface implemented by every transaction signature scheme.
pub trait SignatureScheme: Send + Sync {
    /// Signs a message using the scheme's canonical private-key encoding.
    ///
    /// # Errors
    ///
    /// Returns an error when the private key is not canonically encoded.
    fn sign(&self, private_key: &[u8], message: &[u8]) -> Result<Vec<u8>, CryptoError>;

    /// Verifies a signature using canonical public-key and signature encodings.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed inputs or a failed verification.
    fn verify(
        &self,
        public_key: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), CryptoError>;

    /// Derives the canonical public key from a private key.
    ///
    /// # Errors
    ///
    /// Returns an error when the private key is not canonically encoded.
    fn public_key(&self, private_key: &[u8]) -> Result<Vec<u8>, CryptoError>;

    /// Generates a canonical private key using the provided secure RNG.
    fn generate_private_key(&self, rng: &mut dyn SecureRng) -> Vec<u8>;

    fn public_key_size(&self) -> usize;
    fn signature_size(&self) -> usize;
    fn scheme_id(&self) -> SchemeId;

    /// Derives the crypto-agile account address for a public key.
    ///
    /// # Errors
    ///
    /// Returns an error when the public key has the wrong encoded length.
    fn address(&self, public_key: &[u8]) -> Result<Address, CryptoError> {
        if public_key.len() != self.public_key_size() {
            return Err(CryptoError::InvalidPublicKey);
        }
        Ok(Address::derive(self.scheme_id(), public_key))
    }
}

/// Object-safe interface for a signature scheme whose signatures can be
/// combined into one compact aggregate — the capability consensus needs for
/// quorum certificates. Not every `SignatureScheme` supports this (Ed25519
/// does not), so it is a separate, opt-in extension rather than part of the
/// base trait.
pub trait AggregateSignatureScheme: SignatureScheme {
    /// Proves the holder of a public key knows its private key, guarding
    /// against rogue-key attacks on signature aggregation.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed private-key material.
    fn proof_of_possession(&self, private_key: &[u8]) -> Result<Vec<u8>, CryptoError>;

    /// Verifies a proof of possession before its public key is admitted into
    /// a validator set.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed inputs or a failed proof.
    fn verify_proof_of_possession(
        &self,
        public_key: &[u8],
        proof: &[u8],
    ) -> Result<(), CryptoError>;

    /// Aggregates individual signatures over the same message into one.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty list or malformed signature.
    fn aggregate(&self, signatures: &[Vec<u8>]) -> Result<Vec<u8>, CryptoError>;

    /// Verifies an aggregate signature over one message against every
    /// signer's proof-of-possession-validated public key.
    ///
    /// # Errors
    ///
    /// Returns an error for empty/malformed inputs or a failed aggregate.
    fn verify_aggregate(
        &self,
        public_keys: &[Vec<u8>],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), CryptoError>;
}

/// Trait alias for RNGs suitable for key generation.
pub trait SecureRng: CryptoRng + RngCore {}
impl<T: CryptoRng + RngCore> SecureRng for T {}

/// Ed25519 transaction signatures.
#[derive(Clone, Copy, Debug, Default)]
pub struct Ed25519Scheme;

impl Ed25519Scheme {
    pub const PRIVATE_KEY_SIZE: usize = 32;
    pub const PUBLIC_KEY_SIZE: usize = 32;
    pub const SIGNATURE_SIZE: usize = 64;

    fn signing_key(private_key: &[u8]) -> Result<SigningKey, CryptoError> {
        let bytes: &[u8; Self::PRIVATE_KEY_SIZE] =
            private_key
                .try_into()
                .map_err(|_| CryptoError::InvalidPrivateKeyLength {
                    expected: Self::PRIVATE_KEY_SIZE,
                    actual: private_key.len(),
                })?;
        Ok(SigningKey::from_bytes(bytes))
    }

    /// Generates a private key from the operating system CSPRNG.
    #[must_use]
    pub fn generate_os_private_key() -> Vec<u8> {
        Self.generate_private_key(&mut OsRng)
    }
}

impl SignatureScheme for Ed25519Scheme {
    fn sign(&self, private_key: &[u8], message: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(Self::signing_key(private_key)?
            .sign(message)
            .to_bytes()
            .to_vec())
    }

    fn verify(
        &self,
        public_key: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), CryptoError> {
        let public_key: &[u8; Self::PUBLIC_KEY_SIZE] = public_key
            .try_into()
            .map_err(|_| CryptoError::InvalidPublicKey)?;
        let verifying_key =
            VerifyingKey::from_bytes(public_key).map_err(|_| CryptoError::InvalidPublicKey)?;
        let signature =
            Signature::from_slice(signature).map_err(|_| CryptoError::InvalidSignature)?;
        verifying_key
            .verify_strict(message, &signature)
            .map_err(|_| CryptoError::VerificationFailed)
    }

    fn public_key(&self, private_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(Self::signing_key(private_key)?
            .verifying_key()
            .to_bytes()
            .to_vec())
    }

    fn generate_private_key(&self, rng: &mut dyn SecureRng) -> Vec<u8> {
        let mut bytes = [0_u8; Self::PRIVATE_KEY_SIZE];
        rng.fill_bytes(&mut bytes);
        bytes.to_vec()
    }

    fn public_key_size(&self) -> usize {
        Self::PUBLIC_KEY_SIZE
    }

    fn signature_size(&self) -> usize {
        Self::SIGNATURE_SIZE
    }

    fn scheme_id(&self) -> SchemeId {
        ED25519_SCHEME_ID
    }
}

/// BLS12-381 signatures in the minimum-public-key configuration.
///
/// Consensus public keys are admitted only after proof-of-possession
/// verification, which makes same-message fast aggregate verification safe
/// against rogue-key attacks.
#[derive(Clone, Copy, Debug, Default)]
pub struct Bls12381Scheme;

impl Bls12381Scheme {
    pub const PRIVATE_KEY_SIZE: usize = 32;
    pub const PUBLIC_KEY_SIZE: usize = 48;
    pub const SIGNATURE_SIZE: usize = 96;

    fn secret_key(private_key: &[u8]) -> Result<min_pk::SecretKey, CryptoError> {
        if private_key.len() != Self::PRIVATE_KEY_SIZE {
            return Err(CryptoError::InvalidPrivateKeyLength {
                expected: Self::PRIVATE_KEY_SIZE,
                actual: private_key.len(),
            });
        }
        min_pk::SecretKey::key_gen(private_key, &[]).map_err(|_| CryptoError::InvalidPrivateKey)
    }

    /// Generates validator private-key material from the operating system CSPRNG.
    #[must_use]
    pub fn generate_os_private_key() -> Vec<u8> {
        Self.generate_private_key(&mut OsRng)
    }

    /// Creates a proof that the holder of a validator public key knows its
    /// private key.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed private-key material.
    pub fn proof_of_possession(&self, private_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
        let secret = Self::secret_key(private_key)?;
        let public_key = secret.sk_to_pk().to_bytes();
        Ok(secret
            .sign(&public_key, BLS_POP_DST, &[])
            .to_bytes()
            .to_vec())
    }

    /// Verifies a validator's proof of possession before its key is admitted.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed inputs or a failed proof.
    pub fn verify_proof_of_possession(
        &self,
        public_key: &[u8],
        proof: &[u8],
    ) -> Result<(), CryptoError> {
        let public_key =
            min_pk::PublicKey::from_bytes(public_key).map_err(|_| CryptoError::InvalidPublicKey)?;
        let proof =
            min_pk::Signature::from_bytes(proof).map_err(|_| CryptoError::InvalidSignature)?;
        if proof.verify(
            true,
            &public_key.to_bytes(),
            BLS_POP_DST,
            &[],
            &public_key,
            true,
        ) == BLST_ERROR::BLST_SUCCESS
        {
            Ok(())
        } else {
            Err(CryptoError::InvalidProofOfPossession)
        }
    }

    /// Aggregates individual signatures over the same canonical vote message.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty list or malformed signature.
    pub fn aggregate(&self, signatures: &[Vec<u8>]) -> Result<Vec<u8>, CryptoError> {
        if signatures.is_empty() {
            return Err(CryptoError::EmptyAggregate);
        }
        let parsed = signatures
            .iter()
            .map(|signature| {
                min_pk::Signature::from_bytes(signature).map_err(|_| CryptoError::InvalidSignature)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let references = parsed.iter().collect::<Vec<_>>();
        let aggregate = min_pk::AggregateSignature::aggregate(&references, true)
            .map_err(|_| CryptoError::InvalidSignature)?;
        Ok(aggregate.to_signature().to_bytes().to_vec())
    }

    /// Verifies an aggregate over one message using PoP-validated public keys.
    ///
    /// # Errors
    ///
    /// Returns an error for empty/malformed inputs or a failed aggregate.
    pub fn verify_aggregate(
        &self,
        public_keys: &[Vec<u8>],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), CryptoError> {
        if public_keys.is_empty() {
            return Err(CryptoError::EmptyAggregate);
        }
        let public_keys = public_keys
            .iter()
            .map(|key| {
                min_pk::PublicKey::from_bytes(key).map_err(|_| CryptoError::InvalidPublicKey)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let references = public_keys.iter().collect::<Vec<_>>();
        let signature =
            min_pk::Signature::from_bytes(signature).map_err(|_| CryptoError::InvalidSignature)?;
        if signature.fast_aggregate_verify(true, message, BLS_SIGNATURE_DST, &references)
            == BLST_ERROR::BLST_SUCCESS
        {
            Ok(())
        } else {
            Err(CryptoError::VerificationFailed)
        }
    }
}

impl AggregateSignatureScheme for Bls12381Scheme {
    fn proof_of_possession(&self, private_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
        self.proof_of_possession(private_key)
    }

    fn verify_proof_of_possession(
        &self,
        public_key: &[u8],
        proof: &[u8],
    ) -> Result<(), CryptoError> {
        self.verify_proof_of_possession(public_key, proof)
    }

    fn aggregate(&self, signatures: &[Vec<u8>]) -> Result<Vec<u8>, CryptoError> {
        self.aggregate(signatures)
    }

    fn verify_aggregate(
        &self,
        public_keys: &[Vec<u8>],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), CryptoError> {
        self.verify_aggregate(public_keys, message, signature)
    }
}

impl SignatureScheme for Bls12381Scheme {
    fn sign(&self, private_key: &[u8], message: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(Self::secret_key(private_key)?
            .sign(message, BLS_SIGNATURE_DST, &[])
            .to_bytes()
            .to_vec())
    }

    fn verify(
        &self,
        public_key: &[u8],
        message: &[u8],
        signature: &[u8],
    ) -> Result<(), CryptoError> {
        let public_key =
            min_pk::PublicKey::from_bytes(public_key).map_err(|_| CryptoError::InvalidPublicKey)?;
        let signature =
            min_pk::Signature::from_bytes(signature).map_err(|_| CryptoError::InvalidSignature)?;
        if signature.verify(true, message, BLS_SIGNATURE_DST, &[], &public_key, true)
            == BLST_ERROR::BLST_SUCCESS
        {
            Ok(())
        } else {
            Err(CryptoError::VerificationFailed)
        }
    }

    fn public_key(&self, private_key: &[u8]) -> Result<Vec<u8>, CryptoError> {
        Ok(Self::secret_key(private_key)?
            .sk_to_pk()
            .to_bytes()
            .to_vec())
    }

    fn generate_private_key(&self, rng: &mut dyn SecureRng) -> Vec<u8> {
        let mut bytes = [0_u8; Self::PRIVATE_KEY_SIZE];
        rng.fill_bytes(&mut bytes);
        bytes.to_vec()
    }

    fn public_key_size(&self) -> usize {
        Self::PUBLIC_KEY_SIZE
    }

    fn signature_size(&self) -> usize {
        Self::SIGNATURE_SIZE
    }

    fn scheme_id(&self) -> SchemeId {
        BLS12381_SCHEME_ID
    }
}

/// Immutable registry configured by the genesis signature-scheme allowlist.
pub struct SchemeRegistry {
    schemes: HashMap<SchemeId, Arc<dyn SignatureScheme>>,
    active: BTreeSet<SchemeId>,
}

impl SchemeRegistry {
    /// Constructs a registry and validates its static governance activation list.
    ///
    /// # Errors
    ///
    /// Returns an error for duplicate scheme IDs or activation of an unregistered scheme.
    pub fn from_genesis_config(
        schemes: impl IntoIterator<Item = Arc<dyn SignatureScheme>>,
        active: impl IntoIterator<Item = SchemeId>,
    ) -> Result<Self, CryptoError> {
        let mut registered = HashMap::new();
        for scheme in schemes {
            let id = scheme.scheme_id();
            if registered.insert(id, scheme).is_some() {
                return Err(CryptoError::DuplicateScheme(id));
            }
        }

        let active: BTreeSet<_> = active.into_iter().collect();
        if let Some(id) = active.iter().find(|id| !registered.contains_key(id)) {
            return Err(CryptoError::ActivatedSchemeMissing(*id));
        }

        // TODO(pqc): register ML-DSA/Falcon or hybrid schemes here after assigning
        // protocol-stable scheme IDs; no signing call sites should need to change.
        Ok(Self {
            schemes: registered,
            active,
        })
    }

    /// Phase 0 registry with Ed25519 active from genesis.
    #[must_use]
    pub fn phase_zero() -> Self {
        Self {
            schemes: HashMap::from([(
                ED25519_SCHEME_ID,
                Arc::new(Ed25519Scheme) as Arc<dyn SignatureScheme>,
            )]),
            active: BTreeSet::from([ED25519_SCHEME_ID]),
        }
    }

    /// Looks up an active signature scheme.
    ///
    /// # Errors
    ///
    /// Returns an error when the scheme is unregistered or inactive.
    pub fn get(&self, id: SchemeId) -> Result<&dyn SignatureScheme, CryptoError> {
        let scheme = self
            .schemes
            .get(&id)
            .ok_or(CryptoError::UnknownScheme(id))?;
        if !self.active.contains(&id) {
            return Err(CryptoError::InactiveScheme(id));
        }
        Ok(scheme.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{
        Bls12381Scheme, CryptoError, ED25519_SCHEME_ID, Ed25519Scheme, SchemeRegistry,
        SignatureScheme,
    };

    #[test]
    fn ed25519_sign_verify_round_trip() {
        let scheme = Ed25519Scheme;
        let private_key = [7_u8; Ed25519Scheme::PRIVATE_KEY_SIZE];
        let public_key = scheme.public_key(&private_key).unwrap();
        let signature = scheme.sign(&private_key, b"kestrel").unwrap();
        assert_eq!(public_key.len(), scheme.public_key_size());
        assert_eq!(signature.len(), scheme.signature_size());
        assert_eq!(scheme.verify(&public_key, b"kestrel", &signature), Ok(()));
        assert_eq!(
            scheme.verify(&public_key, b"tampered", &signature),
            Err(CryptoError::VerificationFailed)
        );
    }

    #[test]
    fn registry_enforces_activation() {
        let registry = SchemeRegistry::from_genesis_config(
            [Arc::new(Ed25519Scheme) as Arc<dyn SignatureScheme>],
            [],
        )
        .unwrap();
        assert!(matches!(
            registry.get(ED25519_SCHEME_ID),
            Err(CryptoError::InactiveScheme(ED25519_SCHEME_ID))
        ));
    }

    #[test]
    fn registry_rejects_duplicate_scheme_ids() {
        let schemes = [
            Arc::new(Ed25519Scheme) as Arc<dyn SignatureScheme>,
            Arc::new(Ed25519Scheme) as Arc<dyn SignatureScheme>,
        ];
        assert!(matches!(
            SchemeRegistry::from_genesis_config(schemes, [ED25519_SCHEME_ID]),
            Err(CryptoError::DuplicateScheme(ED25519_SCHEME_ID))
        ));
    }

    #[test]
    fn address_is_bound_to_scheme_and_public_key() {
        let scheme = Ed25519Scheme;
        let private_key = [11_u8; Ed25519Scheme::PRIVATE_KEY_SIZE];
        let public_key = scheme.public_key(&private_key).unwrap();
        let address = scheme.address(&public_key).unwrap();
        assert_ne!(address.as_bytes().as_slice(), public_key.as_slice());
    }

    #[test]
    fn bls_aggregate_round_trip_requires_all_signers() {
        let scheme = Bls12381Scheme;
        let private_keys = [[1_u8; 32], [2_u8; 32], [3_u8; 32]];
        let public_keys = private_keys
            .iter()
            .map(|key| scheme.public_key(key).unwrap())
            .collect::<Vec<_>>();
        for (key, public_key) in private_keys.iter().zip(&public_keys) {
            let proof = scheme.proof_of_possession(key).unwrap();
            scheme
                .verify_proof_of_possession(public_key, &proof)
                .unwrap();
        }
        let signatures = private_keys
            .iter()
            .map(|key| scheme.sign(key, b"vote").unwrap())
            .collect::<Vec<_>>();
        let aggregate = scheme.aggregate(&signatures).unwrap();
        scheme
            .verify_aggregate(&public_keys, b"vote", &aggregate)
            .unwrap();
        assert_eq!(
            scheme.verify_aggregate(&public_keys[..2], b"vote", &aggregate),
            Err(CryptoError::VerificationFailed)
        );
    }

    #[test]
    fn bls_proof_of_possession_is_key_bound() {
        let scheme = Bls12381Scheme;
        let first = [11_u8; 32];
        let second = [12_u8; 32];
        let proof = scheme.proof_of_possession(&first).unwrap();
        let second_public = scheme.public_key(&second).unwrap();
        assert_eq!(
            scheme.verify_proof_of_possession(&second_public, &proof),
            Err(CryptoError::InvalidProofOfPossession)
        );
    }
}
