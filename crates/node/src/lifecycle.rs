use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, RwLock},
};

use consensus::{
    CertificateKind, ConsensusError, FinalizedOrder, Proposal, ValidatorSet, verify_certificate,
};
use crypto::{
    AggregateSignatureScheme, Bls12381Scheme, CryptoError, Ed25519Scheme, SchemeRegistry,
    SignatureScheme,
};
use execution::{
    DeferredExecutionError, DeferredExecutionResult, DeferredExecutor, ExecutableTransaction,
    ExecutionError, OrderedExecutionBlock,
};
use mempool::{FeeLedger, Settlement};
use network::{KestrelCast, KestrelCastConfig, KestrelCastError, Shred};
use rpc::NodeStatus;
use serde::{Deserialize, Serialize};
use state::{StateError, StateSnapshot, StateTree};
use storage::{KvStore, RocksDbStore, StorageError, WriteBatch};
use thiserror::Error;
use tracing::{debug, warn};
use types::{Address, Hash, Transaction};

use crate::{GenesisDocument, GenesisError};

const CHECKPOINT_KEY: &[u8] = b"application/checkpoint/v1";
const BLOCK_PREFIX: &[u8] = b"application/block/v1/";
const PENDING_PREFIX: &[u8] = b"application/pending/v1/";
const TRANSACTION_ID_DOMAIN: &[u8] = b"kestrel/transaction/id/v1";
const CHECKPOINT_FORMAT_VERSION: u16 = 2;
const SIGNED_EXECUTION_MAGIC: [u8; 8] = *b"KSTRTX01";

/// Execution and fee metadata covered by the account signature. Keeping the
/// fee bid inside `Transaction::payload` prevents gossip peers from rewriting
/// ordering priority without invalidating the signature.
///
/// The compute bound lives solely on `executable.compute_limit` (the same
/// value the Move VM and native operations are metered against) so there is
/// exactly one number a sender can set, rather than two that could diverge.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedExecutionPayload {
    magic: [u8; 8],
    pub executable: ExecutableTransaction,
    pub max_fee_per_compute: u128,
    pub priority_fee_per_compute: u128,
    pub policy_data: Vec<u8>,
}

impl SignedExecutionPayload {
    #[must_use]
    pub const fn new(
        executable: ExecutableTransaction,
        max_fee_per_compute: u128,
        priority_fee_per_compute: u128,
        policy_data: Vec<u8>,
    ) -> Self {
        Self {
            magic: SIGNED_EXECUTION_MAGIC,
            executable,
            max_fee_per_compute,
            priority_fee_per_compute,
            policy_data,
        }
    }
}

/// Stateless signature, sender, executable-payload, and signed-fee validator
/// shared by gossip admission and finalized block execution.
pub struct TransactionValidator {
    schemes: SchemeRegistry,
}

impl TransactionValidator {
    /// Builds the active signature registry from immutable genesis configuration.
    ///
    /// # Errors
    ///
    /// Rejects malformed or unsupported genesis scheme activation.
    pub fn from_genesis(genesis: &GenesisDocument) -> Result<Self, LifecycleError> {
        Ok(Self {
            schemes: SchemeRegistry::from_genesis_config(
                [
                    Arc::new(Ed25519Scheme) as Arc<dyn SignatureScheme>,
                    Arc::new(Bls12381Scheme) as Arc<dyn SignatureScheme>,
                ],
                genesis.active_signature_schemes.iter().copied(),
            )?,
        })
    }

    /// Validates one untrusted signed envelope without reserving its nonce.
    ///
    /// # Errors
    ///
    /// Rejects inactive schemes, bad keys/signatures, sender mismatches,
    /// malformed execution payloads, and invalid signed fee bids.
    pub fn validate(
        &self,
        transaction: &Transaction,
    ) -> Result<(Hash, SignedExecutionPayload), LifecycleError> {
        let scheme = self.schemes.get(transaction.scheme_id)?;
        if scheme.address(&transaction.public_key)? != transaction.sender {
            return Err(LifecycleError::SenderMismatch);
        }
        scheme.verify(
            &transaction.public_key,
            &transaction.signing_message(),
            &transaction.signature,
        )?;
        let payload = decode_execution_payload(&transaction.payload)?;
        if payload.executable.operation.sender() != transaction.sender {
            return Err(LifecycleError::SenderMismatch);
        }
        if payload.executable.compute_limit == 0
            || payload.max_fee_per_compute < payload.priority_fee_per_compute
        {
            return Err(LifecycleError::InvalidFeeBid);
        }
        Ok((signed_transaction_id(transaction)?, payload))
    }
}

/// Signed transaction payload propagated as one erasure-coded block.
///
/// `base_fees` is the leader's chosen local base fee per compute unit for
/// each transaction, in the same order as `transactions` — see
/// [`Self::fee_commitment`].
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PropagatedBlock {
    pub height: u64,
    pub parent_id: Hash,
    pub transactions: Vec<Transaction>,
    pub base_fees: Vec<u128>,
}

impl PropagatedBlock {
    /// Returns canonical signed-envelope identifiers in payload order.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical envelope encoding fails.
    pub fn transaction_ids(&self) -> Result<Vec<Hash>, LifecycleError> {
        self.transactions
            .iter()
            .map(signed_transaction_id)
            .collect()
    }

    /// Commits to the leader's chosen `base_fees`, in the same way
    /// `transaction_ids` commits to ordering. Folded into
    /// [`consensus::Proposal::block_id`] so any validator reconstructing this
    /// payload from shreds can detect a leader that certified one fee choice
    /// but propagated a different one.
    #[must_use]
    pub fn fee_commitment(&self) -> Hash {
        consensus::fee_commitment(&self.base_fees)
    }

    /// Encodes this payload into independently integrity-checked `KestrelCast` shreds.
    ///
    /// # Errors
    ///
    /// Returns canonical encoding or erasure-coding failures.
    pub fn shreds(&self, config: KestrelCastConfig) -> Result<Vec<Shred>, LifecycleError> {
        let bytes = canonical_bytes(self)?;
        Ok(KestrelCast::new(config)?.encode(&bytes)?)
    }

    /// Reconstructs and decodes a payload from any sufficient valid shred subset.
    ///
    /// # Errors
    ///
    /// Rejects insufficient/corrupt shreds or malformed canonical payload bytes.
    pub fn from_shreds(shreds: &[Shred]) -> Result<Self, LifecycleError> {
        let bytes = KestrelCast::reconstruct(shreds)?;
        bcs::from_bytes(&bytes).map_err(|error| LifecycleError::Encoding(error.to_string()))
    }

    /// Returns the `KestrelCast` payload commitment.
    ///
    /// # Errors
    ///
    /// Returns an error if canonical encoding fails.
    pub fn payload_id(&self) -> Result<Hash, LifecycleError> {
        Ok(Hash::digest(canonical_bytes(self)?))
    }
}

/// Atomically persisted result joining finality, payload, execution, and state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DurableBlockRecord {
    pub height: u64,
    pub consensus_block_id: Hash,
    pub parent_id: Hash,
    pub payload_id: Hash,
    pub transaction_ids: Vec<Hash>,
    pub state_root: Hash,
    pub certificate: consensus::QuorumCertificate,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct DurableCheckpoint {
    format_version: u16,
    genesis_hash: Hash,
    finalized_height: u64,
    finalized_block: Hash,
    next_nonces: BTreeMap<Address, u64>,
    fee_balances: BTreeMap<Address, u128>,
    state: StateSnapshot,
}

/// One admitted transaction's executable payload plus the fee data needed to
/// settle it deterministically at commit time: `base_fee_per_compute` is the
/// leader's certified local base fee (see [`PropagatedBlock::fee_commitment`]),
/// and `priority_fee_per_compute` is the sender's own signed bid.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PendingTransactionExecution {
    executable: ExecutableTransaction,
    base_fee_per_compute: u128,
    priority_fee_per_compute: u128,
}

/// A finalized order already handed to the deferred executor but not yet
/// committed. Persisted durably so a crash between submission and commit
/// does not strand the node waiting for the network to resend a block it
/// already validated and started executing.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
struct PendingBlock {
    order: FinalizedOrder,
    parent_id: Hash,
    payload_id: Hash,
    next_nonces: BTreeMap<Address, u64>,
    transactions: Vec<PendingTransactionExecution>,
}

/// Restart-safe finalized-block execution and application-state commit pipeline.
pub struct BlockLifecycle {
    genesis_hash: Hash,
    validators: ValidatorSet,
    scheme: Arc<dyn AggregateSignatureScheme>,
    admission: TransactionValidator,
    store: Arc<RocksDbStore>,
    executor: DeferredExecutor,
    status: Arc<RwLock<NodeStatus>>,
    rpc_state: Arc<RwLock<StateTree>>,
    committed_height: u64,
    committed_block: Hash,
    submitted_height: u64,
    submitted_block: Hash,
    admission_nonces: BTreeMap<Address, u64>,
    pending: BTreeMap<u64, PendingBlock>,
    completed: Option<DeferredExecutionResult>,
    fee_ledger: FeeLedger,
}

impl BlockLifecycle {
    /// Opens the durable application checkpoint, restoring it before execution starts.
    ///
    /// # Errors
    ///
    /// Rejects invalid genesis/checkpoint data, storage failures, worker setup
    /// failures, or poisoned shared RPC state.
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        genesis: &GenesisDocument,
        data_directory: impl AsRef<Path>,
        status: Arc<RwLock<NodeStatus>>,
        rpc_state: Arc<RwLock<StateTree>>,
        new_object_rent_balance: u64,
        worker_count: usize,
    ) -> Result<Self, LifecycleError> {
        let validated = genesis.validate()?;
        let admission = TransactionValidator::from_genesis(genesis)?;
        std::fs::create_dir_all(data_directory.as_ref())?;
        let store = Arc::new(RocksDbStore::open(data_directory)?);
        let checkpoint = if let Some(bytes) = store.get(CHECKPOINT_KEY)? {
            let checkpoint = bcs::from_bytes::<DurableCheckpoint>(&bytes)
                .map_err(|error| LifecycleError::Encoding(error.to_string()))?;
            if checkpoint.format_version != CHECKPOINT_FORMAT_VERSION
                || checkpoint.genesis_hash != validated.genesis_hash
            {
                return Err(LifecycleError::CheckpointGenesisMismatch);
            }
            checkpoint
        } else {
            let mut state = StateTree::new(genesis.state_config)?;
            for object in &genesis.initial_objects {
                state.create_object(object.clone())?;
            }
            let checkpoint = DurableCheckpoint {
                format_version: CHECKPOINT_FORMAT_VERSION,
                genesis_hash: validated.genesis_hash,
                finalized_height: 0,
                finalized_block: validated.genesis_hash,
                next_nonces: BTreeMap::new(),
                fee_balances: genesis.initial_fee_balances.clone(),
                state: state.durable_snapshot()?,
            };
            store.put(CHECKPOINT_KEY, &canonical_bytes(&checkpoint)?)?;
            checkpoint
        };
        let fee_ledger = FeeLedger::from_balances(checkpoint.fee_balances.clone());
        let state = StateTree::from_durable_snapshot(checkpoint.state.clone())?;
        {
            let mut shared = rpc_state
                .write()
                .map_err(|_| LifecycleError::LockPoisoned)?;
            *shared = state.clone();
        }
        {
            let mut current = status.write().map_err(|_| LifecycleError::LockPoisoned)?;
            current.finalized_height = checkpoint.finalized_height;
            current.finalized_block = checkpoint.finalized_block;
            current.state_root = checkpoint.state.state_root;
            current.ready = checkpoint.finalized_height > 0;
        }
        let executor = DeferredExecutor::new(
            state,
            new_object_rent_balance,
            worker_count,
            genesis.blocks_per_epoch,
        )?;
        let mut submitted_height = checkpoint.finalized_height;
        let mut submitted_block = checkpoint.finalized_block;
        let mut admission_nonces = checkpoint.next_nonces;
        let mut pending = BTreeMap::new();
        // Replay any block that was validated and handed to the executor
        // before a crash but never reached `poll_commit`'s atomic write. Each
        // entry must extend the previous height by exactly one: the executor
        // only ever runs blocks in strict submission order, so a gap or
        // repeat here means the pending log is corrupt rather than merely
        // stale, and is treated as unrecoverable rather than silently healed.
        for (_, bytes) in store.iterate_prefix(PENDING_PREFIX)? {
            let record: PendingBlock = bcs::from_bytes(&bytes)
                .map_err(|error| LifecycleError::Encoding(error.to_string()))?;
            if record.order.height != submitted_height.saturating_add(1) {
                return Err(LifecycleError::PendingReplayGap {
                    expected: submitted_height.saturating_add(1),
                    found: record.order.height,
                });
            }
            executor.submit(OrderedExecutionBlock {
                height: record.order.height,
                consensus_block_id: record.order.block_id,
                transaction_ids: record.order.transaction_ids.clone(),
                transactions: record
                    .transactions
                    .iter()
                    .map(|execution| execution.executable.clone())
                    .collect(),
            })?;
            submitted_height = record.order.height;
            submitted_block = record.order.block_id;
            admission_nonces = record.next_nonces.clone();
            pending.insert(record.order.height, record);
        }
        Ok(Self {
            genesis_hash: validated.genesis_hash,
            validators: validated.validators,
            scheme: Arc::new(Bls12381Scheme),
            admission,
            store,
            executor,
            status,
            rpc_state,
            committed_height: checkpoint.finalized_height,
            committed_block: checkpoint.finalized_block,
            submitted_height,
            submitted_block,
            admission_nonces,
            pending,
            completed: None,
            fee_ledger,
        })
    }

    /// Reconstructs a propagated payload, verifies finality and signed admission,
    /// then hands it to the bounded deferred executor without waiting.
    ///
    /// # Errors
    ///
    /// Rejects invalid certificates/order/payloads/signatures/nonces or executor backpressure.
    pub fn submit_shreds(
        &mut self,
        order: FinalizedOrder,
        shreds: &[Shred],
    ) -> Result<(), LifecycleError> {
        let payload = PropagatedBlock::from_shreds(shreds)?;
        self.submit_payload(order, &payload)
    }

    /// Verifies and submits an already reconstructed payload.
    ///
    /// # Errors
    ///
    /// Rejects invalid certificates/order/payloads/signatures/nonces or executor backpressure.
    pub fn submit_payload(
        &mut self,
        order: FinalizedOrder,
        payload: &PropagatedBlock,
    ) -> Result<(), LifecycleError> {
        if !matches!(
            order.certificate.kind,
            CertificateKind::Fast | CertificateKind::Commit
        ) {
            return Err(LifecycleError::NonFinalCertificate);
        }
        verify_certificate(&order.certificate, &self.validators, self.scheme.as_ref())?;
        if order.height != order.certificate.height
            || order.block_id != order.certificate.block_id
            || order.height != payload.height
            || order.height != self.submitted_height.saturating_add(1)
            || payload.parent_id != self.submitted_block
        {
            return Err(LifecycleError::OrderMismatch);
        }

        if payload.base_fees.len() != payload.transactions.len() {
            return Err(LifecycleError::FeeCountMismatch);
        }
        let mut next_nonces = self.admission_nonces.clone();
        let mut transaction_ids = Vec::with_capacity(payload.transactions.len());
        let mut executions = Vec::with_capacity(payload.transactions.len());
        for (transaction, &base_fee_per_compute) in
            payload.transactions.iter().zip(&payload.base_fees)
        {
            let (id, signed) = admit(transaction, &self.admission, &mut next_nonces)?;
            let required_fee_per_compute = base_fee_per_compute
                .checked_add(signed.priority_fee_per_compute)
                .ok_or(LifecycleError::FeeCapExceeded)?;
            if signed.max_fee_per_compute < required_fee_per_compute {
                return Err(LifecycleError::FeeCapExceeded);
            }
            transaction_ids.push(id);
            executions.push(PendingTransactionExecution {
                executable: signed.executable,
                base_fee_per_compute,
                priority_fee_per_compute: signed.priority_fee_per_compute,
            });
        }
        let fee_commitment = payload.fee_commitment();
        let expected_block = Proposal::new(
            payload.height,
            0,
            payload.parent_id,
            Hash::default(),
            transaction_ids.clone(),
            fee_commitment,
            None,
        )
        .block_id;
        if transaction_ids != order.transaction_ids
            || fee_commitment != order.fee_commitment
            || expected_block != order.block_id
        {
            return Err(LifecycleError::OrderMismatch);
        }
        let payload_id = payload.payload_id()?;
        let height = order.height;
        let pending = PendingBlock {
            order,
            parent_id: payload.parent_id,
            payload_id,
            next_nonces,
            transactions: executions,
        };
        // Durably record the block before handing it to the executor so a
        // crash never strands a validated, in-flight block: on restart the
        // record is replayed and resubmitted rather than lost, sparing the
        // node from having to wait for the network to resend it.
        self.store
            .put(&pending_key(height), &canonical_bytes(&pending)?)?;
        if let Err(error) = self.executor.submit(OrderedExecutionBlock {
            height,
            consensus_block_id: pending.order.block_id,
            transaction_ids,
            transactions: pending
                .transactions
                .iter()
                .map(|execution| execution.executable.clone())
                .collect(),
        }) {
            let _ = self.store.delete(&pending_key(height));
            return Err(error.into());
        }
        self.submitted_height = height;
        self.submitted_block = pending.order.block_id;
        self.admission_nonces = pending.next_nonces.clone();
        self.pending.insert(height, pending);
        Ok(())
    }

    /// Commits at most one completed execution result atomically to `RocksDB`.
    ///
    /// # Errors
    ///
    /// Returns worker, execution, ordering, checkpoint, persistence, or lock failures.
    #[allow(clippy::too_many_lines)] // Keep the commit/settlement/persistence timeline auditable.
    pub fn poll_commit(&mut self) -> Result<Option<DurableBlockRecord>, LifecycleError> {
        if self.completed.is_none() {
            self.completed = self.executor.try_result()?;
        }
        let Some(completed) = self.completed.as_ref() else {
            return Ok(None);
        };
        if completed.height != self.committed_height.saturating_add(1) {
            return Err(LifecycleError::CommitOrderMismatch);
        }
        let pending = self
            .pending
            .get(&completed.height)
            .ok_or(LifecycleError::CommitOrderMismatch)?;
        if completed.consensus_block_id != pending.order.block_id {
            return Err(LifecycleError::CommitOrderMismatch);
        }
        let result = completed
            .result
            .as_ref()
            .map_err(|error| LifecycleError::ExecutionFailed(error.to_string()))?;
        let snapshot = completed
            .state_snapshot
            .as_ref()
            .ok_or(LifecycleError::MissingStateSnapshot)?;
        if result.state_root != snapshot.state_root {
            return Err(LifecycleError::CommitRootMismatch);
        }
        let record = DurableBlockRecord {
            height: completed.height,
            consensus_block_id: completed.consensus_block_id,
            parent_id: pending.parent_id,
            payload_id: pending.payload_id,
            transaction_ids: pending.order.transaction_ids.clone(),
            state_root: snapshot.state_root,
            certificate: pending.order.certificate.clone(),
        };
        // Settle actual compute against the certified local base fee plus
        // each sender's own signed priority bid, crediting the validator that
        // led this height. A settlement failure (e.g. an unfunded payer) does
        // not unwind an already-finalized, already-executed block — it is
        // only ever logged.
        let leader = self
            .validators
            .leader(pending.order.height, pending.order.certificate.view);
        let leader_address = self.scheme.address(&leader.public_key)?;
        for receipt in &result.receipts {
            let (Some(execution), Some(&transaction_id)) = (
                pending.transactions.get(receipt.transaction_index),
                pending.order.transaction_ids.get(receipt.transaction_index),
            ) else {
                continue;
            };
            let settlement = Settlement {
                transaction_id,
                payer: execution.executable.operation.sender(),
                compute_limit: execution.executable.compute_limit,
                local_base_fee_per_compute: execution.base_fee_per_compute,
                priority_fee_per_compute: execution.priority_fee_per_compute,
            };
            if let Err(error) =
                self.fee_ledger
                    .settle(&settlement, receipt.compute_used, leader_address)
            {
                warn!(
                    %error,
                    height = record.height,
                    %transaction_id,
                    "fee settlement failed; block commit proceeds unaffected"
                );
            }
        }
        let checkpoint = DurableCheckpoint {
            format_version: CHECKPOINT_FORMAT_VERSION,
            genesis_hash: self.genesis_hash,
            finalized_height: record.height,
            finalized_block: record.consensus_block_id,
            next_nonces: pending.next_nonces.clone(),
            fee_balances: self.fee_ledger.balances().clone(),
            state: snapshot.clone(),
        };
        let mut batch = WriteBatch::new();
        batch
            .put(block_key(record.height), canonical_bytes(&record)?)
            .put(CHECKPOINT_KEY, canonical_bytes(&checkpoint)?)
            .delete(pending_key(record.height));
        self.store.write_batch(batch)?;

        let restored = StateTree::from_durable_snapshot(snapshot.clone())?;
        {
            let mut state = self
                .rpc_state
                .write()
                .map_err(|_| LifecycleError::LockPoisoned)?;
            *state = restored;
        }
        {
            let mut status = self
                .status
                .write()
                .map_err(|_| LifecycleError::LockPoisoned)?;
            // `committed_height` is this component's own clock: the height it
            // has executed and durably written, which `state_root` belongs to.
            // `finalized_height` is also advanced here so a node that is not
            // running a coordinator (or has not finalized anything yet) still
            // reports a sensible ordering position, but the coordinator owns it
            // once consensus is running and will be ahead of this.
            status.committed_height = record.height;
            status.finalized_height = status.finalized_height.max(record.height);
            status.finalized_block = record.consensus_block_id;
            status.state_root = record.state_root;
            status.ready = true;
        }
        self.committed_height = record.height;
        self.committed_block = record.consensus_block_id;
        self.pending.remove(&record.height);
        self.completed = None;
        debug!(
            height = record.height,
            transaction_count = record.transaction_ids.len(),
            "committed block"
        );
        Ok(Some(record))
    }

    /// Reads an atomically committed block record by height.
    ///
    /// # Errors
    ///
    /// Returns storage or canonical decoding failures.
    pub fn block(&self, height: u64) -> Result<Option<DurableBlockRecord>, LifecycleError> {
        self.store
            .get(&block_key(height))?
            .map(|bytes| {
                bcs::from_bytes(&bytes).map_err(|error| LifecycleError::Encoding(error.to_string()))
            })
            .transpose()
    }

    #[must_use]
    pub const fn committed_height(&self) -> u64 {
        self.committed_height
    }

    #[must_use]
    pub const fn committed_block(&self) -> Hash {
        self.committed_block
    }

    #[must_use]
    pub fn admission_nonces(&self) -> BTreeMap<Address, u64> {
        self.admission_nonces.clone()
    }

    /// Returns `address`'s durable fee-ledger balance.
    #[must_use]
    pub fn fee_balance(&self, address: Address) -> u128 {
        self.fee_ledger.balance(address)
    }

    /// Shares this node's single durable store with other components (the
    /// Stage 2 pipeline's pre-consensus admission log) instead of opening a
    /// second `RocksDB` instance for the same process.
    #[must_use]
    pub fn store_handle(&self) -> Arc<RocksDbStore> {
        Arc::clone(&self.store)
    }
}

fn admit(
    transaction: &Transaction,
    validator: &TransactionValidator,
    nonces: &mut BTreeMap<Address, u64>,
) -> Result<(Hash, SignedExecutionPayload), LifecycleError> {
    let (id, payload) = validator.validate(transaction)?;
    let expected = nonces.get(&transaction.sender).copied().unwrap_or_default();
    if transaction.nonce != expected {
        return Err(LifecycleError::NonceMismatch {
            expected,
            received: transaction.nonce,
        });
    }
    nonces.insert(
        transaction.sender,
        expected
            .checked_add(1)
            .ok_or(LifecycleError::NonceOverflow)?,
    );
    Ok((id, payload))
}

/// Returns the canonical ID of a complete signed transaction envelope.
///
/// # Errors
///
/// Returns an error if canonical envelope encoding fails.
pub fn signed_transaction_id(transaction: &Transaction) -> Result<Hash, LifecycleError> {
    let mut bytes = Vec::from(TRANSACTION_ID_DOMAIN);
    bytes.extend(canonical_bytes(transaction)?);
    Ok(Hash::digest(bytes))
}

fn decode_execution_payload(bytes: &[u8]) -> Result<SignedExecutionPayload, LifecycleError> {
    if let Ok(payload) = bcs::from_bytes::<SignedExecutionPayload>(bytes)
        && payload.magic == SIGNED_EXECUTION_MAGIC
    {
        return Ok(payload);
    }
    let executable = bcs::from_bytes::<ExecutableTransaction>(bytes)
        .map_err(|error| LifecycleError::Encoding(error.to_string()))?;
    Ok(SignedExecutionPayload::new(executable, 1, 0, Vec::new()))
}

fn block_key(height: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(BLOCK_PREFIX.len() + 8);
    key.extend_from_slice(BLOCK_PREFIX);
    key.extend_from_slice(&height.to_be_bytes());
    key
}

fn pending_key(height: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(PENDING_PREFIX.len() + 8);
    key.extend_from_slice(PENDING_PREFIX);
    key.extend_from_slice(&height.to_be_bytes());
    key
}

fn canonical_bytes<T: Serialize>(value: &T) -> Result<Vec<u8>, LifecycleError> {
    bcs::to_bytes(value).map_err(|error| LifecycleError::Encoding(error.to_string()))
}

#[derive(Debug, Error)]
pub enum LifecycleError {
    #[error(transparent)]
    Genesis(#[from] GenesisError),
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error(transparent)]
    State(#[from] StateError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    #[error(transparent)]
    DeferredExecution(#[from] DeferredExecutionError),
    #[error(transparent)]
    KestrelCast(#[from] KestrelCastError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("canonical lifecycle encoding failed: {0}")]
    Encoding(String),
    #[error("durable checkpoint belongs to a different genesis or format")]
    CheckpointGenesisMismatch,
    #[error("certificate is not a final fast or fallback-commit certificate")]
    NonFinalCertificate,
    #[error("finalized order does not match the propagated payload or local chain head")]
    OrderMismatch,
    #[error("transaction sender does not match its active signature key or executable payload")]
    SenderMismatch,
    #[error("transaction nonce mismatch: expected {expected}, received {received}")]
    NonceMismatch { expected: u64, received: u64 },
    #[error("transaction nonce overflow")]
    NonceOverflow,
    #[error("signed transaction fee bid is invalid")]
    InvalidFeeBid,
    #[error("deferred execution completed outside canonical height/block order")]
    CommitOrderMismatch,
    #[error("deferred execution failed: {0}")]
    ExecutionFailed(String),
    #[error("successful deferred execution omitted its durable state snapshot")]
    MissingStateSnapshot,
    #[error("execution result and durable state snapshot roots differ")]
    CommitRootMismatch,
    #[error("shared RPC state lock was poisoned")]
    LockPoisoned,
    #[error("durable pending-block log is corrupt: expected height {expected}, found {found}")]
    PendingReplayGap { expected: u64, found: u64 },
    #[error("propagated block's base fee count does not match its transaction count")]
    FeeCountMismatch,
    #[error("declared local base fee does not fit the transaction's signed fee cap")]
    FeeCapExceeded,
}
