use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use consensus::FinalizedOrder;
use libp2p::PeerId;
use mempool::{FeeScope, LocalizedMempool, MempoolError, SubmittedTransaction};
use network::{
    GossipError, InboundShred, KestrelCast, KestrelCastConfig, KestrelCastError, NetworkHandle,
    NetworkNode, RelayCandidate, Shred,
};
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle, time::MissedTickBehavior};
use tracing::{debug, warn};
use types::{Address, Hash, Transaction};

use crate::{
    BlockLifecycle, DurableBlockRecord, GenesisDocument, LifecycleError, PropagatedBlock,
    ProposalTransactionSource, TransactionValidator,
};

/// Bounded production composition settings for the Stage 2 socket pipeline.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stage2PipelineConfig {
    pub maximum_block_transactions: usize,
    pub base_fee_per_compute: u128,
    pub congestion_increment: u128,
    pub per_scope_block_limit: usize,
    pub kestrel_cast: KestrelCastConfig,
    pub relay_count: usize,
    pub replication_factor: usize,
    pub maximum_inflight_shred_blocks: usize,
    pub commit_poll_interval: Duration,
    /// Invalid gossiped transactions/shreds attributed to the same peer before
    /// it is disconnected and blocked. Bans are in-memory only (not persisted
    /// or shared across nodes) and never automatically expire.
    pub peer_offense_ban_threshold: u32,
}

impl Default for Stage2PipelineConfig {
    fn default() -> Self {
        Self {
            maximum_block_transactions: 4_096,
            base_fee_per_compute: 1,
            congestion_increment: 1,
            per_scope_block_limit: 1_024,
            kestrel_cast: KestrelCastConfig::default(),
            relay_count: 12,
            replication_factor: 2,
            maximum_inflight_shred_blocks: 64,
            commit_poll_interval: Duration::from_millis(2),
            peer_offense_ban_threshold: 8,
        }
    }
}

/// Cloneable local ingress and consensus proposal interface.
#[derive(Clone)]
pub struct Stage2PipelineHandle {
    source: Arc<PipelineProposalSource>,
    network: NetworkHandle,
}

impl Stage2PipelineHandle {
    /// Validates, reserves, and publishes one signed transaction on the
    /// independent mempool gossip path.
    ///
    /// # Errors
    ///
    /// Rejects invalid signatures/payloads/nonces/fees, full queues, or encoding failures.
    pub fn submit_transaction(&self, transaction: Transaction) -> Result<Hash, PipelineError> {
        let bytes = bcs::to_bytes(&transaction)
            .map_err(|error| PipelineError::Encoding(error.to_string()))?;
        let id = self.source.admit(transaction)?;
        if let Err(error) = self.network.try_publish_transaction(bytes) {
            self.source.rollback(id)?;
            return Err(error.into());
        }
        Ok(id)
    }
}

impl rpc::TransactionSubmitter for Stage2PipelineHandle {
    fn submit(&self, bytes: Vec<u8>) -> Result<Hash, String> {
        let transaction = bcs::from_bytes::<Transaction>(&bytes)
            .map_err(|error| format!("malformed transaction envelope: {error}"))?;
        self.submit_transaction(transaction)
            .map_err(|error| error.to_string())
    }
}

/// Live composition of libp2p ingress, one-hop `KestrelCast` relay, certified
/// consensus order, deferred execution, `RocksDB` commit, and RPC state updates.
pub struct Stage2Pipeline {
    source: Arc<PipelineProposalSource>,
    network: NetworkHandle,
    inbound_transactions: mpsc::Receiver<network::InboundTransaction>,
    inbound_shreds: mpsc::Receiver<InboundShred>,
    finalized_orders: mpsc::UnboundedReceiver<FinalizedOrder>,
    network_task: JoinHandle<()>,
    lifecycle: BlockLifecycle,
    peer_ids: BTreeSet<PeerId>,
    local_peer_id: PeerId,
    pending_orders: BTreeMap<u64, FinalizedOrder>,
    submitted_payloads: BTreeMap<u64, PropagatedBlock>,
    submitted_height: u64,
    config: Stage2PipelineConfig,
    /// In-memory offense count per gossip peer that has sent an invalid
    /// transaction or shred, used to ban repeat offenders.
    offense_counts: BTreeMap<PeerId, u32>,
}

impl Stage2Pipeline {
    /// Joins already-bound networking and lifecycle components. The returned
    /// proposal source is passed to `ConsensusCoordinator::bind_with_pipeline`.
    ///
    /// # Errors
    ///
    /// Rejects an incomplete/foreign peer topology or invalid mempool settings.
    pub fn new(
        genesis: &GenesisDocument,
        network_node: NetworkNode,
        lifecycle: BlockLifecycle,
        validator_peers: BTreeMap<Hash, PeerId>,
        finalized_orders: mpsc::UnboundedReceiver<FinalizedOrder>,
        config: Stage2PipelineConfig,
    ) -> Result<(Self, Stage2PipelineHandle), PipelineError> {
        if config.maximum_block_transactions == 0
            || config.relay_count == 0
            || config.replication_factor == 0
            || config.maximum_inflight_shred_blocks == 0
            || config.commit_poll_interval.is_zero()
        {
            return Err(PipelineError::InvalidConfiguration);
        }
        let expected = genesis
            .validators
            .iter()
            .map(|entry| entry.validator.id)
            .collect::<BTreeSet<_>>();
        if validator_peers.keys().copied().collect::<BTreeSet<_>>() != expected
            || !validator_peers
                .values()
                .any(|peer| *peer == network_node.local_peer_id)
        {
            return Err(PipelineError::InvalidPeerTopology);
        }
        let mempool = LocalizedMempool::new(
            config.base_fee_per_compute,
            config.congestion_increment,
            config.per_scope_block_limit,
        )?;
        let candidates = genesis
            .validators
            .iter()
            .filter_map(|entry| {
                let peer_id = validator_peers[&entry.validator.id];
                (peer_id != network_node.local_peer_id).then_some(RelayCandidate {
                    id: entry.validator.id,
                    stake: entry.validator.stake,
                })
            })
            .collect();
        let source = Arc::new(PipelineProposalSource {
            state: Mutex::new(PipelineState {
                mempool,
                transactions: BTreeMap::new(),
                reserved_senders: BTreeMap::new(),
                next_nonces: lifecycle.admission_nonces(),
                arrival_sequence: 0,
                payloads: BTreeMap::new(),
                shreds: BTreeMap::new(),
                propagated: BTreeSet::new(),
            }),
            validator: TransactionValidator::from_genesis(genesis)?,
            network: network_node.handle.clone(),
            validator_peers,
            candidates,
            config,
        });
        let handle = Stage2PipelineHandle {
            source: Arc::clone(&source),
            network: network_node.handle.clone(),
        };
        let peer_ids = source.validator_peers.values().copied().collect();
        let submitted_height = lifecycle.committed_height();
        Ok((
            Self {
                source,
                network: network_node.handle,
                inbound_transactions: network_node.inbound_transactions,
                inbound_shreds: network_node.inbound_shreds,
                finalized_orders,
                network_task: network_node.task,
                lifecycle,
                peer_ids,
                local_peer_id: network_node.local_peer_id,
                pending_orders: BTreeMap::new(),
                submitted_payloads: BTreeMap::new(),
                submitted_height,
                config,
                offense_counts: BTreeMap::new(),
            },
            handle,
        ))
    }

    #[must_use]
    pub fn proposal_source(&self) -> Arc<dyn ProposalTransactionSource> {
        Arc::clone(&self.source) as Arc<dyn ProposalTransactionSource>
    }

    /// Runs until the task is cancelled or one component fails.
    ///
    /// # Errors
    ///
    /// Returns invalid ingress, propagation, lifecycle, or closed-finality errors.
    pub async fn run(mut self) -> Result<(), PipelineError> {
        let mut ticker = tokio::time::interval(self.config.commit_poll_interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                // A gossip peer is untrusted and unauthenticated by stake: a
                // malformed or invalid transaction/shred must be rejected and
                // logged, never allowed to tear down the whole pipeline task
                // via `?`. Only the trusted arms below (already-certified
                // finalized orders, and our own lifecycle/commit path) treat
                // failure as fatal.
                Some(transaction) = self.inbound_transactions.recv() => {
                    if let Err(error) = self.handle_inbound_transaction(&transaction.bytes) {
                        debug!(%error, "rejected inbound gossiped transaction");
                        if let Some(peer) = transaction.source {
                            self.record_offense(peer);
                        }
                    }
                }
                Some(shred) = self.inbound_shreds.recv() => {
                    let source = shred.source;
                    match self.handle_shred(shred) {
                        Ok(()) => self.submit_available_orders()?,
                        Err(error) => {
                            debug!(%error, "rejected inbound shred");
                            self.record_offense(source);
                        }
                    }
                }
                Some(order) = self.finalized_orders.recv() => {
                    self.pending_orders.insert(order.height, order);
                    self.submit_available_orders()?;
                }
                _ = ticker.tick() => {
                    self.submit_available_orders()?;
                    if let Some(record) = self.lifecycle.poll_commit()? {
                        self.on_commit(&record)?;
                    }
                }
                else => return Err(PipelineError::PipelineClosed),
            }
        }
    }

    /// Records one invalid message from `peer`, banning it once its offense
    /// count reaches `peer_offense_ban_threshold`. Bans live only in this
    /// process's memory: they are not persisted and never automatically lift.
    fn record_offense(&mut self, peer: PeerId) {
        let offenses = tally_offense(&mut self.offense_counts, peer);
        if offenses == self.config.peer_offense_ban_threshold {
            match self.network.ban_peer(peer) {
                Ok(()) => warn!(%peer, offenses, "banned peer after repeated invalid gossip"),
                Err(error) => debug!(%peer, %error, "failed to queue peer ban"),
            }
        }
    }

    /// Decodes and admits one gossiped transaction. Every failure here is
    /// attributable to untrusted network input (malformed bytes, an invalid
    /// signature, a stale nonce, a full mempool scope, ...) and must be
    /// rejected without affecting any other transaction or peer.
    fn handle_inbound_transaction(&self, bytes: &[u8]) -> Result<(), PipelineError> {
        let transaction = bcs::from_bytes::<Transaction>(bytes)
            .map_err(|error| PipelineError::Encoding(error.to_string()))?;
        self.source.admit(transaction)?;
        Ok(())
    }

    fn handle_shred(&mut self, inbound: InboundShred) -> Result<(), PipelineError> {
        if inbound.relay_requested {
            for peer in self
                .peer_ids
                .iter()
                .copied()
                .filter(|peer| *peer != self.local_peer_id && *peer != inbound.source)
            {
                self.network.try_send_shred(peer, inbound.shred.clone())?;
            }
        }
        self.source.record_shred(inbound.shred)
    }

    fn submit_available_orders(&mut self) -> Result<(), PipelineError> {
        loop {
            let height = self.submitted_height.saturating_add(1);
            let Some(order) = self.pending_orders.get(&height).cloned() else {
                return Ok(());
            };
            let Some(payload) = self.source.payload_for_order(&order)? else {
                return Ok(());
            };
            self.lifecycle.submit_payload(order, &payload)?;
            self.pending_orders.remove(&height);
            self.submitted_payloads.insert(height, payload);
            self.submitted_height = height;
        }
    }

    fn on_commit(&mut self, record: &DurableBlockRecord) -> Result<(), PipelineError> {
        let payload = self
            .submitted_payloads
            .remove(&record.height)
            .ok_or(PipelineError::CommittedPayloadUnavailable)?;
        self.source.note_committed(&payload)?;
        Ok(())
    }
}

impl Drop for Stage2Pipeline {
    fn drop(&mut self) {
        self.network_task.abort();
    }
}

struct PipelineState {
    mempool: LocalizedMempool,
    transactions: BTreeMap<Hash, Transaction>,
    reserved_senders: BTreeMap<Address, Hash>,
    next_nonces: BTreeMap<Address, u64>,
    arrival_sequence: u64,
    payloads: BTreeMap<(u64, Hash), PropagatedBlock>,
    shreds: BTreeMap<Hash, BTreeMap<u16, Shred>>,
    propagated: BTreeSet<(u64, Hash)>,
}

struct PipelineProposalSource {
    state: Mutex<PipelineState>,
    validator: TransactionValidator,
    network: NetworkHandle,
    validator_peers: BTreeMap<Hash, PeerId>,
    candidates: Vec<RelayCandidate>,
    config: Stage2PipelineConfig,
}

impl PipelineProposalSource {
    fn admit(&self, transaction: Transaction) -> Result<Hash, PipelineError> {
        let (id, payload) = self.validator.validate(&transaction)?;
        let mut state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
        if state.transactions.contains_key(&id) {
            return Ok(id);
        }
        let expected = state
            .next_nonces
            .get(&transaction.sender)
            .copied()
            .unwrap_or_default();
        if transaction.nonce != expected {
            return Err(PipelineError::NonceMismatch {
                expected,
                received: transaction.nonce,
            });
        }
        if state.reserved_senders.contains_key(&transaction.sender) {
            return Err(PipelineError::SenderAlreadyReserved(transaction.sender));
        }
        let touched_objects = payload
            .executable
            .object_references
            .iter()
            .map(|reference| reference.id)
            .collect::<BTreeSet<_>>();
        let scope = touched_objects
            .iter()
            .next()
            .copied()
            .map_or(FeeScope::Account(transaction.sender), FeeScope::Object);
        let arrival_sequence = state.arrival_sequence;
        state.arrival_sequence = state.arrival_sequence.saturating_add(1);
        state.mempool.submit(SubmittedTransaction {
            id,
            sender: transaction.sender,
            scope,
            touched_objects,
            compute_limit: payload.executable.compute_limit,
            max_fee_per_compute: payload.max_fee_per_compute,
            priority_fee_per_compute: payload.priority_fee_per_compute,
            arrival_sequence,
            policy_data: payload.policy_data,
        })?;
        state.reserved_senders.insert(transaction.sender, id);
        state.transactions.insert(id, transaction);
        Ok(id)
    }

    fn rollback(&self, id: Hash) -> Result<(), PipelineError> {
        let mut state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
        let mut ids = BTreeSet::new();
        ids.insert(id);
        state.mempool.remove_transactions(&ids);
        if let Some(transaction) = state.transactions.remove(&id) {
            state.reserved_senders.remove(&transaction.sender);
        }
        Ok(())
    }

    fn build_or_get_payload(
        &self,
        height: u64,
        parent_id: Hash,
    ) -> Result<PropagatedBlock, PipelineError> {
        let key = (height, parent_id);
        let mut state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
        if let Some(payload) = state.payloads.get(&key) {
            return Ok(payload.clone());
        }
        let selection = state
            .mempool
            .select_block(self.config.maximum_block_transactions);
        let transactions = selection
            .transactions
            .iter()
            .map(|pending| {
                state
                    .transactions
                    .get(&pending.transaction.id)
                    .cloned()
                    .ok_or(PipelineError::SelectedTransactionUnavailable)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let payload = PropagatedBlock {
            height,
            parent_id,
            transactions,
        };
        state.payloads.insert(key, payload.clone());
        Ok(payload)
    }

    fn propagate(
        &self,
        height: u64,
        parent_id: Hash,
        payload: &PropagatedBlock,
    ) -> Result<(), PipelineError> {
        let key = (height, parent_id);
        {
            let state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
            if state.propagated.contains(&key) {
                return Ok(());
            }
        }
        if self.candidates.is_empty() {
            self.state
                .lock()
                .map_err(|_| PipelineError::LockPoisoned)?
                .propagated
                .insert(key);
            return Ok(());
        }
        let shreds = payload.shreds(self.config.kestrel_cast)?;
        let plan = KestrelCast::relay_plan(
            shreds[0].block_id,
            &shreds,
            &self.candidates,
            self.config.relay_count.min(self.candidates.len()),
            self.config.replication_factor,
        )?;
        for (validator, assigned) in plan.assignments {
            let peer = self.validator_peers[&validator];
            for shred in assigned {
                self.network.try_send_relay_shred(peer, shred)?;
            }
        }
        self.state
            .lock()
            .map_err(|_| PipelineError::LockPoisoned)?
            .propagated
            .insert(key);
        Ok(())
    }

    fn record_shred(&self, shred: Shred) -> Result<(), PipelineError> {
        let block_id = shred.block_id;
        let data_shards = usize::from(shred.data_shards);
        let candidate = {
            let mut state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
            if !state.shreds.contains_key(&block_id)
                && state.shreds.len() >= self.config.maximum_inflight_shred_blocks
            {
                return Err(PipelineError::InflightShredLimitReached);
            }
            let shreds = state.shreds.entry(block_id).or_default();
            shreds.entry(shred.index).or_insert(shred);
            (shreds.len() >= data_shards).then(|| shreds.values().cloned().collect::<Vec<_>>())
        };
        let Some(shreds) = candidate else {
            return Ok(());
        };
        let payload = PropagatedBlock::from_shreds(&shreds)?;
        for transaction in &payload.transactions {
            self.validator.validate(transaction)?;
        }
        let mut state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
        state
            .payloads
            .entry((payload.height, payload.parent_id))
            .or_insert(payload);
        state.shreds.remove(&block_id);
        Ok(())
    }

    fn payload_for_order(
        &self,
        order: &FinalizedOrder,
    ) -> Result<Option<PropagatedBlock>, PipelineError> {
        let state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
        for ((height, _), payload) in &state.payloads {
            if *height == order.height && payload.transaction_ids()? == order.transaction_ids {
                return Ok(Some(payload.clone()));
            }
        }
        Ok(None)
    }

    fn note_committed(&self, payload: &PropagatedBlock) -> Result<(), PipelineError> {
        let ids = payload
            .transaction_ids()?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
        for transaction in &payload.transactions {
            if let Some(reserved) = state.reserved_senders.remove(&transaction.sender) {
                state.transactions.remove(&reserved);
            }
            state.next_nonces.insert(
                transaction.sender,
                transaction
                    .nonce
                    .checked_add(1)
                    .ok_or(PipelineError::NonceOverflow)?,
            );
        }
        state.mempool.remove_transactions(&ids);
        for id in ids {
            state.transactions.remove(&id);
        }
        state
            .payloads
            .retain(|(height, _), _| *height > payload.height);
        state
            .propagated
            .retain(|(height, _)| *height > payload.height);
        Ok(())
    }
}

impl ProposalTransactionSource for PipelineProposalSource {
    fn transaction_ids(&self, height: u64, parent_id: Hash) -> Option<Vec<Hash>> {
        let payload = self.build_or_get_payload(height, parent_id).ok()?;
        self.propagate(height, parent_id, &payload).ok()?;
        payload.transaction_ids().ok()
    }
}

/// Failures at the integrated Stage 2 transport/order/execution boundary.
#[derive(Debug, Error)]
pub enum PipelineError {
    #[error("Stage 2 pipeline configuration is invalid")]
    InvalidConfiguration,
    #[error("the validator-to-peer topology does not exactly match genesis")]
    InvalidPeerTopology,
    #[error("the sender already has an uncommitted transaction reserved: {0}")]
    SenderAlreadyReserved(Address),
    #[error("transaction nonce mismatch: expected {expected}, received {received}")]
    NonceMismatch { expected: u64, received: u64 },
    #[error("transaction nonce overflow")]
    NonceOverflow,
    #[error("the mempool selected a transaction whose signed envelope is unavailable")]
    SelectedTransactionUnavailable,
    #[error("the committed block's submitted payload is unavailable")]
    CommittedPayloadUnavailable,
    #[error("the in-flight shred-block limit was reached")]
    InflightShredLimitReached,
    #[error("all Stage 2 pipeline inputs closed")]
    PipelineClosed,
    #[error("Stage 2 pipeline mutex was poisoned")]
    LockPoisoned,
    #[error("pipeline encoding failed: {0}")]
    Encoding(String),
    #[error(transparent)]
    Lifecycle(#[from] LifecycleError),
    #[error(transparent)]
    Mempool(#[from] MempoolError),
    #[error(transparent)]
    Gossip(#[from] GossipError),
    #[error(transparent)]
    KestrelCast(#[from] KestrelCastError),
}

/// Increments `peer`'s offense count and returns the new total.
fn tally_offense(offense_counts: &mut BTreeMap<PeerId, u32>, peer: PeerId) -> u32 {
    let count = offense_counts.entry(peer).or_insert(0);
    *count += 1;
    *count
}

#[cfg(test)]
mod tests {
    use super::tally_offense;

    #[test]
    fn tally_offense_crosses_the_ban_threshold_exactly_once() {
        let threshold = 3;
        let mut counts = std::collections::BTreeMap::new();
        let peer = libp2p::PeerId::random();
        let other = libp2p::PeerId::random();

        assert_eq!(tally_offense(&mut counts, peer), 1);
        assert_eq!(tally_offense(&mut counts, peer), 2);
        // The offending peer crosses the threshold on its third offense...
        assert_eq!(tally_offense(&mut counts, peer), threshold);
        // ...and every offense after that keeps counting rather than
        // re-triggering (the caller compares for equality, not `>=`, so a
        // ban is requested exactly once per peer).
        assert_eq!(tally_offense(&mut counts, peer), threshold + 1);
        assert_ne!(tally_offense(&mut counts, peer), threshold);

        // A well-behaved peer's count is completely independent.
        assert_eq!(tally_offense(&mut counts, other), 1);
    }
}
