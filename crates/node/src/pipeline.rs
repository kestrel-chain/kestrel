use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use consensus::FinalizedOrder;
use execution::DeferredExecutionError;
use libp2p::PeerId;
use mempool::{FeeScope, LocalizedMempool, MempoolError, SubmittedTransaction};
use network::{
    GossipError, InboundShred, KestrelCast, KestrelCastConfig, KestrelCastError, NetworkHandle,
    NetworkNode, RelayCandidate, Shred,
};
use storage::{KvStore, RocksDbStore, StorageError};
use thiserror::Error;
use tokio::{sync::mpsc, task::JoinHandle, time::MissedTickBehavior};
use tracing::{debug, trace, warn};
use types::{Address, Hash, Transaction};

use crate::{
    BlockLifecycle, GenesisDocument, LifecycleError, PropagatedBlock, ProposalTransactionSource,
    TransactionValidator,
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
    /// it is disconnected and blocked. Bans are persisted durably to the node's
    /// store and re-applied on restart (TD-014), but are not shared across
    /// nodes and never automatically expire.
    pub peer_offense_ban_threshold: u32,
    /// Certified orders this validator may hold undelivered to execution before
    /// it stops and reports failure.
    ///
    /// Deferred execution means ordering legitimately runs a little ahead of
    /// execution, so this is a backlog bound rather than the strict one-block
    /// bound `consensus-spec` describes for the executor's own channel. What it
    /// prevents is the unbounded case: when execution stops entirely, the
    /// validator kept accepting certified orders forever, growing this backlog
    /// without limit while continuing to vote and finalize — presenting to the
    /// rest of the network as a healthy validator whose state was frozen and
    /// whose RPC served stale data indefinitely. Exceeding the bound is a real
    /// local failure, so it is reported rather than absorbed silently.
    pub maximum_pending_orders: usize,
    /// How long a validator waits for a certified height's payload to arrive on
    /// its own before asking peers for it, and the cap on the doubling backoff
    /// between repeated asks.
    ///
    /// `KestrelCast` delivery is fire-and-forget, so a single lost send strands
    /// that validator's execution permanently without this: it holds the order
    /// and every transaction named in it, yet cannot rebuild the payload,
    /// because the leader's per-transaction base fees exist nowhere but the
    /// payload and are bound by the certified fee commitment.
    pub payload_repair_delay: Duration,
    pub maximum_payload_repair_backoff: Duration,
    /// How long execution may make no forward progress while its order backlog
    /// exceeds [`maximum_pending_orders`](Self::maximum_pending_orders) before
    /// the validator declares a genuine execution stall and stops.
    ///
    /// The backlog bound alone cannot tell a *frozen* validator from a *busy*
    /// one: a validator that fell behind and is catching up (re-fetching the
    /// payloads it missed and executing them in a burst) legitimately holds a
    /// backlog far above the steady-state bound while it works through it. What
    /// distinguishes the two is progress — catching up keeps advancing
    /// `submitted_height`, a genuine stall does not. So an over-bound backlog is
    /// fatal only if it also stays completely frozen for this whole window,
    /// which is set comfortably above the payload-repair backoff so ordinary
    /// catch-up (or a slow repair round-trip) is never mistaken for a stall.
    pub execution_stall_grace: Duration,
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
            // Comfortably above the few heights ordering normally leads
            // execution by, and far below the hundreds seen when execution has
            // actually stopped.
            maximum_pending_orders: 64,
            payload_repair_delay: Duration::from_millis(500),
            maximum_payload_repair_backoff: Duration::from_secs(8),
            // Above the 8s repair backoff cap, so a validator catching up
            // through missed payloads — which advances in bursts separated by
            // repair round-trips — is never mistaken for one whose execution
            // has genuinely stopped.
            execution_stall_grace: Duration::from_secs(30),
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

    /// Snapshot of this validator's shred/payload path.
    ///
    /// # Errors
    ///
    /// Returns an error if the pipeline state mutex is poisoned.
    pub fn shred_stats(&self) -> Result<ShredStats, PipelineError> {
        let state = self
            .source
            .state
            .lock()
            .map_err(|_| PipelineError::LockPoisoned)?;
        Ok(ShredStats {
            shreds_received: state.shreds_received,
            payloads_reconstructed: state.payloads_reconstructed,
            payloads_available: state.payloads.len(),
            incomplete_shred_blocks: state.shreds.len(),
        })
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
    submitted_height: u64,
    config: Stage2PipelineConfig,
    /// In-memory offense count per gossip peer that has sent an invalid
    /// transaction or shred, used to ban repeat offenders. Resets on restart;
    /// the resulting bans do not (they are persisted via `store`).
    offense_counts: BTreeMap<PeerId, u32>,
    /// Shared node store used to persist peer bans so they survive a restart.
    store: Arc<RocksDbStore>,
    inbound_repair_requests: mpsc::Receiver<(PeerId, u64)>,
    /// When to next ask peers for the payload this validator is stuck on, and
    /// the current backoff between asks. Cleared whenever a height is submitted.
    repair_after: Option<Instant>,
    repair_backoff: Duration,
    /// Starting peer for the next repair attempt. Rotating this prevents a
    /// missing head payload from being requested forever from one peer that
    /// also missed it.
    repair_peer_cursor: usize,
    /// Highest `submitted_height` seen the last time the backlog was checked.
    /// Any advance past it proves execution is still draining the backlog and
    /// resets the stall clock.
    progress_height: u64,
    /// When the backlog first exceeded the bound with no execution progress
    /// since. `None` whenever the backlog is healthy or execution is advancing;
    /// once it has stayed set for `execution_stall_grace`, the stall is fatal.
    stalled_since: Option<Instant>,
}

impl Stage2Pipeline {
    /// Joins already-bound networking and lifecycle components. The returned
    /// proposal source is passed to `ConsensusCoordinator::bind_with_pipeline`.
    ///
    /// # Errors
    ///
    /// Rejects an incomplete/foreign peer topology or invalid mempool settings.
    #[allow(clippy::too_many_lines)] // Keep the full component-wiring order auditable in one place.
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
            || config.maximum_pending_orders == 0
            || config.payload_repair_delay.is_zero()
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
        let admission_store = lifecycle.store_handle();
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
                // A durable pending block is replayed into execution by
                // `BlockLifecycle::open`. It is already canonical and its
                // transactions must not be selected again even though it has
                // not reached the committed checkpoint yet.
                retired_height: lifecycle.submitted_height(),
                shreds_received: 0,
                payloads_reconstructed: 0,
                shred_arrivals: BTreeMap::new(),
                shred_sequence: 0,
                recent_payloads: BTreeMap::new(),
            }),
            validator: TransactionValidator::from_genesis(genesis)?,
            network: network_node.handle.clone(),
            validator_peers,
            candidates,
            config,
            admission_store: Arc::clone(&admission_store),
        });
        // Restore any transaction admitted before a restart but not yet
        // finalized. `admit` re-validates against the freshly restored
        // nonces above, so an entry already finalized before the crash is
        // naturally rejected as stale here rather than silently reinstated.
        for (key, value) in admission_store.iterate_prefix(ADMISSION_KEY_PREFIX)? {
            let restore = bcs::from_bytes::<Transaction>(&value)
                .map_err(|error| PipelineError::Encoding(error.to_string()))
                .and_then(|transaction| source.admit(transaction));
            if let Err(error) = restore {
                debug!(%error, "dropping stale persisted admission on restart");
                admission_store.delete(&key)?;
            }
        }
        // Re-apply any peer bans persisted before a restart, so a peer banned
        // for repeated invalid gossip stays banned across a crash rather than
        // getting a clean slate (TD-014). Malformed entries are pruned.
        for peer in persisted_bans(&admission_store)? {
            if let Err(error) = network_node.handle.ban_peer(peer) {
                debug!(%peer, %error, "failed to re-apply a persisted ban on restart");
            }
        }
        let handle = Stage2PipelineHandle {
            source: Arc::clone(&source),
            network: network_node.handle.clone(),
        };
        let peer_ids = source.validator_peers.values().copied().collect();
        // Includes a finalized-but-not-yet-committed block restored from the
        // lifecycle's durable pending log. Starting from committed height here
        // made the pipeline wait forever to re-fetch and re-submit a height the
        // lifecycle was already executing after restart.
        let submitted_height = lifecycle.submitted_height();
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
                submitted_height,
                config,
                offense_counts: BTreeMap::new(),
                store: admission_store,
                inbound_repair_requests: network_node.inbound_repair_requests,
                repair_after: None,
                repair_backoff: config.payload_repair_delay,
                repair_peer_cursor: 0,
                progress_height: submitted_height,
                stalled_since: None,
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
                        if let Some(peer) = transaction.source
                            && is_peer_misbehaviour(&error)
                        {
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
                            if is_peer_misbehaviour(&error) {
                                self.record_offense(source);
                            }
                        }
                    }
                }
                Some(order) = self.finalized_orders.recv() => {
                    // Consensus and application recovery are independent. A
                    // coordinator can replay an order that the lifecycle has
                    // already restored from its own durable pending log; it is
                    // idempotent at this boundary and must not re-enter the
                    // payload-repair queue.
                    if order.height > self.submitted_height {
                        self.pending_orders.insert(order.height, order);
                    }
                    // Drain first: a backlog that clears is ordinary deferred
                    // execution, not a fault. Only a backlog that both exceeds
                    // the bound and stays frozen counts as a stall.
                    self.submit_available_orders()?;
                    self.enforce_execution_progress()?;
                }
                Some((peer, height)) = self.inbound_repair_requests.recv() => {
                    // Serving a peer is best-effort: not holding the payload is
                    // normal, and a failure here must never be fatal.
                    if let Err(error) = self.serve_payload_repair(peer, height) {
                        debug!(%peer, height, %error, "could not serve a payload repair request");
                    }
                }
                _ = ticker.tick() => {
                    self.submit_available_orders()?;
                    self.request_payload_repair_if_stuck();
                    self.lifecycle.poll_commit()?;
                    // A block just committed frees the executor slot; hand it the
                    // next height immediately so a catching-up validator drains
                    // its backlog at execution speed rather than one per tick.
                    self.submit_available_orders()?;
                    self.enforce_execution_progress()?;
                }
                else => return Err(PipelineError::PipelineClosed),
            }
        }
    }

    /// Records one invalid message from `peer`, banning it once its offense
    /// count reaches `peer_offense_ban_threshold`. The ban is persisted before
    /// it is applied so it survives a restart (TD-014); it is not shared with
    /// other nodes and never automatically lifts.
    fn record_offense(&mut self, peer: PeerId) {
        let offenses = tally_offense(&mut self.offense_counts, peer);
        if offenses == self.config.peer_offense_ban_threshold {
            if let Err(error) = persist_ban(&self.store, peer) {
                debug!(%peer, %error, "failed to persist peer ban");
            }
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

    /// Re-sends a height's shreds to a peer that could not reconstruct it.
    ///
    /// Only this layer can answer: shreds are keyed by the hash of the payload,
    /// which a validator that never received it cannot know, so repair is keyed
    /// by height and resolved against the payloads held here.
    fn serve_payload_repair(&self, peer: PeerId, height: u64) -> Result<(), PipelineError> {
        let Some(payload) = self.source.payload_at_height(height)? else {
            trace!(%peer, height, "requested repair payload is unavailable");
            return Ok(());
        };
        for shred in payload.shreds(self.config.kestrel_cast)? {
            // Deliberately the ordinary delivery path, so a repaired shred is
            // validated, size-checked and fault-injected exactly like any other.
            self.network.try_send_shred(peer, shred)?;
        }
        debug!(%peer, height, "served a payload repair request");
        Ok(())
    }

    /// Asks peers for the payloads this validator is stuck on, once they have
    /// clearly not arrived by themselves, then backs off so genuinely
    /// unavailable payloads cannot generate unbounded traffic.
    ///
    /// It requests a *batch* of upcoming certified heights, not just the head:
    /// a validator catching up from far behind must pull missing blocks faster
    /// than the chain produces them, or it never converges. The backoff still
    /// bounds traffic when nothing is arriving, but every successful submission
    /// resets it (see `submit_available_orders`), so an actively converging
    /// validator keeps requesting at the base cadence rather than backing off.
    fn request_payload_repair_if_stuck(&mut self) {
        let head = self.submitted_height.saturating_add(1);
        if !self.pending_orders.contains_key(&head) {
            // Nothing certified is waiting, so nothing is missing.
            self.repair_after = None;
            self.repair_backoff = self.config.payload_repair_delay;
            return;
        }
        let now = Instant::now();
        let Some(deadline) = self.repair_after else {
            self.repair_after = Some(now + self.config.payload_repair_delay);
            return;
        };
        if now < deadline {
            return;
        }
        let peers = self
            .peer_ids
            .iter()
            .copied()
            .filter(|peer| *peer != self.local_peer_id)
            .collect::<Vec<_>>();
        if peers.is_empty() {
            return;
        }
        // The next contiguous certified heights we cannot yet execute. Spread
        // across peers round-robin so no single one serves the whole batch.
        let wanted = self
            .pending_orders
            .range(head..)
            .take(PAYLOAD_REPAIR_BATCH)
            .map(|(height, _)| *height)
            .collect::<Vec<_>>();
        for (index, height) in wanted.iter().enumerate() {
            let peer = peers[rotating_peer_index(self.repair_peer_cursor, index, peers.len())];
            if let Err(error) = self.network.try_request_payload(peer, *height) {
                debug!(%peer, height = *height, %error, "could not queue a payload repair request");
            }
        }
        self.repair_peer_cursor = (self.repair_peer_cursor + 1) % peers.len();
        warn!(
            from_height = head,
            count = wanted.len(),
            "certified payloads have not arrived; asking peers to resend them"
        );
        self.repair_backoff = self
            .repair_backoff
            .saturating_mul(2)
            .min(self.config.maximum_payload_repair_backoff);
        self.repair_after = Some(now + self.repair_backoff);
    }

    /// Stops the validator only if execution has *genuinely* stalled: its order
    /// backlog exceeds the bound and has made no forward progress for the whole
    /// [`execution_stall_grace`](Stage2PipelineConfig::execution_stall_grace)
    /// window.
    ///
    /// A backlog on its own is not a fault — ordering leads execution by design,
    /// and a validator catching up on missed heights holds a large backlog
    /// while it re-fetches and executes them. The earlier instantaneous
    /// "backlog over bound ⇒ fatal" check could not tell that apart from a real
    /// stall, so a rejoining validator killed itself the moment catch-up
    /// delivered more than a buffer's worth of orders faster than it could fetch
    /// their payloads. Keying the failure on *lack of progress* preserves the
    /// original guard against genuinely frozen execution (which must not keep
    /// voting on blocks it will never execute) while letting catch-up proceed.
    fn enforce_execution_progress(&mut self) -> Result<(), PipelineError> {
        // Any advance means execution is still draining the backlog: reset.
        if self.submitted_height > self.progress_height {
            self.progress_height = self.submitted_height;
            self.stalled_since = None;
            return Ok(());
        }
        if self.pending_orders.len() <= self.config.maximum_pending_orders {
            // Within the ordinary lead of ordering over execution.
            self.stalled_since = None;
            return Ok(());
        }
        let now = Instant::now();
        let stalled_since = *self.stalled_since.get_or_insert(now);
        let stalled_for = now.duration_since(stalled_since);
        if !backlog_is_a_genuine_stall(
            self.pending_orders.len(),
            self.config.maximum_pending_orders,
            stalled_for,
            self.config.execution_stall_grace,
        ) {
            return Ok(());
        }
        let awaiting_height = self.submitted_height.saturating_add(1);
        warn!(
            pending = self.pending_orders.len(),
            limit = self.config.maximum_pending_orders,
            awaiting_height,
            stalled_ms = stalled_for.as_millis(),
            "execution made no progress while its order backlog stayed over the bound for the \
             whole grace window; stopping this validator rather than finalizing blocks it cannot \
             execute"
        );
        Err(PipelineError::ExecutionBacklogExceeded {
            pending: self.pending_orders.len(),
            limit: self.config.maximum_pending_orders,
            awaiting_height,
        })
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
            if let Err(error) = self.lifecycle.submit_payload(order, &payload) {
                if is_execution_backpressure(&error) {
                    // The deferred executor is one block behind. That is the
                    // backpressure the spec mandates, not a fault: leave the
                    // order in `pending_orders` and retry on the next tick, once
                    // `poll_commit` has drained a completed block and freed the
                    // slot. Treating it as fatal crashed the pipeline under
                    // sustained load -- the executor legitimately falls a block
                    // behind at high throughput, so the node killed itself
                    // exactly when it was busiest, and two such deaths cost the
                    // network its quorum. The `maximum_pending_orders` bound
                    // above still catches execution that has genuinely stopped.
                    return Ok(());
                }
                return Err(error.into());
            }
            self.pending_orders.remove(&height);
            // A certified, submitted order is canonical for this height —
            // durable execution/commit still has to happen, but no other
            // block will ever contend for these transactions again. Retiring
            // them from the local mempool/reservation bookkeeping here,
            // rather than waiting for `poll_commit` to observe the durable
            // commit, closes a window where a validator that was not this
            // height's leader (so its own mempool never popped these
            // transactions out via `select_block`) could still have them
            // sitting in queue and re-select them into a later height it
            // does lead — a real, reproduced bug: the later block would
            // duplicate an already-submitted transaction and every honest
            // validator would independently reject it as a nonce reuse,
            // fatally tearing down the pipeline task on the trusted
            // finalized-order path.
            self.source.note_submitted(&payload)?;
            self.submitted_height = height;
            self.repair_after = None;
            self.repair_backoff = self.config.payload_repair_delay;
        }
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
    /// Candidate payloads for heights not yet submitted, keyed by height plus a
    /// discriminator: this validator's own proposals use the parent id, so a
    /// re-proposal of the same height and parent is deterministic, while
    /// payloads reconstructed from shreds use their own payload id, so
    /// competing proposals for one height each keep a slot instead of
    /// overwriting each other. `payload_for_order` picks whichever one the
    /// certified order actually names; `note_submitted` prunes by height.
    payloads: BTreeMap<(u64, Hash), PropagatedBlock>,
    shreds: BTreeMap<Hash, BTreeMap<u16, Shred>>,
    propagated: BTreeSet<(u64, Hash)>,
    /// The last height this validator's own mempool has retired transactions
    /// for (see `note_submitted`). A new proposal may not be built for any
    /// height beyond this plus one: the consensus coordinator's own notion of
    /// "current height" advances the instant a certificate forms, on its own
    /// task, independently of when the pipeline task gets around to draining
    /// the `FinalizedOrder` for that height off its channel and retiring its
    /// transactions here. Without this gate, a validator that never led an
    /// earlier height (so its own `select_block` never popped that height's
    /// transactions out) could race ahead and re-select an already-certified
    /// transaction into a later height it does lead.
    retired_height: u64,
    /// Inbound shreds accepted from the network, and payloads successfully
    /// reconstructed from them. A validator that did not propose a block can
    /// only execute that height once its payload arrives this way, so these
    /// separate "no shreds are reaching me at all" from "shreds arrive but
    /// never complete a block".
    shreds_received: u64,
    payloads_reconstructed: u64,
    /// Monotonic arrival order of each in-flight shred group, so the stalest
    /// can be evicted when the buffer is full. See `evict_stale_shred_group`.
    shred_arrivals: BTreeMap<Hash, u64>,
    shred_sequence: u64,
    /// Payloads for heights already submitted, kept solely to answer a peer's
    /// repair request. `note_submitted` prunes `payloads` as soon as a height is
    /// handed to execution, which would otherwise leave exactly the heights a
    /// lagging validator needs unavailable from every peer that is keeping up.
    recent_payloads: BTreeMap<u64, PropagatedBlock>,
}

/// Makes room for shreds belonging to `incoming` when the in-flight buffer is
/// full, by dropping the group that was last updated longest ago.
///
/// A group is otherwise only removed once it completes, and plenty never do:
/// a validator receives shreds for blocks it does not need and partial sets it
/// can never finish. Those accumulate until the buffer is permanently full, and
/// from then on every shred for a *new* block is refused — so the validator can
/// never reconstruct another payload and its execution stops for good while
/// consensus keeps ordering ahead of it. Evicting the stalest group instead
/// keeps the memory bound while preserving liveness: the newest block is the
/// one execution is most likely waiting on.
///
/// Returns whether room is now available for `incoming`.
fn evict_stale_shred_group(
    shreds: &mut BTreeMap<Hash, BTreeMap<u16, Shred>>,
    arrivals: &mut BTreeMap<Hash, u64>,
    capacity: usize,
    incoming: Hash,
) -> bool {
    if shreds.contains_key(&incoming) || shreds.len() < capacity {
        return true;
    }
    let stalest = arrivals
        .iter()
        .filter(|(block_id, _)| **block_id != incoming)
        .min_by_key(|(_, arrival)| **arrival)
        .map(|(block_id, _)| *block_id);
    if let Some(stalest) = stalest {
        shreds.remove(&stalest);
        arrivals.remove(&stalest);
        return true;
    }
    false
}

/// Observability for the shred/payload path, which decides whether a validator
/// can execute a height it did not propose itself. Without a payload the
/// pipeline retries that height indefinitely and executes nothing, while
/// consensus keeps ordering — so these counters are what distinguish a
/// delivery failure from a reconstruction failure.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ShredStats {
    pub shreds_received: u64,
    pub payloads_reconstructed: u64,
    /// Payloads available to match against a finalized order, whether
    /// reconstructed from shreds or built locally as proposer.
    pub payloads_available: usize,
    /// Blocks holding some shreds but not yet enough to reconstruct.
    pub incomplete_shred_blocks: usize,
}

struct PipelineProposalSource {
    state: Mutex<PipelineState>,
    validator: TransactionValidator,
    network: NetworkHandle,
    validator_peers: BTreeMap<Hash, PeerId>,
    candidates: Vec<RelayCandidate>,
    config: Stage2PipelineConfig,
    /// Durable log of admitted-but-not-yet-finalized transactions, so a
    /// restart does not silently drop one (the sender would otherwise have
    /// to notice and resubmit). Shares the node's single `RocksDB` store
    /// (see `BlockLifecycle::store_handle`) rather than opening a second one.
    admission_store: Arc<RocksDbStore>,
}

const ADMISSION_KEY_PREFIX: &[u8] = b"pipeline/admission/pending/";
const BAN_KEY_PREFIX: &[u8] = b"pipeline/ban/v1/";
/// A leader's deterministic proposal for `(height, parent)`. This must outlive
/// a crash once the consensus replica may have durably voted for its block ID:
/// rebuilding from a changed/reordered mempool can produce a different block,
/// which the replica correctly refuses to double-vote for, while its earlier
/// order vote also prevents a timeout vote in that view.
const PROPOSAL_KEY_PREFIX: &[u8] = b"pipeline/proposal/v1/";
/// Executed heights whose payloads stay available *in memory* to answer a
/// repair request on the hot path.
const RETAINED_PAYLOADS_FOR_REPAIR: usize = 64;
/// Executed heights whose payloads are retained *durably* so a validator that
/// fell far behind can fetch and re-execute the blocks it missed. Matches the
/// consensus catch-up order-retention window (`coordinator::CATCHUP_RETENTION`):
/// any height the coordinator can replay as a finalized order, some peer can
/// also serve as a payload, so catch-up does not deliver orders that can never
/// be executed (INC-002's underlying gap).
const DURABLE_PAYLOAD_HISTORY: u64 = 1_024;
const PAYLOAD_HISTORY_KEY_PREFIX: &[u8] = b"pipeline/payload-history/v1/";
/// How many upcoming missing payloads a lagging validator requests per repair
/// round instead of one. A validator catching up must pull blocks faster than
/// the chain produces them or it never converges; kept below
/// `maximum_inflight_shred_blocks` so a batch's reconstructions all fit.
const PAYLOAD_REPAIR_BATCH: usize = 32;

fn rotating_peer_index(cursor: usize, batch_index: usize, peer_count: usize) -> usize {
    debug_assert!(peer_count > 0);
    (cursor + batch_index) % peer_count
}

fn admission_key(id: Hash) -> Vec<u8> {
    let mut key = ADMISSION_KEY_PREFIX.to_vec();
    key.extend_from_slice(id.as_bytes());
    key
}

fn payload_history_key(height: u64) -> Vec<u8> {
    let mut key = PAYLOAD_HISTORY_KEY_PREFIX.to_vec();
    key.extend_from_slice(&height.to_be_bytes());
    key
}

fn proposal_key(height: u64, parent_id: Hash) -> Vec<u8> {
    let mut key = PROPOSAL_KEY_PREFIX.to_vec();
    key.extend_from_slice(&height.to_be_bytes());
    key.extend_from_slice(parent_id.as_bytes());
    key
}

fn ban_key(peer: PeerId) -> Vec<u8> {
    let mut key = BAN_KEY_PREFIX.to_vec();
    key.extend_from_slice(&peer.to_bytes());
    key
}

/// Durably records that `peer` is banned. The value is empty; the key's
/// presence is the ban.
fn persist_ban(store: &RocksDbStore, peer: PeerId) -> Result<(), StorageError> {
    store.put(&ban_key(peer), &[])
}

/// Reads back every persisted ban, pruning any entry whose key does not decode
/// to a valid `PeerId` (so a corrupt record can never wedge startup).
fn persisted_bans(store: &RocksDbStore) -> Result<Vec<PeerId>, PipelineError> {
    let mut peers = Vec::new();
    for (key, _value) in store.iterate_prefix(BAN_KEY_PREFIX)? {
        match PeerId::from_bytes(&key[BAN_KEY_PREFIX.len()..]) {
            Ok(peer) => peers.push(peer),
            Err(_) => store.delete(&key)?,
        }
    }
    Ok(peers)
}

fn persisted_payload_at_height(
    store: &RocksDbStore,
    height: u64,
) -> Result<Option<PropagatedBlock>, PipelineError> {
    match store.get(&payload_history_key(height))? {
        Some(bytes) => {
            Ok(Some(bcs::from_bytes(&bytes).map_err(|error| {
                PipelineError::Encoding(error.to_string())
            })?))
        }
        None => Ok(None),
    }
}

fn persist_payload_history(
    store: &RocksDbStore,
    payload: &PropagatedBlock,
) -> Result<(), PipelineError> {
    store.put(
        &payload_history_key(payload.height),
        &bcs::to_bytes(payload).map_err(|error| PipelineError::Encoding(error.to_string()))?,
    )?;
    if let Some(prune_below) = payload.height.checked_sub(DURABLE_PAYLOAD_HISTORY) {
        store.delete(&payload_history_key(prune_below))?;
    }
    Ok(())
}

fn persisted_proposal(
    store: &RocksDbStore,
    height: u64,
    parent_id: Hash,
) -> Result<Option<PropagatedBlock>, PipelineError> {
    match store.get(&proposal_key(height, parent_id))? {
        Some(bytes) => {
            Ok(Some(bcs::from_bytes(&bytes).map_err(|error| {
                PipelineError::Encoding(error.to_string())
            })?))
        }
        None => Ok(None),
    }
}

fn persist_proposal(store: &RocksDbStore, payload: &PropagatedBlock) -> Result<(), PipelineError> {
    store.put(
        &proposal_key(payload.height, payload.parent_id),
        &bcs::to_bytes(payload).map_err(|error| PipelineError::Encoding(error.to_string()))?,
    )?;
    Ok(())
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
        // Persist before any in-memory mutation: if this fails, admission
        // aborts cleanly with no partial mempool/reservation state to unwind.
        let encoded = bcs::to_bytes(&transaction)
            .map_err(|error| PipelineError::Encoding(error.to_string()))?;
        self.admission_store.put(&admission_key(id), &encoded)?;
        state.arrival_sequence = state.arrival_sequence.saturating_add(1);
        if let Err(error) = state.mempool.submit(SubmittedTransaction {
            id,
            sender: transaction.sender,
            scope,
            touched_objects,
            compute_limit: payload.executable.compute_limit,
            max_fee_per_compute: payload.max_fee_per_compute,
            priority_fee_per_compute: payload.priority_fee_per_compute,
            arrival_sequence,
            policy_data: payload.policy_data,
        }) {
            // Nothing durable should outlive a rejected admission.
            self.admission_store.delete(&admission_key(id))?;
            return Err(error.into());
        }
        let sender = transaction.sender;
        state.reserved_senders.insert(sender, id);
        state.transactions.insert(id, transaction);
        // One event per transaction per validator floods a debug capture,
        // so this stays at trace; `profile_pipeline` enables trace to read it.
        trace!(transaction_id = %id, %sender, "admitted transaction");
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
        self.admission_store.delete(&admission_key(id))?;
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
        if let Some(payload) = persisted_proposal(&self.admission_store, height, parent_id)? {
            state.payloads.insert(key, payload.clone());
            return Ok(payload);
        }
        // The consensus coordinator's own height advances the instant a
        // certificate forms, on a separate task from this one — it can ask
        // for height N+1 before this validator's mempool has retired height
        // N's transactions (`note_submitted` runs only once the
        // `FinalizedOrder` for N works its way through the pipeline task's
        // channel). Selecting now would risk re-including a transaction
        // that is already canonical at an earlier height. Report not-ready
        // instead; the coordinator leaves the height open and retries.
        if height > state.retired_height.saturating_add(1) {
            return Err(PipelineError::PreviousHeightNotRetired {
                height,
                retired_height: state.retired_height,
            });
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
        let base_fees = selection
            .transactions
            .iter()
            .map(|pending| pending.local_base_fee_per_compute)
            .collect();
        let payload = PropagatedBlock {
            height,
            parent_id,
            transactions,
            base_fees,
        };
        debug!(
            height,
            transaction_count = payload.transactions.len(),
            scope_visits = selection.scope_visits,
            "built new block proposal"
        );
        // Persist before returning transaction IDs to consensus. The caller may
        // immediately sign and durably record an order vote for this payload's
        // block ID; after that point losing the payload makes the leader unable
        // to vote for a rebuilt block or cast a mutually-exclusive timeout.
        persist_proposal(&self.admission_store, &payload)?;
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
            let mut guard = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
            let state = &mut *guard;
            state.shreds_received = state.shreds_received.saturating_add(1);
            if !evict_stale_shred_group(
                &mut state.shreds,
                &mut state.shred_arrivals,
                self.config.maximum_inflight_shred_blocks,
                block_id,
            ) {
                return Err(PipelineError::InflightShredLimitReached);
            }
            state.shred_sequence = state.shred_sequence.saturating_add(1);
            state.shred_arrivals.insert(block_id, state.shred_sequence);
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
        // Key a reconstructed payload by its own identity, not by its parent.
        // Different leaders in different views propose *different* payloads for
        // the same (height, parent_id), so keying on the parent collapses them
        // onto one slot: whichever arrived first won it permanently, and
        // `or_insert` silently discarded the rest. A validator that reconstructed
        // a losing proposal first then held a payload whose transaction IDs could
        // never match the certified order, so `payload_for_order` returned None
        // for that height forever and its execution stopped there for good while
        // consensus kept ordering ahead of it. Keying by payload identity keeps
        // every candidate, and the height-based pruning below still bounds them.
        let payload_key = payload.payload_id()?;
        let mut state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
        state.payloads_reconstructed = state.payloads_reconstructed.saturating_add(1);
        debug!(
            height = payload.height,
            block = %payload_key,
            transaction_count = payload.transactions.len(),
            "reconstructed payload from shreds"
        );
        state
            .payloads
            .entry((payload.height, payload_key))
            .or_insert(payload);
        state.shreds.remove(&block_id);
        state.shred_arrivals.remove(&block_id);
        Ok(())
    }

    /// Any payload held for `height`, for answering a peer's repair request.
    /// Checks the in-memory caches first, then falls back to the durable
    /// block-history so a far-behind peer can be served a height evicted from
    /// memory but still within the retained window.
    fn payload_at_height(&self, height: u64) -> Result<Option<PropagatedBlock>, PipelineError> {
        {
            let state = self.state.lock().map_err(|_| PipelineError::LockPoisoned)?;
            if let Some(payload) = state
                .payloads
                .iter()
                .find(|((payload_height, _), _)| *payload_height == height)
                .map(|(_, payload)| payload.clone())
                .or_else(|| state.recent_payloads.get(&height).cloned())
            {
                return Ok(Some(payload));
            }
        }
        persisted_payload_at_height(&self.admission_store, height)
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

    /// Retires a certified, submitted order's transactions from local
    /// admission bookkeeping (mempool queues, sender reservations, nonce
    /// tracking, the durable admission log). Called once a block is
    /// submitted, not once it durably commits: a follower that never led
    /// this height never popped these transactions out of its own mempool
    /// via `select_block`, so leaving them queued until commit would let it
    /// re-select an already-canonical transaction into a later height it
    /// does lead.
    fn note_submitted(&self, payload: &PropagatedBlock) -> Result<(), PipelineError> {
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
            self.admission_store.delete(&admission_key(id))?;
        }
        state
            .recent_payloads
            .insert(payload.height, payload.clone());
        // Bounded in-memory hot cache for the common (slightly-behind) case.
        while state.recent_payloads.len() > RETAINED_PAYLOADS_FOR_REPAIR {
            let oldest = state.recent_payloads.keys().next().copied();
            if let Some(oldest) = oldest {
                state.recent_payloads.remove(&oldest);
            }
        }
        // Durable, bounded block-history so a validator that fell further behind
        // than the in-memory cache — or a peer serving one that did — can still
        // fetch and re-execute the blocks it missed, and survive a restart.
        persist_payload_history(&self.admission_store, payload)?;
        self.admission_store
            .delete(&proposal_key(payload.height, payload.parent_id))?;
        state
            .payloads
            .retain(|(height, _), _| *height > payload.height);
        state
            .propagated
            .retain(|(height, _)| *height > payload.height);
        state.retired_height = state.retired_height.max(payload.height);
        Ok(())
    }
}

impl ProposalTransactionSource for PipelineProposalSource {
    fn transaction_ids(&self, height: u64, parent_id: Hash) -> Option<(Vec<Hash>, Hash)> {
        let payload = self.build_or_get_payload(height, parent_id).ok()?;
        self.propagate(height, parent_id, &payload).ok()?;
        let transaction_ids = payload.transaction_ids().ok()?;
        Some((transaction_ids, payload.fee_commitment()))
    }

    fn is_empty(&self) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.transactions.is_empty())
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
    #[error(
        "cannot propose height {height}: this validator has only retired transactions through height {retired_height}"
    )]
    PreviousHeightNotRetired { height: u64, retired_height: u64 },
    #[error("the in-flight shred-block limit was reached")]
    InflightShredLimitReached,
    #[error(
        "execution has fallen too far behind ordering: {pending} certified orders are undelivered to execution (limit {limit}), still waiting to execute height {awaiting_height}"
    )]
    ExecutionBacklogExceeded {
        pending: usize,
        limit: usize,
        awaiting_height: u64,
    },
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
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Whether a submit-payload failure is the deferred executor reporting it is one
/// block behind -- the backpressure `consensus-spec` mandates, to be retried,
/// not a fault. It is the executor's own one-block bound, hit long before the
/// pipeline's `maximum_pending_orders` backlog bound, so it must be handled
/// gracefully rather than crashing the pipeline.
fn is_execution_backpressure(error: &LifecycleError) -> bool {
    matches!(
        error,
        LifecycleError::DeferredExecution(DeferredExecutionError::LagLimitReached)
    )
}

/// Whether an order backlog now represents a genuinely stalled execution rather
/// than a validator that is merely behind and catching up.
///
/// It is a stall only if the backlog both exceeds the steady-state bound *and*
/// has stayed frozen (no `submitted_height` progress) for the entire grace
/// window. A backlog within the bound, or one that is still advancing (grace
/// window not yet elapsed because progress keeps resetting it), is normal
/// deferred execution or catch-up, never a fault.
fn backlog_is_a_genuine_stall(
    pending: usize,
    limit: usize,
    stalled_for: Duration,
    grace: Duration,
) -> bool {
    pending > limit && stalled_for >= grace
}

/// Increments `peer`'s offense count and returns the new total.
fn tally_offense(offense_counts: &mut BTreeMap<PeerId, u32>, peer: PeerId) -> u32 {
    let count = offense_counts.entry(peer).or_insert(0);
    *count += 1;
    *count
}

/// Whether a rejected inbound message should count against the peer that sent
/// it. Only failures an honest peer cannot legitimately produce are counted.
///
/// This distinction is load-bearing, not cosmetic. Re-gossiping a transaction
/// that has since been committed is *normal* behaviour — the sender's nonce has
/// simply moved on — and so is racing two copies of the same sender's
/// transaction, or hitting a full mempool scope. Counting those as offenses
/// meant honest validators accumulated offenses against each other purely from
/// ordinary concurrent gossip and, at the default threshold of 8, permanently
/// banned one another: a self-inflicted network partition under load, made
/// worse now that bans survive restarts. Genuine misbehaviour a peer *is*
/// accountable for — undecodable bytes, a bad signature, an invalid shred —
/// still counts, so the denial-of-service protection from TD-025 is unchanged.
fn is_peer_misbehaviour(error: &PipelineError) -> bool {
    !matches!(
        error,
        // Ordinary races between commit and in-flight gossip.
        PipelineError::NonceMismatch { .. }
            | PipelineError::SenderAlreadyReserved(_)
            // Local congestion/limits, not the sender's doing. Gossip errors
            // here come from relaying a peer's shred failing because our own
            // outbound queue is full -- backpressure under load, never the
            // peer's fault. Counting it banned honest relaying peers, durably
            // (TD-014), exactly when the network was busiest.
            | PipelineError::Mempool(_)
            | PipelineError::InflightShredLimitReached
            | PipelineError::Gossip(_)
            // Our own internal faults are never the peer's fault.
            | PipelineError::LockPoisoned
            | PipelineError::Storage(_)
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use libp2p::PeerId;
    use storage::{KvStore, RocksDbStore};
    use tempfile::TempDir;
    use types::Hash;

    use super::{
        BAN_KEY_PREFIX, DURABLE_PAYLOAD_HISTORY, PipelineError, PropagatedBlock,
        is_peer_misbehaviour, persist_ban, persist_payload_history, persist_proposal,
        persisted_bans, persisted_payload_at_height, persisted_proposal, proposal_key,
        rotating_peer_index, tally_offense,
    };

    #[test]
    fn a_single_missing_height_rotates_across_repair_peers() {
        let visited = (0..6)
            .map(|repair_round| rotating_peer_index(repair_round, 0, 3))
            .collect::<Vec<_>>();
        assert_eq!(visited, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn a_full_shred_buffer_evicts_the_stalest_group_instead_of_wedging() {
        use std::collections::BTreeMap;
        use types::Hash;
        let capacity = 4;
        let mut shreds: BTreeMap<Hash, BTreeMap<u16, super::Shred>> = BTreeMap::new();
        let mut arrivals: BTreeMap<Hash, u64> = BTreeMap::new();

        // Fill the buffer with groups that will never complete, which is what
        // happens in practice: a validator receives shreds for blocks it does
        // not need, and partial sets it can never finish.
        for index in 0..capacity {
            let block = Hash::digest([u8::try_from(index).unwrap()]);
            shreds.insert(block, BTreeMap::new());
            arrivals.insert(block, index as u64);
        }
        assert_eq!(shreds.len(), capacity);

        // A shred for a new block must still be accepted. Before this, it was
        // refused outright, so once the buffer filled with stuck groups the
        // validator could never reconstruct another payload and its execution
        // stopped permanently while consensus kept ordering ahead of it.
        let wanted = Hash::digest(b"the block execution is waiting for");
        assert!(super::evict_stale_shred_group(
            &mut shreds,
            &mut arrivals,
            capacity,
            wanted
        ));
        // The stalest group made way, and the bound still holds.
        assert!(!shreds.contains_key(&Hash::digest([0_u8])));
        assert_eq!(shreds.len(), capacity - 1);

        // An in-flight group is never evicted to make room for itself.
        let existing = Hash::digest([1_u8]);
        assert!(super::evict_stale_shred_group(
            &mut shreds,
            &mut arrivals,
            capacity,
            existing
        ));
        assert!(shreds.contains_key(&existing));
    }

    #[test]
    fn executor_one_block_behind_is_treated_as_backpressure_not_a_crash() {
        use super::is_execution_backpressure;
        use crate::LifecycleError;
        use execution::DeferredExecutionError;

        // The deferred executor's one-block bound is the backpressure the spec
        // mandates: retried, never fatal. Before this, it crashed the pipeline
        // under sustained load, and two crashes cost the network its quorum.
        assert!(is_execution_backpressure(
            &LifecycleError::DeferredExecution(DeferredExecutionError::LagLimitReached)
        ));

        // A genuinely stopped worker, or any other lifecycle failure, is still
        // fatal.
        assert!(!is_execution_backpressure(
            &LifecycleError::DeferredExecution(DeferredExecutionError::WorkerStopped)
        ));
        assert!(!is_execution_backpressure(&LifecycleError::OrderMismatch));
    }

    #[test]
    fn a_catching_up_backlog_is_not_a_stall_but_a_frozen_one_is() {
        use super::backlog_is_a_genuine_stall;
        use std::time::Duration;

        let limit = 64;
        let grace = Duration::from_secs(30);

        // A backlog within the bound is ordinary deferred execution, whatever
        // the elapsed time — ordering is allowed to lead execution.
        assert!(!backlog_is_a_genuine_stall(limit, limit, grace, grace));
        assert!(!backlog_is_a_genuine_stall(
            10,
            limit,
            Duration::from_secs(3_600),
            grace
        ));

        // Over the bound but not yet frozen for the whole window: this is a
        // validator catching up on missed heights, re-fetching payloads and
        // executing them in a burst. It must NOT be killed — the exact case
        // where the old instantaneous check crashed a rejoining validator.
        assert!(!backlog_is_a_genuine_stall(
            1_000,
            limit,
            Duration::from_secs(29),
            grace
        ));
        assert!(!backlog_is_a_genuine_stall(
            limit + 1,
            limit,
            Duration::ZERO,
            grace
        ));

        // Over the bound AND frozen for the whole grace window: execution has
        // genuinely stopped and the validator must stop rather than keep voting
        // on blocks it will never execute.
        assert!(backlog_is_a_genuine_stall(limit + 1, limit, grace, grace));
        assert!(backlog_is_a_genuine_stall(
            1_000,
            limit,
            Duration::from_secs(45),
            grace
        ));
    }

    #[test]
    fn routine_gossip_races_are_not_counted_as_peer_offenses() {
        // An honest validator re-gossiping a transaction that has since
        // committed produces exactly this, and must never be banned for it.
        assert!(!is_peer_misbehaviour(&PipelineError::NonceMismatch {
            expected: 1,
            received: 0,
        }));
        assert!(!is_peer_misbehaviour(
            &PipelineError::SenderAlreadyReserved(types::Address::from_bytes([7; 32]))
        ));
        // Local congestion and our own internal faults are not the peer's doing.
        assert!(!is_peer_misbehaviour(
            &PipelineError::InflightShredLimitReached
        ));
        assert!(!is_peer_misbehaviour(&PipelineError::LockPoisoned));

        // A relay send failing because our own outbound queue is full is local
        // backpressure, not the peer's fault -- it must not ban honest peers
        // under load.
        assert!(!is_peer_misbehaviour(&PipelineError::Gossip(
            network::GossipError::ShredQueueUnavailable
        )));

        // Genuine misbehaviour a peer is accountable for still counts, so the
        // TD-025 denial-of-service protection is unchanged.
        assert!(is_peer_misbehaviour(&PipelineError::Encoding(
            "malformed bytes".to_owned()
        )));
        assert!(is_peer_misbehaviour(&PipelineError::NonceOverflow));
    }

    #[test]
    fn persisted_bans_survive_a_store_reopen_and_tolerate_corrupt_entries() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("store");
        let banned = PeerId::random();
        let also_banned = PeerId::random();

        // First "process": persist two bans, plus a corrupt ban record whose
        // key does not decode to a PeerId.
        {
            let store = RocksDbStore::open(&path).unwrap();
            persist_ban(&store, banned).unwrap();
            persist_ban(&store, also_banned).unwrap();
            let mut corrupt = BAN_KEY_PREFIX.to_vec();
            corrupt.extend_from_slice(b"not-a-peer-id");
            store.put(&corrupt, &[]).unwrap();
        }

        // Second "process": reopen at the same path (a restart). Both real bans
        // must come back; the corrupt entry is skipped and pruned.
        let store = RocksDbStore::open(&path).unwrap();
        let restored = persisted_bans(&store).unwrap();
        assert_eq!(
            restored.iter().copied().collect::<BTreeSet<_>>(),
            BTreeSet::from([banned, also_banned]),
            "every persisted ban must be re-read after a restart"
        );
        // The corrupt record was deleted on read, so a second pass returns only
        // the two valid bans and leaves nothing malformed behind.
        assert_eq!(persisted_bans(&store).unwrap().len(), 2);
    }

    #[test]
    fn payload_history_survives_restart_and_prunes_to_the_catchup_window() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("store");
        let last_height = DURABLE_PAYLOAD_HISTORY + 2;

        {
            let store = RocksDbStore::open(&path).unwrap();
            for height in 1..=last_height {
                persist_payload_history(
                    &store,
                    &PropagatedBlock {
                        height,
                        parent_id: Hash::digest(height.to_be_bytes()),
                        transactions: Vec::new(),
                        base_fees: Vec::new(),
                    },
                )
                .unwrap();
            }
        }

        // Reopening stands in for the serving peer restarting after it
        // committed these blocks. The two heights outside the 1,024-height
        // catch-up window are gone, while both ends of the retained range
        // decode back to their exact payload.
        let store = RocksDbStore::open(&path).unwrap();
        assert!(persisted_payload_at_height(&store, 1).unwrap().is_none());
        assert!(persisted_payload_at_height(&store, 2).unwrap().is_none());
        for height in [3, last_height] {
            let payload = persisted_payload_at_height(&store, height)
                .unwrap()
                .expect("a retained payload must survive restart");
            assert_eq!(payload.height, height);
            assert_eq!(payload.parent_id, Hash::digest(height.to_be_bytes()));
        }
    }

    #[test]
    fn a_leaders_proposal_identity_survives_restart_until_submission() {
        let directory = TempDir::new().unwrap();
        let path = directory.path().join("store");
        let payload = PropagatedBlock {
            height: 41,
            parent_id: Hash::digest(b"proposal-parent"),
            transactions: Vec::new(),
            base_fees: Vec::new(),
        };

        {
            let store = RocksDbStore::open(&path).unwrap();
            persist_proposal(&store, &payload).unwrap();
        }

        let store = RocksDbStore::open(&path).unwrap();
        assert_eq!(
            persisted_proposal(&store, payload.height, payload.parent_id).unwrap(),
            Some(payload.clone()),
            "a restarted leader must rebuild the exact block it may already have voted for"
        );
        store
            .delete(&proposal_key(payload.height, payload.parent_id))
            .unwrap();
        assert!(
            persisted_proposal(&store, payload.height, payload.parent_id)
                .unwrap()
                .is_none(),
            "a submitted height no longer needs its pre-consensus proposal record"
        );
    }

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
