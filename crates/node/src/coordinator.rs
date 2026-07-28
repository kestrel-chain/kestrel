use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use consensus::{
    CertificateKind, ConsensusError, FinalizedOrder, Proposal, QuorumCertificate, Replica,
    ReplicaSnapshot, SignedProposal, ValidatorSet, Vote, VoteCollector, VotePhase,
    verify_certificate, verify_vote,
};
use crypto::{AggregateSignatureScheme, Bls12381Scheme};
use rpc::NodeStatus;
use serde::{Deserialize, Serialize};
use storage::{KvStore, RocksDbStore, StorageError};
use thiserror::Error;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::{MissedTickBehavior, interval, sleep, timeout},
};
use tracing::{debug, warn};
use types::Hash;

use crate::{GenesisDocument, GenesisError};

/// Turns any replica *refusal to sign a vote* into "skip this vote", never a
/// fatal fault. Applied only to the vote-casting calls (`vote_for_proposal`,
/// `vote_to_commit`), whose every error means "do not vote for this" —
/// `ProposalRoundMismatch`, `WrongLeader`, `InvalidProposal`,
/// `LockedOnDifferentBlock`, `ConflictingFirstRoundVote`, `LocalDoubleVote`, or
/// a bad justify certificate.
///
/// The invariant that makes this safe: **declining to cast a vote can never
/// cause a safety violation** — safety is only ever endangered by *casting*
/// conflicting votes, which these refusals exist to prevent. So the correct
/// response to a refusal is simply not to vote; the round then advances by
/// timeout or the validator advances by catching up. Propagating a refusal as
/// fatal instead crashed the whole coordinator, taking a slow, restarted, or
/// partitioned validator off the network — and under fault churn a second such
/// death cost the network its quorum. This was found three separate times
/// (`ConflictingFirstRoundVote`, then `LocalDoubleVote` in INC-003, then
/// `LockedOnDifferentBlock`), each the same class as INC-001/INC-002; keying on
/// the invariant rather than on individual variants ends that whack-a-mole.
///
/// Certificate *application* (`advance_view`, `finalize`) is deliberately not
/// routed through here: an error there (e.g. `SafetyViolation`) is a genuine
/// invariant break and must stay fatal.
fn skip_safe_vote_refusal(result: Result<Vote, ConsensusError>) -> Option<Vote> {
    match result {
        Ok(vote) => Some(vote),
        // The equivocation/lock safety guards firing — rarer, worth surfacing.
        Err(
            error @ (ConsensusError::LocalDoubleVote
            | ConsensusError::ConflictingFirstRoundVote
            | ConsensusError::LockedOnDifferentBlock),
        ) => {
            warn!(%error, "declined to sign an unsafe vote; skipping it rather than failing");
            None
        }
        // Stale, misrouted, or wrong-round proposals arrive routinely.
        Err(error) => {
            debug!(%error, "skipping a vote for an unvotable proposal");
            None
        }
    }
}

/// Round timeout for `view`, doubling each view up to `MAX_TIMEOUT_DOUBLINGS`.
///
/// A fixed round timeout cannot recover from an environment slower than it.
/// Once rounds stop completing within it, replicas split -- some cast this
/// view's order vote, the rest time out -- and because an honest replica may
/// not cast both, neither side reaches its quorum. The view cannot advance and
/// the height cannot finalize, so the chain makes no progress for as long as
/// the condition lasts. Backing the timeout off per view is what partial
/// synchrony relies on for liveness: it grows until it exceeds the real round
/// time, at which point rounds complete again. The view resets to 0 on every
/// committed height, so a healthy chain always runs at the base timeout.
fn round_timeout_for_view(base: Duration, view: u64) -> Duration {
    const MAX_TIMEOUT_DOUBLINGS: u32 = 6;
    let doublings = u32::try_from(view).unwrap_or(MAX_TIMEOUT_DOUBLINGS);
    base.saturating_mul(1_u32 << doublings.min(MAX_TIMEOUT_DOUBLINGS))
}

const SAFETY_STATE_KEY: &[u8] = b"consensus/replica-snapshot/v1";
const CONNECT_TIMEOUT: Duration = Duration::from_millis(250);
/// Gives transaction gossip one measured propagation window to reach the next
/// leader before it snapshots an otherwise-empty mempool into a proposal.
const EMPTY_MEMPOOL_PROPAGATION_MARGIN: Duration = Duration::from_millis(15);

/// Static real-socket consensus timings and termination bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatorConfig {
    pub fast_path_wait: Duration,
    pub round_timeout: Duration,
    pub proposal_rebroadcast: Duration,
    pub proposal_vote_delay: Duration,
    pub maximum_message_bytes: usize,
    pub stop_after_height: Option<u64>,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            fast_path_wait: Duration::from_millis(80),
            round_timeout: Duration::from_millis(300),
            proposal_rebroadcast: Duration::from_millis(50),
            proposal_vote_delay: Duration::from_millis(20),
            maximum_message_bytes: 4 * 1024 * 1024,
            stop_after_height: None,
        }
    }
}

/// Operator-controlled Stage 2 fault injection. All fields are disabled by default.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CoordinatorFaults {
    pub withhold_votes: bool,
    pub certificate_emission: CertificateEmission,
    pub corrupt_votes: bool,
    pub equivocate_when_leader: bool,
    pub blocked_peers: BTreeSet<Hash>,
    pub outbound_delay: Duration,
    pub drop_basis_points: u16,
    pub proposal_delay: Duration,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum CertificateEmission {
    #[default]
    Enabled,
    /// Forms and disseminates votes but never originates a certificate. This
    /// models a leader that collects votes and withholds the aggregate.
    Withhold,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CoordinatorOutcome {
    pub finalized_height: u64,
    pub finalized_block: Hash,
    pub finality_latency_ms: u64,
    pub view_changes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Envelope {
    sender: Hash,
    message: WireMessage,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
enum WireMessage {
    Proposal(SignedProposal),
    Vote(Vote),
    Certificate {
        certificate: QuorumCertificate,
        transaction_ids: Option<Vec<Hash>>,
        fee_commitment: Option<Hash>,
    },
    /// "I am behind; send me finalized orders starting at this height." Sent by
    /// a validator that saw a certificate for a height beyond its own.
    RequestOrders {
        from_height: u64,
    },
    /// A contiguous batch of retained finalized orders answering a request,
    /// ascending from the requested height.
    Orders(Vec<FinalizedOrder>),
}

/// Supplies the canonical transaction IDs and fee commitment a leader should
/// order at a height. Returning `None` leaves the height open without
/// proposing synthetic work. The fee commitment binds whatever per-transaction
/// local base fees the source chose (see `consensus::fee_commitment`) into the
/// certified block, so it must be the same one recoverable from the actual
/// propagated payload — see `BlockLifecycle::submit_payload`.
pub trait ProposalTransactionSource: Send + Sync {
    fn transaction_ids(&self, height: u64, parent_id: Hash) -> Option<(Vec<Hash>, Hash)>;

    /// Reports whether asking for a proposal now would snapshot an empty
    /// transaction set. Sources that cannot answer cheaply should retain the
    /// default and will not be delayed.
    fn is_empty(&self) -> bool {
        false
    }
}

#[derive(Debug)]
struct SyntheticProposalSource;

impl ProposalTransactionSource for SyntheticProposalSource {
    fn transaction_ids(&self, height: u64, _parent_id: Hash) -> Option<(Vec<Hash>, Hash)> {
        Some((vec![Hash::digest(height.to_be_bytes())], Hash::default()))
    }
}

/// Multi-process coordinator using authenticated protocol messages over real TCP sockets.
pub struct ConsensusCoordinator {
    id: Hash,
    private_key: Vec<u8>,
    validators: ValidatorSet,
    scheme: Arc<dyn AggregateSignatureScheme>,
    peers: BTreeMap<Hash, SocketAddr>,
    listener: Option<TcpListener>,
    store: Arc<RocksDbStore>,
    replica: Replica,
    status: Arc<RwLock<NodeStatus>>,
    config: CoordinatorConfig,
    faults: CoordinatorFaults,
    proposal_source: Arc<dyn ProposalTransactionSource>,
    finalized_order_sender: Option<mpsc::UnboundedSender<FinalizedOrder>>,
    /// When this validator last asked a peer to replay missing orders, used to
    /// throttle catch-up requests.
    last_catchup_request: Option<Instant>,
}

impl ConsensusCoordinator {
    /// Validates configuration, binds the advertised TCP address, and restores
    /// durable safety state when present.
    ///
    /// # Errors
    ///
    /// Returns genesis/key/address/storage/listener validation failures.
    pub async fn bind(
        genesis: &GenesisDocument,
        id: Hash,
        private_key: Vec<u8>,
        data_directory: impl AsRef<Path>,
        status: Arc<RwLock<NodeStatus>>,
        config: CoordinatorConfig,
        faults: CoordinatorFaults,
    ) -> Result<Self, CoordinatorError> {
        Self::bind_inner(
            genesis,
            id,
            private_key,
            data_directory,
            status,
            config,
            faults,
            Arc::new(SyntheticProposalSource),
            None,
        )
        .await
    }

    /// Binds consensus to a real proposal source and a non-blocking finalized-order sink.
    ///
    /// # Errors
    ///
    /// Returns genesis/key/address/storage/listener validation failures.
    #[allow(clippy::too_many_arguments)]
    pub async fn bind_with_pipeline(
        genesis: &GenesisDocument,
        id: Hash,
        private_key: Vec<u8>,
        data_directory: impl AsRef<Path>,
        status: Arc<RwLock<NodeStatus>>,
        config: CoordinatorConfig,
        faults: CoordinatorFaults,
        proposal_source: Arc<dyn ProposalTransactionSource>,
        finalized_order_sender: mpsc::UnboundedSender<FinalizedOrder>,
    ) -> Result<Self, CoordinatorError> {
        Self::bind_inner(
            genesis,
            id,
            private_key,
            data_directory,
            status,
            config,
            faults,
            proposal_source,
            Some(finalized_order_sender),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn bind_inner(
        genesis: &GenesisDocument,
        id: Hash,
        private_key: Vec<u8>,
        data_directory: impl AsRef<Path>,
        status: Arc<RwLock<NodeStatus>>,
        config: CoordinatorConfig,
        faults: CoordinatorFaults,
        proposal_source: Arc<dyn ProposalTransactionSource>,
        finalized_order_sender: Option<mpsc::UnboundedSender<FinalizedOrder>>,
    ) -> Result<Self, CoordinatorError> {
        if config.fast_path_wait.is_zero()
            || config.round_timeout <= config.fast_path_wait
            || config.proposal_rebroadcast.is_zero()
            || config.proposal_vote_delay.is_zero()
            || config.proposal_vote_delay >= config.fast_path_wait
            || config.maximum_message_bytes == 0
            || faults.drop_basis_points > 10_000
        {
            return Err(CoordinatorError::InvalidConfiguration);
        }
        let validated = genesis.validate()?;
        let scheme: Arc<dyn AggregateSignatureScheme> = Arc::new(Bls12381Scheme);
        let validator = validated
            .validators
            .validator(id)
            .ok_or(CoordinatorError::UnknownLocalValidator)?;
        if scheme.public_key(&private_key)? != validator.public_key {
            return Err(CoordinatorError::PrivateKeyMismatch);
        }
        let peers = genesis
            .validators
            .iter()
            .map(|entry| {
                entry
                    .network_address
                    .parse::<SocketAddr>()
                    .map(|address| (entry.validator.id, address))
                    .map_err(|_| {
                        CoordinatorError::InvalidNetworkAddress(entry.network_address.clone())
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let listen_address = *peers
            .get(&id)
            .ok_or(CoordinatorError::UnknownLocalValidator)?;
        let listener = TcpListener::bind(listen_address).await?;
        std::fs::create_dir_all(data_directory.as_ref())?;
        let store = Arc::new(RocksDbStore::open(data_directory)?);
        let replica = match store.get(SAFETY_STATE_KEY)? {
            Some(bytes) => Replica::restore(
                id,
                private_key.clone(),
                validated.validators.clone(),
                Arc::clone(&scheme),
                bcs::from_bytes::<ReplicaSnapshot>(&bytes)
                    .map_err(|error| CoordinatorError::Encoding(error.to_string()))?,
            )?,
            None => Replica::new(
                id,
                private_key.clone(),
                validated.validators.clone(),
                Arc::clone(&scheme),
                1,
                validated.genesis_hash,
            )?,
        };
        Ok(Self {
            id,
            private_key,
            validators: validated.validators,
            scheme,
            peers,
            listener: Some(listener),
            store,
            replica,
            status,
            config,
            faults,
            proposal_source,
            finalized_order_sender,
            last_catchup_request: None,
        })
    }

    /// Runs consensus until the optional height bound is reached.
    ///
    /// # Errors
    ///
    /// Returns listener, persistence, encoding, or consensus failures.
    pub async fn run(
        mut self,
        genesis_unix_ms: u64,
    ) -> Result<CoordinatorOutcome, CoordinatorError> {
        let (incoming_sender, mut incoming) = mpsc::channel(4_096);
        let listener = self
            .listener
            .take()
            .ok_or(CoordinatorError::ListenerAlreadyTaken)?;
        let maximum_message_bytes = self.config.maximum_message_bytes;
        let listener_task = AbortTask(tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let sender = incoming_sender.clone();
                tokio::spawn(async move {
                    if let Ok(envelope) = read_envelope(stream, maximum_message_bytes).await {
                        let _ = sender.send(envelope).await;
                    }
                });
            }
        }));
        wait_for_genesis(genesis_unix_ms).await;

        let mut round = RoundState::new();
        let mut ticker = interval(Duration::from_millis(10));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        let mut last_outcome = CoordinatorOutcome {
            finalized_height: self.replica.height().saturating_sub(1),
            finalized_block: self.replica.parent_id(),
            finality_latency_ms: 0,
            view_changes: 0,
        };
        loop {
            if self
                .config
                .stop_after_height
                .is_some_and(|height| self.replica.height() > height)
            {
                drop(listener_task);
                return Ok(last_outcome);
            }
            tokio::select! {
                Some(envelope) = incoming.recv() => {
                    if !self.faults.blocked_peers.contains(&envelope.sender)
                        && let Some(outcome) = self.handle(envelope, &mut round).await?
                    {
                        last_outcome = outcome;
                    }
                }
                _ = ticker.tick() => {
                    if let Some(outcome) = self.on_tick(&mut round).await? {
                        last_outcome = outcome;
                    }
                }
            }
        }
    }

    #[allow(clippy::too_many_lines)] // Keeping the round transition order visible aids safety review.
    async fn on_tick(
        &mut self,
        round: &mut RoundState,
    ) -> Result<Option<CoordinatorOutcome>, CoordinatorError> {
        let height = self.replica.height();
        let view = self.replica.view();
        let leader = self.validators.leader(height, view).id;
        // Proposal delay/readiness may suppress proposal creation, but must
        // never suppress the pacemaker below. Returning early here (or when the
        // proposal source was temporarily not ready) froze the current view
        // forever because the round-timeout code was never reached.
        let proposal_delay_elapsed = round.proposal_delay_elapsed(self.faults.proposal_delay);
        if self.id == leader
            && proposal_delay_elapsed
            && self.faults.equivocate_when_leader
            && !round.has(RoundFlag::EquivocationSent)
        {
            self.broadcast_equivocation(height, view).await?;
            round.set(RoundFlag::EquivocationSent);
        } else if self.id == leader
            && proposal_delay_elapsed
            && !self.faults.equivocate_when_leader
            && (round.proposal.is_none()
                || round.last_proposal_broadcast.elapsed() >= self.config.proposal_rebroadcast)
        {
            if round.proposal.is_none() {
                let defer_empty = round.should_defer_empty_proposal(
                    EMPTY_MEMPOOL_PROPAGATION_MARGIN,
                    self.proposal_source.is_empty(),
                );
                if defer_empty {
                    debug!(
                        height,
                        view, "waiting briefly for transaction gossip before an empty proposal"
                    );
                } else if let Some((transaction_ids, fee_commitment)) = self
                    .proposal_source
                    .transaction_ids(height, self.replica.parent_id())
                {
                    debug!(
                        height,
                        view,
                        transaction_count = transaction_ids.len(),
                        "proposing as leader"
                    );
                    let proposal = Proposal::new(
                        height,
                        view,
                        self.replica.parent_id(),
                        self.id,
                        transaction_ids,
                        fee_commitment,
                        None,
                    );
                    let signed = SignedProposal::sign(
                        proposal.clone(),
                        &self.private_key,
                        self.scheme.as_ref(),
                    )?;
                    round.proposal = Some(signed);
                    if self.faults.withhold_votes {
                        debug!(height, view, "withholding local leader order vote");
                    } else if let Some(vote) =
                        skip_safe_vote_refusal(self.replica.vote_for_proposal(&proposal))
                    {
                        self.persist()?;
                        round.order_votes.insert(vote.validator, vote.clone());
                        round.proposal_vote = Some(vote);
                    } else {
                        debug!(
                            height,
                            view, "already timed out this view; not proposing into it"
                        );
                    }
                } else {
                    // A recovering pipeline can temporarily refuse to build
                    // height H+1 until H is retired locally. Keep ticking so
                    // the round can time out and another leader can take over.
                    debug!(height, view, "leader has no proposal available");
                }
            }
            if let Some(proposal) = &round.proposal {
                self.broadcast(WireMessage::Proposal(proposal.clone()))
                    .await;
                round.last_proposal_broadcast = Instant::now();
            }
        }

        if self.id != leader
            && !self.faults.withhold_votes
            && !round.has(RoundFlag::EquivocationDetected)
            && round
                .first_proposal_at
                .is_some_and(|received| received.elapsed() >= self.config.proposal_vote_delay)
            && let Some(signed) = round.observed_proposals.values().next()
            && !round.has(RoundFlag::ProposalVoteSent)
        {
            let voted = skip_safe_vote_refusal(self.replica.vote_for_proposal(&signed.proposal));
            // Marked either way: a view this replica has already timed out
            // will never produce an order vote, so retrying the signing
            // operation on every tick would only spin and re-log.
            round.set(RoundFlag::ProposalVoteSent);
            if let Some(mut vote) = voted {
                self.persist()?;
                if self.faults.corrupt_votes && !vote.signature.is_empty() {
                    vote.signature[0] ^= 1;
                }
                round.order_votes.insert(vote.validator, vote.clone());
                round.proposal_vote = Some(vote);
            } else {
                debug!(
                    height,
                    view, "already timed out this view; skipping its order vote"
                );
            }
        }
        if let Some(vote) = round.proposal_vote.clone()
            && round
                .last_proposal_vote_sent
                .is_none_or(|sent| sent.elapsed() >= self.config.proposal_rebroadcast)
        {
            debug!(
                height,
                view,
                block = %vote.block_id,
                "broadcasting order vote"
            );
            self.broadcast(WireMessage::Vote(vote)).await;
            round.last_proposal_vote_sent = Some(Instant::now());
        }

        if self.faults.certificate_emission == CertificateEmission::Enabled
            && let Some(proposal) = round.proposal_for_certification()
        {
            let block_id = proposal.proposal.block_id;
            let proposal_transaction_ids = proposal.proposal.transaction_ids.clone();
            let proposal_fee_commitment = proposal.proposal.fee_commitment;
            if let Some(certificate) = make_certificate(
                &self.validators,
                Arc::clone(&self.scheme),
                CertificateKind::Fast,
                height,
                view,
                block_id,
                &round.order_votes,
            ) {
                let transaction_ids = proposal_transaction_ids.clone();
                self.broadcast(WireMessage::Certificate {
                    certificate: certificate.clone(),
                    transaction_ids: Some(transaction_ids.clone()),
                    fee_commitment: Some(proposal_fee_commitment),
                })
                .await;
                return self.apply_certificate(
                    &certificate,
                    transaction_ids,
                    proposal_fee_commitment,
                    round,
                );
            }
            if !round.has(RoundFlag::PrepareSent)
                && round.started.elapsed() >= self.config.fast_path_wait
                && let Some(certificate) = make_certificate(
                    &self.validators,
                    Arc::clone(&self.scheme),
                    CertificateKind::Prepare,
                    height,
                    view,
                    block_id,
                    &round.order_votes,
                )
            {
                round.set(RoundFlag::PrepareSent);
                self.broadcast(WireMessage::Certificate {
                    certificate: certificate.clone(),
                    transaction_ids: Some(proposal_transaction_ids.clone()),
                    fee_commitment: Some(proposal_fee_commitment),
                })
                .await;
                self.apply_prepare(&certificate, round).await?;
            }
            if let Some(certificate) = make_certificate(
                &self.validators,
                Arc::clone(&self.scheme),
                CertificateKind::Commit,
                height,
                view,
                block_id,
                &round.commit_votes,
            ) {
                let transaction_ids = proposal_transaction_ids;
                self.broadcast(WireMessage::Certificate {
                    certificate: certificate.clone(),
                    transaction_ids: Some(transaction_ids.clone()),
                    fee_commitment: Some(proposal_fee_commitment),
                })
                .await;
                return self.apply_certificate(
                    &certificate,
                    transaction_ids,
                    proposal_fee_commitment,
                    round,
                );
            }
        }

        if let Some(vote) = round.commit_vote.clone()
            && round
                .last_commit_vote_sent
                .is_none_or(|sent| sent.elapsed() >= self.config.proposal_rebroadcast)
        {
            debug!(
                height,
                view,
                block = %vote.block_id,
                "broadcasting commit vote"
            );
            self.broadcast(WireMessage::Vote(vote)).await;
            round.last_commit_vote_sent = Some(Instant::now());
        }

        if round.started.elapsed() >= round_timeout_for_view(self.config.round_timeout, view)
            && !round.has(RoundFlag::TimeoutSent)
        {
            round.set(RoundFlag::TimeoutSent);
            debug!(height, view, "round timed out");
            if let Some(vote) = self.replica.local_timeout()? {
                self.persist()?;
                // Broadcast timeout votes so view recovery does not depend on
                // the designated next leader already being caught up to this
                // height. The vote is individually signed and the resulting
                // certificate still needs the full timeout quorum.
                round.timeout_votes.insert(vote.validator, vote.clone());
                self.broadcast(WireMessage::Vote(vote)).await;
            }
        }
        if let Some(certificate) = make_certificate(
            &self.validators,
            Arc::clone(&self.scheme),
            CertificateKind::Timeout,
            height,
            view,
            Hash::default(),
            &round.timeout_votes,
        ) {
            self.broadcast(WireMessage::Certificate {
                certificate: certificate.clone(),
                transaction_ids: None,
                fee_commitment: None,
            })
            .await;
            debug!(height, view, "timeout certificate formed; advancing view");
            self.replica.advance_view(&certificate)?;
            self.persist()?;
            *round = RoundState::new();
        }
        Ok(None)
    }

    #[allow(clippy::too_many_lines)] // Keeping the full message-dispatch match visible aids safety review.
    async fn handle(
        &mut self,
        envelope: Envelope,
        round: &mut RoundState,
    ) -> Result<Option<CoordinatorOutcome>, CoordinatorError> {
        match envelope.message {
            WireMessage::Proposal(signed) => {
                if envelope.sender != signed.proposal.proposer {
                    return Ok(None);
                }
                signed.verify(&self.validators, self.scheme.as_ref())?;
                if signed.proposal.height != self.replica.height()
                    || signed.proposal.view != self.replica.view()
                {
                    return Ok(None);
                }
                if self.faults.withhold_votes {
                    return Ok(None);
                }
                let block_id = signed.proposal.block_id;
                if round
                    .observed_proposals
                    .insert(block_id, signed.clone())
                    .is_none()
                {
                    round.first_proposal_at.get_or_insert_with(Instant::now);
                    if round.relayed_proposals.insert(block_id) {
                        self.broadcast(WireMessage::Proposal(signed)).await;
                    }
                }
                if round.observed_proposals.len() > 1 {
                    round.set(RoundFlag::EquivocationDetected);
                }
            }
            WireMessage::Vote(vote) => {
                if envelope.sender != vote.validator
                    || vote.height != self.replica.height()
                    || vote.view != self.replica.view()
                {
                    return Ok(None);
                }
                if let Err(error) = verify_vote(&vote, &self.validators, self.scheme.as_ref()) {
                    debug!(%error, validator = %vote.validator, "discarding invalid vote");
                    return Ok(None);
                }
                match vote.phase {
                    VotePhase::Order => {
                        round.order_votes.entry(vote.validator).or_insert(vote);
                    }
                    VotePhase::Commit => {
                        round.commit_votes.entry(vote.validator).or_insert(vote);
                    }
                    VotePhase::Timeout => {
                        if vote.block_id == Hash::default() {
                            round.timeout_votes.entry(vote.validator).or_insert(vote);
                        }
                    }
                }
            }
            WireMessage::Certificate {
                certificate,
                transaction_ids,
                fee_commitment,
            } => {
                // Every certificate is quorum-authenticated in its own right.
                // Any admitted validator may aggregate and relay it, removing
                // the designated leader as a single point of liveness failure.
                if self.validators.validator(envelope.sender).is_none() {
                    return Ok(None);
                }
                let expected_sender = self
                    .validators
                    .leader(certificate.height, certificate.view)
                    .id;
                verify_certificate(&certificate, &self.validators, self.scheme.as_ref())?;
                if certificate.height != self.replica.height() {
                    if certificate.height > self.replica.height() {
                        // A verified certificate for a height beyond ours proves
                        // we have fallen behind (it cannot be forged). Ask the
                        // sender to replay the orders we are missing.
                        self.request_catchup(envelope.sender).await;
                    }
                    return Ok(None);
                }
                let certified_order = match certificate.kind {
                    CertificateKind::Timeout => {
                        if transaction_ids.is_some() || fee_commitment.is_some() {
                            return Err(CoordinatorError::InvalidCertifiedOrder);
                        }
                        None
                    }
                    CertificateKind::Fast | CertificateKind::Prepare | CertificateKind::Commit => {
                        let transaction_ids =
                            transaction_ids.ok_or(CoordinatorError::MissingCertifiedOrder)?;
                        let fee_commitment =
                            fee_commitment.ok_or(CoordinatorError::MissingCertifiedOrder)?;
                        let expected = Proposal::new(
                            certificate.height,
                            certificate.view,
                            self.replica.parent_id(),
                            expected_sender,
                            transaction_ids.clone(),
                            fee_commitment,
                            None,
                        );
                        if expected.block_id != certificate.block_id {
                            return Err(CoordinatorError::InvalidCertifiedOrder);
                        }
                        Some((transaction_ids, fee_commitment))
                    }
                };
                match certificate.kind {
                    CertificateKind::Prepare => self.apply_prepare(&certificate, round).await?,
                    CertificateKind::Fast | CertificateKind::Commit => {
                        let (transaction_ids, fee_commitment) =
                            certified_order.ok_or(CoordinatorError::MissingCertifiedOrder)?;
                        return self.apply_certificate(
                            &certificate,
                            transaction_ids,
                            fee_commitment,
                            round,
                        );
                    }
                    CertificateKind::Timeout => {
                        if certificate.height == self.replica.height()
                            && certificate.view == self.replica.view()
                        {
                            debug!(
                                height = certificate.height,
                                view = certificate.view,
                                "received timeout certificate; advancing view"
                            );
                            self.replica.advance_view(&certificate)?;
                            self.persist()?;
                            *round = RoundState::new();
                        }
                    }
                }
            }
            WireMessage::RequestOrders { from_height } => {
                let orders = self.load_finalized_orders(
                    from_height,
                    from_height.saturating_add(MAXIMUM_CATCHUP_BATCH as u64),
                )?;
                if !orders.is_empty() {
                    self.send(envelope.sender, WireMessage::Orders(orders))
                        .await;
                }
            }
            WireMessage::Orders(orders) => {
                return self.apply_catchup_orders(orders, round);
            }
        }
        Ok(None)
    }

    /// Asks `peer` to replay the finalized orders this validator is missing,
    /// throttled so repeated behind-signals do not flood the peer.
    async fn request_catchup(&mut self, peer: Hash) {
        let now = Instant::now();
        if self
            .last_catchup_request
            .is_some_and(|last| now.duration_since(last) < CATCHUP_REQUEST_INTERVAL)
        {
            return;
        }
        self.last_catchup_request = Some(now);
        let from_height = self.replica.height();
        debug!(from_height, %peer, "fell behind; requesting catch-up orders");
        self.send(peer, WireMessage::RequestOrders { from_height })
            .await;
    }

    /// Replays a peer's finalized orders to catch up. Each is untrusted, so its
    /// certificate is verified against the (immutable) validator set and its
    /// certified order is bound to the certificate's block id exactly as the
    /// live path does, before it is finalized. Orders apply strictly in height
    /// order; a gap or any invalid order stops the batch without failing.
    fn apply_catchup_orders(
        &mut self,
        orders: Vec<FinalizedOrder>,
        round: &mut RoundState,
    ) -> Result<Option<CoordinatorOutcome>, CoordinatorError> {
        let mut last_outcome = None;
        let mut advanced = false;
        for order in orders {
            if order.height < self.replica.height() {
                continue;
            }
            if order.height != self.replica.height() {
                break;
            }
            if !matches!(
                order.certificate.kind,
                CertificateKind::Fast | CertificateKind::Commit
            ) || order.certificate.height != order.height
                || order.certificate.block_id != order.block_id
                || verify_certificate(&order.certificate, &self.validators, self.scheme.as_ref())
                    .is_err()
            {
                break;
            }
            let leader = self
                .validators
                .leader(order.certificate.height, order.certificate.view)
                .id;
            let expected = Proposal::new(
                order.certificate.height,
                order.certificate.view,
                self.replica.parent_id(),
                leader,
                order.transaction_ids.clone(),
                order.fee_commitment,
                None,
            );
            if expected.block_id != order.certificate.block_id {
                break;
            }
            // The certificate is verified above, so `commit_finalized_order` can
            // only fail on a genuine internal fault, which stays fatal.
            last_outcome = Some(self.commit_finalized_order(order, 0)?);
            advanced = true;
        }
        if advanced {
            debug!(height = self.replica.height(), "caught up to a peer");
            *round = RoundState::new_height();
        }
        Ok(last_outcome)
    }

    async fn apply_prepare(
        &mut self,
        certificate: &QuorumCertificate,
        round: &mut RoundState,
    ) -> Result<(), CoordinatorError> {
        if certificate.height != self.replica.height() || certificate.view != self.replica.view() {
            return Ok(());
        }
        if self.faults.withhold_votes {
            return Ok(());
        }
        if round.commit_vote.is_some() {
            return Ok(());
        }
        // Same invariant as the order-vote path: a refusal to cast the fallback
        // commit vote (a stale prepare, or a lock on a different block) is safe
        // to skip, never fatal.
        let Some(mut vote) = skip_safe_vote_refusal(self.replica.vote_to_commit(certificate))
        else {
            return Ok(());
        };
        self.persist()?;
        if self.faults.corrupt_votes && !vote.signature.is_empty() {
            vote.signature[0] ^= 1;
        }
        round.commit_votes.insert(vote.validator, vote.clone());
        round.commit_vote = Some(vote.clone());
        self.broadcast(WireMessage::Vote(vote)).await;
        round.last_commit_vote_sent = Some(Instant::now());
        Ok(())
    }

    fn apply_certificate(
        &mut self,
        certificate: &QuorumCertificate,
        transaction_ids: Vec<Hash>,
        fee_commitment: Hash,
        round: &mut RoundState,
    ) -> Result<Option<CoordinatorOutcome>, CoordinatorError> {
        if certificate.height != self.replica.height() {
            return Ok(None);
        }
        let latency = u64::try_from(round.height_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let order = FinalizedOrder {
            height: certificate.height,
            block_id: certificate.block_id,
            transaction_ids,
            fee_commitment,
            certificate: certificate.clone(),
        };
        let outcome = self.commit_finalized_order(order, latency)?;
        *round = RoundState::new_height();
        Ok(Some(outcome))
    }

    /// Finalizes one certified order at the replica's current height: advances
    /// the replica, persists safety state and the order (for catch-up serving),
    /// updates RPC status, and hands the order to execution. Shared by the live
    /// consensus path and catch-up replay; the caller has already verified the
    /// certificate and that `order.height == self.replica.height()`.
    fn commit_finalized_order(
        &mut self,
        order: FinalizedOrder,
        latency_ms: u64,
    ) -> Result<CoordinatorOutcome, CoordinatorError> {
        let finalized_height = order.height;
        let view = order.certificate.view;
        let finalized_block = self.replica.finalize(&order.certificate)?;
        debug!(
            height = finalized_height,
            view,
            block = %finalized_block,
            latency_ms,
            "finalized height"
        );
        self.persist()?;
        let outcome = CoordinatorOutcome {
            finalized_height,
            finalized_block,
            finality_latency_ms: latency_ms,
            view_changes: view,
        };
        if let Ok(mut status) = self.status.write() {
            status.finalized_height = finalized_height;
            status.finalized_block = finalized_block;
            status.peer_count = self.peers.len().saturating_sub(1);
            status.ready = true;
            status.finality_latency_ms = Some(latency_ms);
            status.view_changes = view;
        }
        // Retain recent finalized orders durably so a peer that fell behind can
        // request and replay the heights it missed. Without this a validator
        // that restarts or is briefly partitioned can never rejoin: `finalize`
        // only accepts a certificate for its exact current height, and nothing
        // else can supply the missing ones.
        self.persist_finalized_order(&order)?;
        if let Some(sender) = &self.finalized_order_sender {
            sender
                .send(order)
                .map_err(|_| CoordinatorError::FinalizedOrderSinkClosed)?;
        }
        Ok(outcome)
    }

    async fn broadcast_equivocation(&self, height: u64, view: u64) -> Result<(), CoordinatorError> {
        let first = SignedProposal::sign(
            Proposal::new(
                height,
                view,
                self.replica.parent_id(),
                self.id,
                vec![Hash::digest(b"equivocation-a")],
                Hash::default(),
                None,
            ),
            &self.private_key,
            self.scheme.as_ref(),
        )?;
        let second = SignedProposal::sign(
            Proposal::new(
                height,
                view,
                self.replica.parent_id(),
                self.id,
                vec![Hash::digest(b"equivocation-b")],
                Hash::default(),
                None,
            ),
            &self.private_key,
            self.scheme.as_ref(),
        )?;
        for (index, peer) in self
            .peers
            .keys()
            .copied()
            .filter(|peer| *peer != self.id)
            .enumerate()
        {
            self.send(
                peer,
                WireMessage::Proposal(if index % 2 == 0 {
                    first.clone()
                } else {
                    second.clone()
                }),
            )
            .await;
        }
        Ok(())
    }

    async fn broadcast(&self, message: WireMessage) {
        for peer in self.peers.keys().copied().filter(|peer| *peer != self.id) {
            self.send(peer, message.clone()).await;
        }
    }

    async fn send(&self, peer: Hash, message: WireMessage) {
        if self.faults.blocked_peers.contains(&peer) || self.should_drop(peer, &message) {
            return;
        }
        let Some(address) = self.peers.get(&peer).copied() else {
            return;
        };
        if !self.faults.outbound_delay.is_zero() {
            sleep(self.faults.outbound_delay).await;
        }
        let envelope = Envelope {
            sender: self.id,
            message,
        };
        let _ = send_envelope(address, &envelope).await;
    }

    fn should_drop(&self, peer: Hash, message: &WireMessage) -> bool {
        if self.faults.drop_basis_points == 0 {
            return false;
        }
        let mut bytes = b"kestrel/stage2/drop/v1".to_vec();
        bytes.extend_from_slice(self.id.as_bytes());
        bytes.extend_from_slice(peer.as_bytes());
        if let Ok(encoded) = bcs::to_bytes(message) {
            bytes.extend_from_slice(&encoded);
        }
        let digest = Hash::digest(bytes);
        let sample = u16::from_be_bytes([digest.as_bytes()[0], digest.as_bytes()[1]]) % 10_000;
        sample < self.faults.drop_basis_points
    }

    fn persist(&self) -> Result<(), CoordinatorError> {
        let bytes = bcs::to_bytes(&self.replica.snapshot())
            .map_err(|error| CoordinatorError::Encoding(error.to_string()))?;
        self.store.put(SAFETY_STATE_KEY, &bytes)?;
        Ok(())
    }

    /// Durably retains `order` for catch-up serving, pruning history older than
    /// `CATCHUP_RETENTION` heights so the retained range stays bounded.
    fn persist_finalized_order(&self, order: &FinalizedOrder) -> Result<(), CoordinatorError> {
        let bytes =
            bcs::to_bytes(order).map_err(|error| CoordinatorError::Encoding(error.to_string()))?;
        self.store.put(&catchup_order_key(order.height), &bytes)?;
        if let Some(prune_below) = order.height.checked_sub(CATCHUP_RETENTION) {
            self.store.delete(&catchup_order_key(prune_below))?;
        }
        Ok(())
    }

    /// Reads the retained finalized orders for `from_height..=to_height`,
    /// capped at `MAXIMUM_CATCHUP_BATCH` to bound a single response, in height
    /// order. A gap (a height already pruned) stops the batch, since orders must
    /// be applied contiguously.
    fn load_finalized_orders(
        &self,
        from_height: u64,
        to_height: u64,
    ) -> Result<Vec<FinalizedOrder>, CoordinatorError> {
        let mut orders = Vec::new();
        for height in from_height..=to_height {
            if orders.len() >= MAXIMUM_CATCHUP_BATCH {
                break;
            }
            let Some(bytes) = self.store.get(&catchup_order_key(height))? else {
                break;
            };
            orders.push(
                bcs::from_bytes::<FinalizedOrder>(&bytes)
                    .map_err(|error| CoordinatorError::Encoding(error.to_string()))?,
            );
        }
        Ok(orders)
    }
}

const CATCHUP_KEY_PREFIX: &[u8] = b"consensus/catchup/order/v1/";
/// Finalized heights retained for serving catch-up. A restarted or briefly
/// partitioned validator that fell no further behind than this can rejoin by
/// replaying; one further behind needs full state sync (TD-012).
const CATCHUP_RETENTION: u64 = 1_024;
/// Orders returned in a single catch-up response, bounding its size.
const MAXIMUM_CATCHUP_BATCH: usize = 64;
/// Minimum spacing between a validator's catch-up requests, so a run of
/// behind-signals asks a peer to replay at most once per interval.
const CATCHUP_REQUEST_INTERVAL: Duration = Duration::from_millis(200);

fn catchup_order_key(height: u64) -> Vec<u8> {
    let mut key = CATCHUP_KEY_PREFIX.to_vec();
    key.extend_from_slice(&height.to_be_bytes());
    key
}

struct RoundState {
    started: Instant,
    height_started: Instant,
    follows_commit: bool,
    last_proposal_broadcast: Instant,
    proposal: Option<SignedProposal>,
    observed_proposals: BTreeMap<Hash, SignedProposal>,
    relayed_proposals: BTreeSet<Hash>,
    first_proposal_at: Option<Instant>,
    /// The already-signed order vote is retransmitted until a certificate
    /// arrives. Once an order vote is cast this protocol forbids a timeout vote
    /// in the same view, so a lost one-shot send would otherwise strand it.
    proposal_vote: Option<Vote>,
    last_proposal_vote_sent: Option<Instant>,
    /// The already-signed fallback commit vote is likewise retransmitted until
    /// a commit certificate arrives.
    commit_vote: Option<Vote>,
    last_commit_vote_sent: Option<Instant>,
    order_votes: BTreeMap<Hash, Vote>,
    commit_votes: BTreeMap<Hash, Vote>,
    timeout_votes: BTreeMap<Hash, Vote>,
    flags: BTreeSet<RoundFlag>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum RoundFlag {
    PrepareSent,
    TimeoutSent,
    EquivocationSent,
    EquivocationDetected,
    ProposalVoteSent,
}

struct AbortTask(tokio::task::JoinHandle<()>);

impl Drop for AbortTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl RoundState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            started: now,
            height_started: now,
            follows_commit: false,
            last_proposal_broadcast: now,
            proposal: None,
            observed_proposals: BTreeMap::new(),
            relayed_proposals: BTreeSet::new(),
            first_proposal_at: None,
            proposal_vote: None,
            last_proposal_vote_sent: None,
            commit_vote: None,
            last_commit_vote_sent: None,
            order_votes: BTreeMap::new(),
            commit_votes: BTreeMap::new(),
            timeout_votes: BTreeMap::new(),
            flags: BTreeSet::new(),
        }
    }

    fn new_height() -> Self {
        Self {
            follows_commit: true,
            ..Self::new()
        }
    }

    fn proposal_delay_elapsed(&self, delay: Duration) -> bool {
        self.started.elapsed() >= delay
    }

    fn should_defer_empty_proposal(&self, margin: Duration, source_is_empty: bool) -> bool {
        self.follows_commit && source_is_empty && self.height_started.elapsed() < margin
    }

    fn proposal_for_certification(&self) -> Option<SignedProposal> {
        self.proposal
            .clone()
            .or_else(|| self.observed_proposals.values().next().cloned())
    }

    fn has(&self, flag: RoundFlag) -> bool {
        self.flags.contains(&flag)
    }

    fn set(&mut self, flag: RoundFlag) {
        self.flags.insert(flag);
    }
}

fn make_certificate(
    validators: &ValidatorSet,
    scheme: Arc<dyn AggregateSignatureScheme>,
    kind: CertificateKind,
    height: u64,
    view: u64,
    block_id: Hash,
    votes: &BTreeMap<Hash, Vote>,
) -> Option<QuorumCertificate> {
    let mut collector = VoteCollector::new(validators, scheme, kind, height, view, block_id);
    let mut certificate = None;
    for vote in votes.values().cloned() {
        if let Ok(result) = collector.add_vote(vote)
            && result.is_some()
        {
            certificate = result;
        }
    }
    certificate
}

async fn wait_for_genesis(genesis_unix_ms: u64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let target = u128::from(genesis_unix_ms);
    if target > now {
        let delay = u64::try_from(target - now).unwrap_or(u64::MAX);
        sleep(Duration::from_millis(delay)).await;
    }
}

async fn send_envelope(address: SocketAddr, envelope: &Envelope) -> Result<(), CoordinatorError> {
    let bytes =
        bcs::to_bytes(envelope).map_err(|error| CoordinatorError::Encoding(error.to_string()))?;
    let length = u32::try_from(bytes.len()).map_err(|_| CoordinatorError::MessageTooLarge)?;
    let mut stream = timeout(CONNECT_TIMEOUT, TcpStream::connect(address))
        .await
        .map_err(|_| CoordinatorError::ConnectTimeout)??;
    stream.write_all(&length.to_be_bytes()).await?;
    stream.write_all(&bytes).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn read_envelope(
    mut stream: TcpStream,
    maximum_message_bytes: usize,
) -> Result<Envelope, CoordinatorError> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length).await?;
    let length = usize::try_from(u32::from_be_bytes(length)).unwrap_or(usize::MAX);
    if length == 0 || length > maximum_message_bytes {
        return Err(CoordinatorError::MessageTooLarge);
    }
    let mut bytes = vec![0_u8; length];
    stream.read_exact(&mut bytes).await?;
    bcs::from_bytes(&bytes).map_err(|error| CoordinatorError::Encoding(error.to_string()))
}

#[derive(Debug, Error)]
pub enum CoordinatorError {
    #[error("consensus coordinator configuration is invalid")]
    InvalidConfiguration,
    #[error("local validator is absent from genesis")]
    UnknownLocalValidator,
    #[error("validator private key does not match genesis")]
    PrivateKeyMismatch,
    #[error("invalid consensus socket address {0}")]
    InvalidNetworkAddress(String),
    #[error("consensus frame exceeds its configured limit")]
    MessageTooLarge,
    #[error("a non-timeout certificate omitted its canonical transaction order")]
    MissingCertifiedOrder,
    #[error("the transaction order attached to a certificate does not derive its block ID")]
    InvalidCertifiedOrder,
    #[error("the finalized-order execution sink is closed")]
    FinalizedOrderSinkClosed,
    #[error("consensus peer connection timed out")]
    ConnectTimeout,
    #[error("consensus listener was already consumed")]
    ListenerAlreadyTaken,
    #[error("consensus encoding failed: {0}")]
    Encoding(String),
    #[error(transparent)]
    Genesis(#[from] GenesisError),
    #[error(transparent)]
    Consensus(#[from] consensus::ConsensusError),
    #[error(transparent)]
    Crypto(#[from] crypto::CryptoError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        net::TcpListener as StdTcpListener,
        sync::{Arc, RwLock},
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };

    use consensus::Validator;
    use crypto::{Bls12381Scheme, SignatureScheme};
    use rpc::NodeStatus;
    use tempfile::TempDir;
    use types::Hash;

    use crate::{GENESIS_FORMAT_VERSION, GenesisDocument, GenesisValidator};

    use super::{
        ConsensusCoordinator, CoordinatorConfig, CoordinatorFaults, ProposalTransactionSource,
    };

    /// Serialises the real-TCP consensus tests against each other. Each starts
    /// five validators running BLS consensus in an unoptimized build; the test
    /// harness runs tests in parallel, so two of them together put ten such
    /// validators on the same cores. On a small CI runner that starves both,
    /// producing round timeouts and extra view changes that say nothing about
    /// the behaviour under test.
    static REAL_TCP_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Upper bound on real-TCP consensus rounds completing, not an expectation
    /// of how long they take: these finish in well under a second on an idle
    /// machine. It is generous because the value only matters when the machine
    /// is slow, and a bound calibrated on an idle developer machine fails on a
    /// small contended CI runner for reasons that have nothing to do with the
    /// property under test. Assertions after the wait still check the real
    /// behaviour, so a genuine hang fails on those rather than passing here.
    const REAL_TCP_CONSENSUS_BOUND: Duration = Duration::from_secs(150);

    struct PipelineSource;

    impl ProposalTransactionSource for PipelineSource {
        fn transaction_ids(&self, height: u64, parent_id: Hash) -> Option<(Vec<Hash>, Hash)> {
            let mut bytes = b"real-pipeline-transaction".to_vec();
            bytes.extend_from_slice(&height.to_be_bytes());
            bytes.extend_from_slice(parent_id.as_bytes());
            Some((vec![Hash::digest(bytes)], Hash::default()))
        }
    }

    #[test]
    fn every_vote_refusal_is_skipped_never_fatal() {
        // Declining to cast a vote can never cause a safety violation, so no
        // vote refusal is fatal — each was found (one at a time) crashing the
        // coordinator and, under fault churn, costing the network its quorum:
        // ConflictingFirstRoundVote (a timed-out view), LocalDoubleVote (a
        // restart re-proposal, INC-003), and LockedOnDifferentBlock (a lock
        // conflict). The routine round-mismatch/wrong-leader/invalid-proposal
        // refusals are equally safe to skip. All must return `None`, never fail.
        for error in [
            super::ConsensusError::ConflictingFirstRoundVote,
            super::ConsensusError::LocalDoubleVote,
            super::ConsensusError::LockedOnDifferentBlock,
            super::ConsensusError::ProposalRoundMismatch,
            super::ConsensusError::WrongLeader,
            super::ConsensusError::InvalidProposal,
        ] {
            assert!(
                super::skip_safe_vote_refusal(Err(error)).is_none(),
                "a vote refusal must be skipped, not crash the coordinator"
            );
        }
    }

    #[test]
    fn only_an_empty_post_commit_height_waits_for_the_propagation_margin() {
        let margin = Duration::from_millis(15);

        let initial_height = super::RoundState::new();
        assert!(!initial_height.should_defer_empty_proposal(margin, true));

        let mut post_commit = super::RoundState::new_height();
        assert!(post_commit.should_defer_empty_proposal(margin, true));
        assert!(!post_commit.should_defer_empty_proposal(margin, false));

        post_commit.height_started = Instant::now().checked_sub(margin).unwrap();
        assert!(!post_commit.should_defer_empty_proposal(margin, true));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::too_many_lines)] // Keep the full restart/continuation timeline visible for review.
    async fn five_real_tcp_nodes_finalize_the_same_two_heights() {
        let _serialised = REAL_TCP_TESTS.lock().await;
        let directory = TempDir::new().unwrap();
        let (genesis, keys, address_reservations) = fixture_genesis(5);
        let validated = genesis.validate().unwrap();
        drop(address_reservations);
        let mut tasks = Vec::new();
        let mut finalized_orders = Vec::new();
        for (index, entry) in genesis.validators.iter().enumerate() {
            let status = Arc::new(RwLock::new(NodeStatus {
                chain_id: genesis.chain_id.clone(),
                genesis_hash: validated.genesis_hash,
                finalized_height: 0,
                committed_height: 0,
                finalized_block: validated.genesis_hash,
                state_root: validated.state_root,
                peer_count: 0,
                ready: false,
                finality_latency_ms: None,
                view_changes: 0,
            }));
            let (finalized_order_sender, finalized_order_receiver) =
                tokio::sync::mpsc::unbounded_channel();
            let coordinator = ConsensusCoordinator::bind_with_pipeline(
                &genesis,
                entry.validator.id,
                keys[index].clone(),
                directory.path().join(index.to_string()),
                status,
                CoordinatorConfig {
                    stop_after_height: Some(2),
                    ..CoordinatorConfig::default()
                },
                CoordinatorFaults::default(),
                Arc::new(PipelineSource),
                finalized_order_sender,
            )
            .await
            .unwrap();
            finalized_orders.push(finalized_order_receiver);
            let genesis_time = genesis.genesis_unix_ms;
            tasks.push(tokio::spawn(async move {
                coordinator.run(genesis_time).await.unwrap()
            }));
        }
        let outcomes = tokio::time::timeout(REAL_TCP_CONSENSUS_BOUND, async {
            let mut outcomes = Vec::new();
            for task in tasks {
                outcomes.push(task.await.unwrap());
            }
            outcomes
        })
        .await
        .unwrap();
        assert!(outcomes.iter().all(|outcome| outcome.finalized_height == 2));
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.finalized_block == outcomes[0].finalized_block)
        );
        for receiver in &mut finalized_orders {
            let first = receiver.recv().await.unwrap();
            let second = receiver.recv().await.unwrap();
            assert_eq!((first.height, second.height), (1, 2));
            assert_eq!(first.transaction_ids.len(), 1);
            assert_eq!(second.transaction_ids.len(), 1);
            assert_eq!(second.block_id, outcomes[0].finalized_block);
        }

        tokio::time::sleep(Duration::from_millis(25)).await;
        let mut restarted = Vec::new();
        for (index, entry) in genesis.validators.iter().enumerate() {
            let status = Arc::new(RwLock::new(NodeStatus {
                chain_id: genesis.chain_id.clone(),
                genesis_hash: validated.genesis_hash,
                finalized_height: 2,
                committed_height: 2,
                finalized_block: outcomes[0].finalized_block,
                state_root: validated.state_root,
                peer_count: 0,
                ready: false,
                finality_latency_ms: None,
                view_changes: 0,
            }));
            let coordinator = ConsensusCoordinator::bind(
                &genesis,
                entry.validator.id,
                keys[index].clone(),
                directory.path().join(index.to_string()),
                status,
                CoordinatorConfig {
                    stop_after_height: Some(3),
                    ..CoordinatorConfig::default()
                },
                CoordinatorFaults::default(),
            )
            .await
            .unwrap();
            let genesis_time = genesis.genesis_unix_ms;
            restarted.push(tokio::spawn(async move {
                coordinator.run(genesis_time).await.unwrap()
            }));
        }
        let restarted = tokio::time::timeout(REAL_TCP_CONSENSUS_BOUND, async {
            let mut outcomes = Vec::new();
            for task in restarted {
                outcomes.push(task.await.unwrap());
            }
            outcomes
        })
        .await
        .unwrap();
        assert!(
            restarted
                .iter()
                .all(|outcome| outcome.finalized_height == 3)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn validators_form_a_fast_certificate_when_the_leader_withholds_it() {
        let _serialised = REAL_TCP_TESTS.lock().await;
        let outcomes = run_certificate_withholding_scenario(false).await;

        assert_eq!(outcomes.len(), 5);
        assert!(outcomes.iter().all(|outcome| outcome.finalized_height == 1));
        assert!(outcomes.iter().all(|outcome| outcome.view_changes == 0));
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.finalized_block == outcomes[0].finalized_block)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn validators_form_prepare_and_commit_certificates_when_the_leader_withholds_them() {
        let _serialised = REAL_TCP_TESTS.lock().await;
        let outcomes = run_certificate_withholding_scenario(true).await;

        assert_eq!(outcomes.len(), 5);
        assert!(outcomes.iter().all(|outcome| outcome.finalized_height == 1));
        assert!(outcomes.iter().all(|outcome| outcome.view_changes == 0));
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.finalized_block == outcomes[0].finalized_block)
        );
    }

    async fn run_certificate_withholding_scenario(
        force_fallback: bool,
    ) -> Vec<super::CoordinatorOutcome> {
        let directory = TempDir::new().unwrap();
        let (mut genesis, keys, address_reservations) = fixture_genesis(5);
        let initial = genesis.validate().unwrap();
        let leader = initial.validators.leader(1, 0).id;
        let silent_voter = genesis
            .validators
            .iter()
            .map(|entry| entry.validator.id)
            .find(|id| *id != leader)
            .unwrap();

        if force_fallback {
            // Three honest replicas contribute exactly 60%. The 10% leader
            // withholds its own vote and the remaining 30% replica does not
            // vote. This can form prepare and commit certificates, but can
            // never reach the 80% fast threshold.
            for entry in &mut genesis.validators {
                entry.validator.stake = if entry.validator.id == leader {
                    10
                } else if entry.validator.id == silent_voter {
                    30
                } else {
                    20
                };
            }
        }
        let validated = genesis.validate().unwrap();
        drop(address_reservations);

        let mut tasks = Vec::new();
        for (index, entry) in genesis.validators.iter().enumerate() {
            let status = Arc::new(RwLock::new(NodeStatus {
                chain_id: genesis.chain_id.clone(),
                genesis_hash: validated.genesis_hash,
                finalized_height: 0,
                committed_height: 0,
                finalized_block: validated.genesis_hash,
                state_root: validated.state_root,
                peer_count: 0,
                ready: false,
                finality_latency_ms: None,
                view_changes: 0,
            }));
            let coordinator = ConsensusCoordinator::bind(
                &genesis,
                entry.validator.id,
                keys[index].clone(),
                directory
                    .path()
                    .join(format!("certificate-withholding-{index}")),
                status,
                CoordinatorConfig {
                    stop_after_height: Some(1),
                    ..CoordinatorConfig::default()
                },
                CoordinatorFaults {
                    withhold_votes: entry.validator.id == leader
                        || (force_fallback && entry.validator.id == silent_voter),
                    certificate_emission: if entry.validator.id == leader {
                        super::CertificateEmission::Withhold
                    } else {
                        super::CertificateEmission::Enabled
                    },
                    ..CoordinatorFaults::default()
                },
            )
            .await
            .unwrap();
            let genesis_time = genesis.genesis_unix_ms;
            tasks.push(tokio::spawn(async move {
                coordinator.run(genesis_time).await.unwrap()
            }));
        }

        tokio::time::timeout(REAL_TCP_CONSENSUS_BOUND, async {
            let mut outcomes = Vec::new();
            for task in tasks {
                outcomes.push(task.await.unwrap());
            }
            outcomes
        })
        .await
        .expect("a withholding leader stranded votes that already reached quorum")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[allow(clippy::too_many_lines)] // Keep the full fault timeline visible for safety review.
    async fn real_tcp_nodes_recover_after_leader_kill_with_a_corrupt_voter() {
        let _serialised = REAL_TCP_TESTS.lock().await;
        let directory = TempDir::new().unwrap();
        let (mut genesis, keys, address_reservations) = fixture_genesis(5);
        let initial = genesis.validate().unwrap();
        let leader = initial.validators.leader(1, 0).id;
        let corrupt = genesis
            .validators
            .iter()
            .map(|entry| entry.validator.id)
            .find(|id| *id != leader)
            .unwrap();
        genesis
            .validators
            .iter_mut()
            .find(|entry| entry.validator.id == corrupt)
            .unwrap()
            .validator
            .stake = 10;
        genesis
            .validators
            .iter_mut()
            .find(|entry| entry.validator.id != leader && entry.validator.id != corrupt)
            .unwrap()
            .validator
            .stake = 30;
        let validated = genesis.validate().unwrap();
        assert_eq!(validated.validators.validator(leader).unwrap().stake, 20);
        assert_eq!(validated.validators.validator(corrupt).unwrap().stake, 10);
        drop(address_reservations);

        let mut tasks = Vec::new();
        for (index, entry) in genesis.validators.iter().enumerate() {
            let status = Arc::new(RwLock::new(NodeStatus {
                chain_id: genesis.chain_id.clone(),
                genesis_hash: validated.genesis_hash,
                finalized_height: 0,
                committed_height: 0,
                finalized_block: validated.genesis_hash,
                state_root: validated.state_root,
                peer_count: 0,
                ready: false,
                finality_latency_ms: None,
                view_changes: 0,
            }));
            let coordinator = ConsensusCoordinator::bind(
                &genesis,
                entry.validator.id,
                keys[index].clone(),
                directory.path().join(format!("fault-{index}")),
                status,
                CoordinatorConfig {
                    stop_after_height: Some(1),
                    ..CoordinatorConfig::default()
                },
                CoordinatorFaults {
                    corrupt_votes: entry.validator.id == corrupt,
                    outbound_delay: Duration::from_millis(5),
                    drop_basis_points: 5,
                    proposal_delay: if entry.validator.id == leader {
                        Duration::from_millis(500)
                    } else {
                        Duration::ZERO
                    },
                    ..CoordinatorFaults::default()
                },
            )
            .await
            .unwrap();
            let id = entry.validator.id;
            let genesis_time = genesis.genesis_unix_ms;
            tasks.push((
                id,
                tokio::spawn(async move { coordinator.run(genesis_time).await }),
            ));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let wait = u64::try_from(u128::from(genesis.genesis_unix_ms).saturating_sub(now))
            .unwrap_or_default();
        tokio::time::sleep(Duration::from_millis(wait.saturating_add(100))).await;
        tasks
            .iter()
            .find(|(id, _)| *id == leader)
            .unwrap()
            .1
            .abort();

        let outcomes = tokio::time::timeout(REAL_TCP_CONSENSUS_BOUND, async {
            let mut outcomes = Vec::new();
            for (id, task) in tasks {
                if id != leader {
                    outcomes.push(task.await.unwrap().unwrap());
                }
            }
            outcomes
        })
        .await
        .unwrap();
        assert_eq!(outcomes.len(), 4);
        assert!(outcomes.iter().all(|outcome| outcome.finalized_height == 1));
        // The killed leader cannot finalize view 0, so recovery must take at
        // least one view change. The exact number is timing-dependent -- a slow
        // or contended machine times a round out again and lands a view or two
        // later -- so asserting equality tested the machine, not the protocol.
        // What must hold is that every survivor recovered and agreed on the
        // same block in the same view, which the next two assertions cover.
        assert!(outcomes.iter().all(|outcome| outcome.view_changes >= 1));
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.view_changes == outcomes[0].view_changes)
        );
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.finalized_block == outcomes[0].finalized_block)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn timeout_quorum_advances_without_the_next_view_leader() {
        let _serialised = REAL_TCP_TESTS.lock().await;
        let directory = TempDir::new().unwrap();
        let (genesis, keys, address_reservations) = fixture_genesis(5);
        let validated = genesis.validate().unwrap();
        let initial_leader = validated.validators.leader(1, 0).id;
        let missing_next_leader = validated.validators.leader(1, 1).id;
        assert_ne!(initial_leader, missing_next_leader);
        drop(address_reservations);

        let mut tasks = Vec::new();
        for (index, entry) in genesis.validators.iter().enumerate() {
            if entry.validator.id == missing_next_leader {
                continue;
            }
            let status = Arc::new(RwLock::new(NodeStatus {
                chain_id: genesis.chain_id.clone(),
                genesis_hash: validated.genesis_hash,
                finalized_height: 0,
                committed_height: 0,
                finalized_block: validated.genesis_hash,
                state_root: validated.state_root,
                peer_count: 0,
                ready: false,
                finality_latency_ms: None,
                view_changes: 0,
            }));
            let coordinator = ConsensusCoordinator::bind(
                &genesis,
                entry.validator.id,
                keys[index].clone(),
                directory
                    .path()
                    .join(format!("missing-next-leader-{index}")),
                status,
                CoordinatorConfig {
                    stop_after_height: Some(1),
                    ..CoordinatorConfig::default()
                },
                CoordinatorFaults {
                    // Force view 0 to time out. View 1's scheduled leader is
                    // absent, so the live 80% must aggregate both timeout
                    // certificates without depending on that validator.
                    proposal_delay: if entry.validator.id == initial_leader {
                        Duration::from_millis(500)
                    } else {
                        Duration::ZERO
                    },
                    ..CoordinatorFaults::default()
                },
            )
            .await
            .unwrap();
            let genesis_time = genesis.genesis_unix_ms;
            tasks.push(tokio::spawn(async move {
                coordinator.run(genesis_time).await.unwrap()
            }));
        }

        let outcomes = tokio::time::timeout(REAL_TCP_CONSENSUS_BOUND, async {
            let mut outcomes = Vec::new();
            for task in tasks {
                outcomes.push(task.await.unwrap());
            }
            outcomes
        })
        .await
        .expect("the live timeout quorum was stranded behind the absent view-1 leader");

        assert_eq!(outcomes.len(), 4);
        assert!(outcomes.iter().all(|outcome| outcome.finalized_height == 1));
        assert!(outcomes.iter().all(|outcome| outcome.view_changes >= 2));
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.finalized_block == outcomes[0].finalized_block)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_late_starting_validator_catches_up_to_the_others() {
        // Heights the late validator must reach. By the time it starts, the
        // other four (a 4-of-5 quorum) have already finalized past this without
        // it, so it can reach the stop height only by catching up on the orders
        // it missed, never by participating in them live.
        const STOP_AFTER: u64 = 6;
        const LATE: usize = 4;
        let _serialised = REAL_TCP_TESTS.lock().await;
        let directory = TempDir::new().unwrap();
        let (genesis, keys, address_reservations) = fixture_genesis(5);
        let validated = genesis.validate().unwrap();
        drop(address_reservations);

        let mut peer_tasks = Vec::new();
        let mut late_task = None;
        // Keep the finalized-order receivers alive: `commit_finalized_order`
        // emits into them, and a dropped receiver would close the sink and
        // fail every node's first finalize.
        let mut receivers = Vec::new();
        for (index, entry) in genesis.validators.iter().enumerate() {
            let status = Arc::new(RwLock::new(NodeStatus {
                chain_id: genesis.chain_id.clone(),
                genesis_hash: validated.genesis_hash,
                finalized_height: 0,
                committed_height: 0,
                finalized_block: validated.genesis_hash,
                state_root: validated.state_root,
                peer_count: 0,
                ready: false,
                finality_latency_ms: None,
                view_changes: 0,
            }));
            let (finalized_order_sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            receivers.push(receiver);
            let is_late = index == LATE;
            // The four peers run until aborted, not to a stop height. Catch-up is
            // only triggered by seeing a certificate fresher than one's own
            // height, so a peer set that finished first would leave the late
            // validator with no live quorum to trigger on or fetch from -- the
            // exact timing race that made an earlier version of this test flaky.
            // Peers that never stop keep a quorum producing on any hardware.
            let stop_after_height = is_late.then_some(STOP_AFTER);
            let coordinator = ConsensusCoordinator::bind_with_pipeline(
                &genesis,
                entry.validator.id,
                keys[index].clone(),
                directory.path().join(index.to_string()),
                status,
                CoordinatorConfig {
                    stop_after_height,
                    ..CoordinatorConfig::default()
                },
                CoordinatorFaults::default(),
                Arc::new(PipelineSource),
                finalized_order_sender,
            )
            .await
            .unwrap();
            let genesis_time = genesis.genesis_unix_ms;
            if is_late {
                late_task = Some(tokio::spawn(async move {
                    // Start well after genesis so the quorum finalizes several
                    // heights first.
                    tokio::time::sleep(Duration::from_millis(1_200)).await;
                    coordinator.run(genesis_time).await.unwrap()
                }));
            } else {
                peer_tasks.push(tokio::spawn(async move {
                    coordinator.run(genesis_time).await.unwrap()
                }));
            }
        }

        let late_outcome = tokio::time::timeout(REAL_TCP_CONSENSUS_BOUND, late_task.unwrap())
            .await
            .expect(
                "the late validator never caught up, so its run() never reached the stop height",
            )
            .unwrap();

        for task in peer_tasks {
            task.abort();
        }

        // The late validator was absent for the earlier heights, so reaching the
        // stop height at all proves it caught up. `>=` rather than `==` because a
        // single catch-up batch can carry it several heights past the quorum's
        // position in one step.
        assert!(
            late_outcome.finalized_height >= STOP_AFTER,
            "late validator finalized only up to {}, below the stop height {STOP_AFTER}",
            late_outcome.finalized_height,
        );
        drop(receivers);
    }

    fn fixture_genesis(count: u8) -> (GenesisDocument, Vec<Vec<u8>>, Vec<StdTcpListener>) {
        let scheme = Bls12381Scheme;
        let mut keys = Vec::new();
        let mut address_reservations = Vec::new();
        let validators = (1..=count)
            .map(|index| {
                let key = vec![index; 32];
                let public_key = scheme.public_key(&key).unwrap();
                keys.push(key.clone());
                let gossip_identity =
                    libp2p::identity::Keypair::ed25519_from_bytes([index; 32]).unwrap();
                GenesisValidator {
                    name: format!("validator-{index}"),
                    validator: Validator {
                        id: Hash::digest([index]),
                        stake: 20,
                        public_key,
                        proof_of_possession: scheme.proof_of_possession(&key).unwrap(),
                    },
                    network_address: reserve_address(&mut address_reservations).to_string(),
                    rpc_address: reserve_address(&mut address_reservations).to_string(),
                    gossip_peer_id: gossip_identity.public().to_peer_id().to_string(),
                    gossip_address: format!(
                        "/ip4/127.0.0.1/tcp/{}",
                        reserve_address(&mut address_reservations).port()
                    ),
                }
            })
            .collect();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        (
            GenesisDocument {
                format_version: GENESIS_FORMAT_VERSION,
                chain_id: "kestrel-stage-2-test".to_owned(),
                genesis_unix_ms: u64::try_from(now).unwrap() + 250,
                blocks_per_epoch: 100,
                state_config: state::StateConfig::default(),
                active_signature_schemes: vec![1, 2],
                equivocation_slash_basis_points: 5_000,
                validators,
                initial_objects: Vec::new(),
                initial_fee_balances: BTreeMap::new(),
            },
            keys,
            address_reservations,
        )
    }

    fn reserve_address(reservations: &mut Vec<StdTcpListener>) -> std::net::SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        reservations.push(listener);
        address
    }
}
