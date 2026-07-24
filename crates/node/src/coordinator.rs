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
    verify_certificate,
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
use tracing::debug;
use types::Hash;

use crate::{GenesisDocument, GenesisError};

/// Reclassifies an honest replica's refusal to cast both first-round votes in
/// one view as "skip this vote", not a local fault.
///
/// A replica that has already cast this view's timeout vote must not also cast
/// an order vote for it — that mutual exclusion is the TD-022 safety rule and
/// is working as intended. But under a slow round it is an ordinary race: the
/// round timed out, and only then did a delayed proposal arrive. Propagating
/// the refusal killed the whole coordinator task, so a validator that merely
/// ran slowly took itself off the network instead of waiting for the timeout
/// certificate to advance the view. Every other consensus failure stays fatal.
fn skip_when_view_already_timed_out(
    result: Result<Vote, ConsensusError>,
) -> Result<Option<Vote>, ConsensusError> {
    match result {
        Ok(vote) => Ok(Some(vote)),
        Err(ConsensusError::ConflictingFirstRoundVote) => Ok(None),
        Err(error) => Err(error),
    }
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
    pub corrupt_votes: bool,
    pub equivocate_when_leader: bool,
    pub blocked_peers: BTreeSet<Hash>,
    pub outbound_delay: Duration,
    pub drop_basis_points: u16,
    pub proposal_delay: Duration,
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
        if self.id == leader && !round.proposal_delay_elapsed(self.faults.proposal_delay) {
            return Ok(None);
        }
        if self.id == leader
            && self.faults.equivocate_when_leader
            && !round.has(RoundFlag::EquivocationSent)
        {
            self.broadcast_equivocation(height, view).await?;
            round.set(RoundFlag::EquivocationSent);
        } else if self.id == leader
            && !self.faults.equivocate_when_leader
            && (round.proposal.is_none()
                || round.last_proposal_broadcast.elapsed() >= self.config.proposal_rebroadcast)
        {
            if round.proposal.is_none() {
                if round.should_defer_empty_proposal(
                    EMPTY_MEMPOOL_PROPAGATION_MARGIN,
                    self.proposal_source.is_empty(),
                ) {
                    return Ok(None);
                }
                let Some((transaction_ids, fee_commitment)) = self
                    .proposal_source
                    .transaction_ids(height, self.replica.parent_id())
                else {
                    return Ok(None);
                };
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
                if let Some(vote) =
                    skip_when_view_already_timed_out(self.replica.vote_for_proposal(&proposal))?
                {
                    self.persist()?;
                    round.order_votes.insert(vote.validator, vote);
                    round.proposal = Some(signed);
                } else {
                    debug!(
                        height,
                        view, "already timed out this view; not proposing into it"
                    );
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
            && !round.has(RoundFlag::ProposalVoteSent)
            && !round.has(RoundFlag::EquivocationDetected)
            && round
                .first_proposal_at
                .is_some_and(|received| received.elapsed() >= self.config.proposal_vote_delay)
            && let Some(signed) = round.observed_proposals.values().next()
        {
            let proposer = signed.proposal.proposer;
            let voted =
                skip_when_view_already_timed_out(self.replica.vote_for_proposal(&signed.proposal))?;
            // Marked either way: a view this replica has already timed out will
            // never produce an order vote, so retrying on every tick would only
            // spin and re-log until the timeout certificate advances the view.
            round.set(RoundFlag::ProposalVoteSent);
            if let Some(mut vote) = voted {
                self.persist()?;
                if self.faults.corrupt_votes && !vote.signature.is_empty() {
                    vote.signature[0] ^= 1;
                }
                self.send(proposer, WireMessage::Vote(vote)).await;
            } else {
                debug!(
                    height,
                    view, "already timed out this view; skipping its order vote"
                );
            }
        }

        if self.id == leader
            && let Some(proposal) = &round.proposal
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

        if round.started.elapsed() >= self.config.round_timeout
            && !round.has(RoundFlag::TimeoutSent)
        {
            round.set(RoundFlag::TimeoutSent);
            if let Some(vote) = self.replica.local_timeout()? {
                self.persist()?;
                let next_leader = self.validators.leader(height, view.saturating_add(1)).id;
                if self.id == next_leader {
                    round.timeout_votes.insert(vote.validator, vote);
                } else {
                    self.send(next_leader, WireMessage::Vote(vote)).await;
                }
            }
        }
        let next_leader = self.validators.leader(height, view.saturating_add(1)).id;
        if self.id == next_leader
            && let Some(certificate) = make_certificate(
                &self.validators,
                Arc::clone(&self.scheme),
                CertificateKind::Timeout,
                height,
                view,
                Hash::default(),
                &round.timeout_votes,
            )
        {
            self.broadcast(WireMessage::Certificate {
                certificate: certificate.clone(),
                transaction_ids: None,
                fee_commitment: None,
            })
            .await;
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
                match vote.phase {
                    VotePhase::Order if self.id == self.replica.leader() => {
                        round.order_votes.insert(vote.validator, vote);
                    }
                    VotePhase::Commit if self.id == self.replica.leader() => {
                        round.commit_votes.insert(vote.validator, vote);
                    }
                    VotePhase::Timeout
                        if self.id
                            == self
                                .validators
                                .leader(
                                    self.replica.height(),
                                    self.replica.view().saturating_add(1),
                                )
                                .id =>
                    {
                        round.timeout_votes.insert(vote.validator, vote);
                    }
                    _ => {}
                }
            }
            WireMessage::Certificate {
                certificate,
                transaction_ids,
                fee_commitment,
            } => {
                let expected_sender = if certificate.kind == CertificateKind::Timeout {
                    self.validators
                        .leader(certificate.height, certificate.view.saturating_add(1))
                        .id
                } else {
                    self.validators
                        .leader(certificate.height, certificate.view)
                        .id
                };
                if envelope.sender != expected_sender {
                    return Ok(None);
                }
                verify_certificate(&certificate, &self.validators, self.scheme.as_ref())?;
                if certificate.height != self.replica.height() {
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
                            self.replica.advance_view(&certificate)?;
                            self.persist()?;
                            *round = RoundState::new();
                        }
                    }
                }
            }
        }
        Ok(None)
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
        let mut vote = self.replica.vote_to_commit(certificate)?;
        self.persist()?;
        if self.faults.corrupt_votes && !vote.signature.is_empty() {
            vote.signature[0] ^= 1;
        }
        let leader = self.replica.leader();
        if leader == self.id {
            round.commit_votes.insert(vote.validator, vote);
        } else {
            self.send(leader, WireMessage::Vote(vote)).await;
        }
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
        let finalized_height = certificate.height;
        let finalized_block = self.replica.finalize(certificate)?;
        self.persist()?;
        let latency = u64::try_from(round.height_started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let outcome = CoordinatorOutcome {
            finalized_height,
            finalized_block,
            finality_latency_ms: latency,
            view_changes: certificate.view,
        };
        if let Ok(mut status) = self.status.write() {
            status.finalized_height = finalized_height;
            status.finalized_block = finalized_block;
            status.peer_count = self.peers.len().saturating_sub(1);
            status.ready = true;
            status.finality_latency_ms = Some(latency);
            status.view_changes = certificate.view;
        }
        if let Some(sender) = &self.finalized_order_sender {
            sender
                .send(FinalizedOrder {
                    height: finalized_height,
                    block_id: finalized_block,
                    transaction_ids,
                    fee_commitment,
                    certificate: certificate.clone(),
                })
                .map_err(|_| CoordinatorError::FinalizedOrderSinkClosed)?;
        }
        *round = RoundState::new_height();
        Ok(Some(outcome))
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
    fn a_view_already_timed_out_skips_its_order_vote_instead_of_failing() {
        // The replica refusing to cast both first-round votes in one view is
        // the TD-022 safety rule working, and under a slow round it happens
        // routinely: the round times out, then a delayed proposal arrives.
        // Treating it as a local fault killed the coordinator outright, so a
        // merely slow validator removed itself from the network.
        assert!(
            super::skip_when_view_already_timed_out(Err(
                super::ConsensusError::ConflictingFirstRoundVote
            ))
            .expect("a timed-out view must skip its order vote, not fail")
            .is_none()
        );

        // Every other consensus failure must still be fatal.
        assert!(
            super::skip_when_view_already_timed_out(Err(super::ConsensusError::LocalDoubleVote))
                .is_err()
        );
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
        let directory = TempDir::new().unwrap();
        let (genesis, keys) = fixture_genesis(5);
        let validated = genesis.validate().unwrap();
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
        let outcomes = tokio::time::timeout(Duration::from_secs(8), async {
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
        let restarted = tokio::time::timeout(Duration::from_secs(8), async {
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
    #[allow(clippy::too_many_lines)] // Keep the full fault timeline visible for safety review.
    async fn real_tcp_nodes_recover_after_leader_kill_with_a_corrupt_voter() {
        let directory = TempDir::new().unwrap();
        let (mut genesis, keys) = fixture_genesis(5);
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

        let outcomes = tokio::time::timeout(Duration::from_secs(8), async {
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
        assert!(outcomes.iter().all(|outcome| outcome.view_changes == 1));
        assert!(
            outcomes
                .iter()
                .all(|outcome| outcome.finalized_block == outcomes[0].finalized_block)
        );
    }

    fn fixture_genesis(count: u8) -> (GenesisDocument, Vec<Vec<u8>>) {
        let scheme = Bls12381Scheme;
        let mut keys = Vec::new();
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
                    network_address: reserve_address().to_string(),
                    rpc_address: reserve_address().to_string(),
                    gossip_peer_id: gossip_identity.public().to_peer_id().to_string(),
                    gossip_address: format!("/ip4/127.0.0.1/tcp/{}", reserve_address().port()),
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
        )
    }

    fn reserve_address() -> std::net::SocketAddr {
        let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    }
}
