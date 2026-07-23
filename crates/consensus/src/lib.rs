//! Kestrel-BFT ordering, dual-path finality, and local-timeout view changes.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
    },
    thread::{self, JoinHandle},
};

use crypto::{AggregateSignatureScheme, CryptoError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use types::Hash;

const FAST_PERCENT: u128 = 80;
const FALLBACK_PERCENT: u128 = 60;
const PERCENT_DENOMINATOR: u128 = 100;
const VOTE_DOMAIN: &[u8] = b"kestrel/consensus/vote/v1";
const BLOCK_DOMAIN: &[u8] = b"kestrel/consensus/block/v1";
const PROPOSAL_SIGNATURE_DOMAIN: &[u8] = b"kestrel/consensus/proposal-signature/v1";

/// The protocol safety assumption is strictly less than 20% Byzantine stake.
pub const MAX_BYZANTINE_PERCENT_EXCLUSIVE: u8 = 20;
/// Liveness additionally permits up to 20% separate crash/offline stake.
pub const MAX_OFFLINE_PERCENT_INCLUSIVE: u8 = 20;

/// Validator identity, stake, and proof-of-possession protected BLS key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Validator {
    pub id: Hash,
    pub stake: u64,
    pub public_key: Vec<u8>,
    pub proof_of_possession: Vec<u8>,
}

/// Immutable stake table used for one epoch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidatorSet {
    validators: Vec<Validator>,
    positions: BTreeMap<Hash, usize>,
    total_stake: u128,
}

impl ValidatorSet {
    /// Validates unique validators, nonzero stake, and every proof of possession.
    ///
    /// # Errors
    ///
    /// Returns an error when the set cannot safely authenticate aggregate votes.
    pub fn new(
        mut validators: Vec<Validator>,
        scheme: &dyn AggregateSignatureScheme,
    ) -> Result<Self, ConsensusError> {
        if validators.len() < 4 {
            return Err(ConsensusError::TooFewValidators);
        }
        validators.sort_unstable_by_key(|validator| validator.id);
        let mut positions = BTreeMap::new();
        let mut total_stake = 0_u128;
        for (position, validator) in validators.iter().enumerate() {
            if validator.stake == 0 || positions.insert(validator.id, position).is_some() {
                return Err(ConsensusError::InvalidValidatorSet);
            }
            scheme.verify_proof_of_possession(
                &validator.public_key,
                &validator.proof_of_possession,
            )?;
            total_stake = total_stake
                .checked_add(u128::from(validator.stake))
                .ok_or(ConsensusError::StakeOverflow)?;
        }
        Ok(Self {
            validators,
            positions,
            total_stake,
        })
    }

    #[must_use]
    pub const fn total_stake(&self) -> u128 {
        self.total_stake
    }

    #[must_use]
    pub fn validators(&self) -> &[Validator] {
        &self.validators
    }

    #[must_use]
    pub fn validator(&self, id: Hash) -> Option<&Validator> {
        self.positions
            .get(&id)
            .map(|position| &self.validators[*position])
    }

    /// Deterministic stake-table order rotation. A view change always changes leader.
    #[must_use]
    pub fn leader(&self, height: u64, view: u64) -> &Validator {
        let count = self.validators.len() as u64;
        let position = height.wrapping_add(view) % count;
        &self.validators[usize::try_from(position).unwrap_or_default()]
    }

    #[must_use]
    pub fn fast_threshold(&self) -> u128 {
        threshold(self.total_stake, FAST_PERCENT)
    }

    #[must_use]
    pub fn fallback_threshold(&self) -> u128 {
        threshold(self.total_stake, FALLBACK_PERCENT)
    }

    /// Checks whether an injected-fault scenario is within the 20+20 model.
    ///
    /// Byzantine stake must be strictly below 20%. Offline stake is separate,
    /// disjoint, and may be exactly 20%.
    ///
    /// # Errors
    ///
    /// Rejects scenarios outside the stated proof assumptions.
    pub fn validate_fault_budget(
        &self,
        byzantine_stake: u128,
        offline_stake: u128,
    ) -> Result<(), ConsensusError> {
        if byzantine_stake.saturating_mul(5) >= self.total_stake {
            return Err(ConsensusError::ByzantineBudgetExceeded);
        }
        if offline_stake.saturating_mul(5) > self.total_stake
            || byzantine_stake.saturating_add(offline_stake) > self.total_stake
        {
            return Err(ConsensusError::OfflineBudgetExceeded);
        }
        Ok(())
    }
}

/// Proposal whose transaction hashes define only canonical order, not effects.
///
/// `fee_commitment` binds the leader's chosen per-transaction local base fee
/// (see [`fee_commitment`]) into `block_id` itself, the same way
/// `transaction_ids` does for ordering. Without this, a leader could send
/// different honest validators different fee choices for an otherwise
/// identical, equally-certifiable block, producing a settlement amount that
/// silently diverges across honest nodes despite everyone agreeing on
/// `block_id`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Proposal {
    pub height: u64,
    pub view: u64,
    pub parent_id: Hash,
    pub proposer: Hash,
    pub transaction_ids: Vec<Hash>,
    pub fee_commitment: Hash,
    pub justify: Option<Box<QuorumCertificate>>,
    pub block_id: Hash,
}

impl Proposal {
    #[must_use]
    pub fn new(
        height: u64,
        view: u64,
        parent_id: Hash,
        proposer: Hash,
        transaction_ids: Vec<Hash>,
        fee_commitment: Hash,
        justify: Option<QuorumCertificate>,
    ) -> Self {
        let block_id = proposal_id(height, parent_id, &transaction_ids, fee_commitment);
        Self {
            height,
            view,
            parent_id,
            proposer,
            transaction_ids,
            fee_commitment,
            justify: justify.map(Box::new),
            block_id,
        }
    }

    #[must_use]
    pub fn has_valid_id(&self) -> bool {
        self.block_id
            == proposal_id(
                self.height,
                self.parent_id,
                &self.transaction_ids,
                self.fee_commitment,
            )
    }
}

/// Authenticated leader proposal suitable for an untrusted network transport.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SignedProposal {
    pub proposal: Proposal,
    pub signature: Vec<u8>,
}

impl SignedProposal {
    /// Signs a complete proposal, including its height, view, parent and order.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed private-key material.
    pub fn sign(
        proposal: Proposal,
        private_key: &[u8],
        scheme: &dyn AggregateSignatureScheme,
    ) -> Result<Self, ConsensusError> {
        let signature = scheme.sign(private_key, &proposal_signature_message(&proposal))?;
        Ok(Self {
            proposal,
            signature,
        })
    }

    /// Verifies block identity, deterministic leader selection, and signature.
    ///
    /// # Errors
    ///
    /// Rejects invalid proposal fields, a wrong leader, or a bad signature.
    pub fn verify(
        &self,
        validators: &ValidatorSet,
        scheme: &dyn AggregateSignatureScheme,
    ) -> Result<(), ConsensusError> {
        if !self.proposal.has_valid_id() {
            return Err(ConsensusError::InvalidProposal);
        }
        if validators
            .leader(self.proposal.height, self.proposal.view)
            .id
            != self.proposal.proposer
        {
            return Err(ConsensusError::WrongLeader);
        }
        let validator = validators
            .validator(self.proposal.proposer)
            .ok_or(ConsensusError::UnknownValidator(self.proposal.proposer))?;
        scheme.verify(
            &validator.public_key,
            &proposal_signature_message(&self.proposal),
            &self.signature,
        )?;
        Ok(())
    }
}

/// The first order vote can become an 80% fast certificate or 60% prepare certificate.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum VotePhase {
    Order,
    Commit,
    Timeout,
}

/// Individually signed vote. These are aggregated locally and are not stored on-chain.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Vote {
    pub validator: Hash,
    pub height: u64,
    pub view: u64,
    pub block_id: Hash,
    pub phase: VotePhase,
    pub signature: Vec<u8>,
}

/// Two conflicting, individually signed votes proving Byzantine equivocation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EquivocationEvidence {
    pub first: Vote,
    pub second: Vote,
}

impl EquivocationEvidence {
    /// Returns an order-independent identifier suitable for replay protection.
    #[must_use]
    pub fn id(&self) -> Hash {
        let (first, second) = if self.first.block_id <= self.second.block_id {
            (&self.first, &self.second)
        } else {
            (&self.second, &self.first)
        };
        let mut bytes = Vec::with_capacity(160 + first.signature.len() + second.signature.len());
        bytes.extend_from_slice(b"kestrel/consensus/equivocation/v1");
        bytes.extend_from_slice(first.validator.as_bytes());
        bytes.extend_from_slice(&first.height.to_be_bytes());
        bytes.extend_from_slice(&first.view.to_be_bytes());
        bytes.push(vote_phase_tag(first.phase));
        bytes.extend_from_slice(first.block_id.as_bytes());
        bytes.extend_from_slice(&(first.signature.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&first.signature);
        bytes.extend_from_slice(second.block_id.as_bytes());
        bytes.extend_from_slice(&(second.signature.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&second.signature);
        Hash::digest(bytes)
    }
}

/// Epoch-bound penalty emitted after cryptographic evidence verification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SlashingDirective {
    pub evidence_id: Hash,
    pub validator: Hash,
    pub slash_stake: u64,
    pub offence: SlashingOffence,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SlashingOffence {
    DoubleVote,
}

/// Genesis-configured penalty policy. It never treats downtime as slashable.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SlashingPolicy {
    equivocation_basis_points: u16,
}

impl SlashingPolicy {
    /// Creates an equivocation penalty between 1 and 10,000 basis points.
    ///
    /// # Errors
    ///
    /// Rejects zero or values above 100%.
    pub fn new(equivocation_basis_points: u16) -> Result<Self, ConsensusError> {
        if !(1..=10_000).contains(&equivocation_basis_points) {
            return Err(ConsensusError::InvalidSlashBasisPoints);
        }
        Ok(Self {
            equivocation_basis_points,
        })
    }

    /// Verifies evidence and computes the next-epoch stake penalty.
    ///
    /// # Errors
    ///
    /// Rejects malformed evidence, unknown validators, or bad signatures.
    pub fn adjudicate(
        self,
        evidence: &EquivocationEvidence,
        validators: &ValidatorSet,
        scheme: &dyn AggregateSignatureScheme,
    ) -> Result<SlashingDirective, ConsensusError> {
        let validator = verify_equivocation_evidence(evidence, validators, scheme)?;
        let slash_stake = u128::from(validator.stake)
            .saturating_mul(u128::from(self.equivocation_basis_points))
            .div_ceil(10_000)
            .try_into()
            .map_err(|_| ConsensusError::StakeOverflow)?;
        Ok(SlashingDirective {
            evidence_id: evidence.id(),
            validator: validator.id,
            slash_stake,
            offence: SlashingOffence::DoubleVote,
        })
    }
}

/// Replay-protected evidence intake. Applying stake changes remains an epoch transition.
#[derive(Debug, Default)]
pub struct EvidenceBook {
    applied: std::collections::BTreeSet<Hash>,
}

impl EvidenceBook {
    /// Verifies and records evidence exactly once.
    ///
    /// # Errors
    ///
    /// Rejects invalid or already-recorded evidence.
    pub fn record(
        &mut self,
        evidence: &EquivocationEvidence,
        validators: &ValidatorSet,
        policy: SlashingPolicy,
        scheme: &dyn AggregateSignatureScheme,
    ) -> Result<SlashingDirective, ConsensusError> {
        let directive = policy.adjudicate(evidence, validators, scheme)?;
        if !self.applied.insert(directive.evidence_id) {
            return Err(ConsensusError::EvidenceAlreadyApplied);
        }
        Ok(directive)
    }
}

/// Cryptographically verifies same-height/view/phase double-vote evidence.
///
/// # Errors
///
/// Rejects evidence that is not conflicting, uses different signers/rounds, or
/// contains an invalid signature.
pub fn verify_equivocation_evidence<'a>(
    evidence: &EquivocationEvidence,
    validators: &'a ValidatorSet,
    scheme: &dyn AggregateSignatureScheme,
) -> Result<&'a Validator, ConsensusError> {
    let first = &evidence.first;
    let second = &evidence.second;
    if first.validator != second.validator
        || first.height != second.height
        || first.view != second.view
        || first.phase != second.phase
        || first.block_id == second.block_id
    {
        return Err(ConsensusError::InvalidEquivocationEvidence);
    }
    let validator = validators
        .validator(first.validator)
        .ok_or(ConsensusError::UnknownValidator(first.validator))?;
    for vote in [first, second] {
        scheme.verify(
            &validator.public_key,
            &vote_message(vote.height, vote.view, vote.block_id, vote.phase),
            &vote.signature,
        )?;
    }
    Ok(validator)
}

impl Vote {
    /// Signs one domain-separated canonical vote.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed key material.
    pub fn sign(
        validator: Hash,
        private_key: &[u8],
        height: u64,
        view: u64,
        block_id: Hash,
        phase: VotePhase,
        scheme: &dyn AggregateSignatureScheme,
    ) -> Result<Self, ConsensusError> {
        let signature = scheme.sign(private_key, &vote_message(height, view, block_id, phase))?;
        Ok(Self {
            validator,
            height,
            view,
            block_id,
            phase,
            signature,
        })
    }
}

/// Meaning assigned to an aggregate certificate.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum CertificateKind {
    Fast,
    Prepare,
    Commit,
    Timeout,
}

impl CertificateKind {
    const fn vote_phase(self) -> VotePhase {
        match self {
            Self::Fast | Self::Prepare => VotePhase::Order,
            Self::Commit => VotePhase::Commit,
            Self::Timeout => VotePhase::Timeout,
        }
    }

    const fn required_percent(self) -> u128 {
        match self {
            Self::Fast => FAST_PERCENT,
            Self::Prepare | Self::Commit | Self::Timeout => FALLBACK_PERCENT,
        }
    }
}

/// Compact aggregate certificate suitable for gossip and durable storage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct QuorumCertificate {
    pub kind: CertificateKind,
    pub height: u64,
    pub view: u64,
    pub block_id: Hash,
    pub signers: Vec<Hash>,
    pub signed_stake: u128,
    pub aggregate_signature: Vec<u8>,
}

/// Local collector. Individual votes remain off-chain and disappear after aggregation.
pub struct VoteCollector<'a> {
    validators: &'a ValidatorSet,
    scheme: Arc<dyn AggregateSignatureScheme>,
    kind: CertificateKind,
    height: u64,
    view: u64,
    block_id: Hash,
    votes: BTreeMap<Hash, Vec<u8>>,
    signed_stake: u128,
}

enum AggregationCommand {
    Vote(Vote),
    Shutdown,
}

/// Background verifier/aggregator keeping BLS combination off the consensus task.
pub struct AsyncVoteAggregator {
    commands: SyncSender<AggregationCommand>,
    certificates: Receiver<Result<Option<QuorumCertificate>, ConsensusError>>,
    worker: Option<JoinHandle<()>>,
}

impl AsyncVoteAggregator {
    /// Starts one collector worker with a bounded individual-vote queue.
    ///
    /// # Errors
    ///
    /// Returns an error if the operating system cannot start the worker.
    pub fn new(
        validators: ValidatorSet,
        scheme: Arc<dyn AggregateSignatureScheme>,
        kind: CertificateKind,
        height: u64,
        view: u64,
        block_id: Hash,
    ) -> Result<Self, ConsensusError> {
        let (command_sender, command_receiver) = mpsc::sync_channel(1_024);
        let (certificate_sender, certificate_receiver) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("kestrel-vote-aggregation".to_owned())
            .spawn(move || {
                let mut collector =
                    VoteCollector::new(&validators, scheme, kind, height, view, block_id);
                while let Ok(command) = command_receiver.recv() {
                    match command {
                        AggregationCommand::Vote(vote) => {
                            if certificate_sender.send(collector.add_vote(vote)).is_err() {
                                break;
                            }
                        }
                        AggregationCommand::Shutdown => break,
                    }
                }
            })
            .map_err(|error| ConsensusError::AggregationWorker(error.to_string()))?;
        Ok(Self {
            commands: command_sender,
            certificates: certificate_receiver,
            worker: Some(worker),
        })
    }

    /// Queues a vote without performing pairing work on the caller's task.
    ///
    /// # Errors
    ///
    /// Reports bounded-queue backpressure or a stopped worker.
    pub fn submit(&self, vote: Vote) -> Result<(), ConsensusError> {
        match self.commands.try_send(AggregationCommand::Vote(vote)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(ConsensusError::AggregationQueueFull),
            Err(TrySendError::Disconnected(_)) => Err(ConsensusError::AggregationWorkerStopped),
        }
    }

    /// Polls the next verification/aggregation result without blocking consensus.
    ///
    /// # Errors
    ///
    /// Returns vote validation errors or a stopped worker.
    pub fn try_certificate(&self) -> Result<Option<QuorumCertificate>, ConsensusError> {
        match self.certificates.try_recv() {
            Ok(result) => result,
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => Err(ConsensusError::AggregationWorkerStopped),
        }
    }
}

impl Drop for AsyncVoteAggregator {
    fn drop(&mut self) {
        let _ = self.commands.send(AggregationCommand::Shutdown);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl<'a> VoteCollector<'a> {
    #[must_use]
    pub fn new(
        validators: &'a ValidatorSet,
        scheme: Arc<dyn AggregateSignatureScheme>,
        kind: CertificateKind,
        height: u64,
        view: u64,
        block_id: Hash,
    ) -> Self {
        Self {
            validators,
            scheme,
            kind,
            height,
            view,
            block_id,
            votes: BTreeMap::new(),
            signed_stake: 0,
        }
    }

    /// Validates and inserts a vote. Returns a certificate once its exact threshold is met.
    ///
    /// # Errors
    ///
    /// Rejects unknown signers, wrong targets, malformed signatures, and equivocation
    /// observed by this collector.
    pub fn add_vote(&mut self, vote: Vote) -> Result<Option<QuorumCertificate>, ConsensusError> {
        if vote.height != self.height
            || vote.view != self.view
            || vote.block_id != self.block_id
            || vote.phase != self.kind.vote_phase()
        {
            return Err(ConsensusError::VoteTargetMismatch);
        }
        let validator = self
            .validators
            .validator(vote.validator)
            .ok_or(ConsensusError::UnknownValidator(vote.validator))?;
        self.scheme.verify(
            &validator.public_key,
            &vote_message(vote.height, vote.view, vote.block_id, vote.phase),
            &vote.signature,
        )?;
        if let Some(existing) = self.votes.get(&vote.validator) {
            if existing == &vote.signature {
                return self.certificate_if_ready();
            }
            return Err(ConsensusError::Equivocation(vote.validator));
        }
        self.signed_stake = self
            .signed_stake
            .checked_add(u128::from(validator.stake))
            .ok_or(ConsensusError::StakeOverflow)?;
        self.votes.insert(vote.validator, vote.signature);
        self.certificate_if_ready()
    }

    fn certificate_if_ready(&self) -> Result<Option<QuorumCertificate>, ConsensusError> {
        let required = threshold(self.validators.total_stake, self.kind.required_percent());
        if self.signed_stake < required {
            return Ok(None);
        }
        let signatures = self.votes.values().cloned().collect::<Vec<_>>();
        let aggregate_signature = self.scheme.aggregate(&signatures)?;
        Ok(Some(QuorumCertificate {
            kind: self.kind,
            height: self.height,
            view: self.view,
            block_id: self.block_id,
            signers: self.votes.keys().copied().collect(),
            signed_stake: self.signed_stake,
            aggregate_signature,
        }))
    }
}

/// Verifies signer identity, exact stake, threshold, and BLS aggregate.
///
/// # Errors
///
/// Rejects every malformed or under-threshold certificate.
pub fn verify_certificate(
    certificate: &QuorumCertificate,
    validators: &ValidatorSet,
    scheme: &dyn AggregateSignatureScheme,
) -> Result<(), ConsensusError> {
    if certificate.signers.is_empty()
        || certificate
            .signers
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
    {
        return Err(ConsensusError::InvalidSignerSet);
    }
    let mut stake = 0_u128;
    let mut public_keys = Vec::with_capacity(certificate.signers.len());
    for signer in &certificate.signers {
        let validator = validators
            .validator(*signer)
            .ok_or(ConsensusError::UnknownValidator(*signer))?;
        stake = stake
            .checked_add(u128::from(validator.stake))
            .ok_or(ConsensusError::StakeOverflow)?;
        public_keys.push(validator.public_key.clone());
    }
    if stake != certificate.signed_stake {
        return Err(ConsensusError::IncorrectSignedStake);
    }
    let required = threshold(validators.total_stake, certificate.kind.required_percent());
    if stake < required {
        return Err(ConsensusError::InsufficientStake {
            required,
            received: stake,
        });
    }
    scheme.verify_aggregate(
        &public_keys,
        &vote_message(
            certificate.height,
            certificate.view,
            certificate.block_id,
            certificate.kind.vote_phase(),
        ),
        &certificate.aggregate_signature,
    )?;
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaLock {
    pub height: u64,
    pub view: u64,
    pub block_id: Hash,
}

/// Durable consensus safety state. Persist before transmitting a newly signed vote.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReplicaSnapshot {
    pub height: u64,
    pub view: u64,
    pub parent_id: Hash,
    pub lock: Option<ReplicaLock>,
    pub votes: BTreeMap<(u64, u64, VotePhase), Hash>,
    pub finalized: BTreeMap<u64, Hash>,
}

/// Honest replica safety state and deterministic local view timer transitions.
pub struct Replica {
    id: Hash,
    private_key: Vec<u8>,
    validators: ValidatorSet,
    scheme: Arc<dyn AggregateSignatureScheme>,
    height: u64,
    view: u64,
    parent_id: Hash,
    lock: Option<ReplicaLock>,
    votes: BTreeMap<(u64, u64, VotePhase), Hash>,
    finalized: BTreeMap<u64, Hash>,
}

impl Replica {
    /// Creates a replica and ensures its private key matches its admitted validator key.
    ///
    /// # Errors
    ///
    /// Rejects an unknown identity or mismatched private key.
    pub fn new(
        id: Hash,
        private_key: Vec<u8>,
        validators: ValidatorSet,
        scheme: Arc<dyn AggregateSignatureScheme>,
        height: u64,
        parent_id: Hash,
    ) -> Result<Self, ConsensusError> {
        let validator = validators
            .validator(id)
            .ok_or(ConsensusError::UnknownValidator(id))?;
        if scheme.public_key(&private_key)? != validator.public_key {
            return Err(ConsensusError::PrivateKeyMismatch);
        }
        Ok(Self {
            id,
            private_key,
            validators,
            scheme,
            height,
            view: 0,
            parent_id,
            lock: None,
            votes: BTreeMap::new(),
            finalized: BTreeMap::new(),
        })
    }

    /// Restores an authenticated replica from previously durable safety state.
    ///
    /// # Errors
    ///
    /// Rejects unknown identities, mismatched keys, or internally inconsistent state.
    pub fn restore(
        id: Hash,
        private_key: Vec<u8>,
        validators: ValidatorSet,
        scheme: Arc<dyn AggregateSignatureScheme>,
        snapshot: ReplicaSnapshot,
    ) -> Result<Self, ConsensusError> {
        let validator = validators
            .validator(id)
            .ok_or(ConsensusError::UnknownValidator(id))?;
        if scheme.public_key(&private_key)? != validator.public_key {
            return Err(ConsensusError::PrivateKeyMismatch);
        }
        if snapshot.height == 0
            || snapshot
                .lock
                .is_some_and(|lock| lock.height != snapshot.height || lock.view > snapshot.view)
            || snapshot
                .finalized
                .keys()
                .next_back()
                .is_some_and(|height| *height >= snapshot.height)
        {
            return Err(ConsensusError::InvalidReplicaSnapshot);
        }
        Ok(Self {
            id,
            private_key,
            validators,
            scheme,
            height: snapshot.height,
            view: snapshot.view,
            parent_id: snapshot.parent_id,
            lock: snapshot.lock,
            votes: snapshot.votes,
            finalized: snapshot.finalized,
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> ReplicaSnapshot {
        ReplicaSnapshot {
            height: self.height,
            view: self.view,
            parent_id: self.parent_id,
            lock: self.lock,
            votes: self.votes.clone(),
            finalized: self.finalized.clone(),
        }
    }

    #[must_use]
    pub const fn parent_id(&self) -> Hash {
        self.parent_id
    }

    #[must_use]
    pub const fn height(&self) -> u64 {
        self.height
    }

    #[must_use]
    pub const fn view(&self) -> u64 {
        self.view
    }

    #[must_use]
    pub fn leader(&self) -> Hash {
        self.validators.leader(self.height, self.view).id
    }

    /// Validates leader, parent, block ID, and lock rule, then emits one order vote.
    ///
    /// # Errors
    ///
    /// Rejects stale/wrong-leader proposals, unsafe unlocks, or local equivocation.
    pub fn vote_for_proposal(&mut self, proposal: &Proposal) -> Result<Vote, ConsensusError> {
        self.validate_proposal(proposal)?;
        self.sign_once(proposal.block_id, VotePhase::Order)
    }

    fn validate_proposal(&self, proposal: &Proposal) -> Result<(), ConsensusError> {
        if proposal.height != self.height || proposal.view != self.view {
            return Err(ConsensusError::ProposalRoundMismatch);
        }
        if proposal.proposer != self.leader() {
            return Err(ConsensusError::WrongLeader);
        }
        if proposal.parent_id != self.parent_id || !proposal.has_valid_id() {
            return Err(ConsensusError::InvalidProposal);
        }
        if let Some(lock) = self.lock
            && proposal.block_id != lock.block_id
        {
            let justify = proposal
                .justify
                .as_deref()
                .ok_or(ConsensusError::LockedOnDifferentBlock)?;
            verify_certificate(justify, &self.validators, self.scheme.as_ref())?;
            if justify.kind != CertificateKind::Prepare
                || justify.height != lock.height
                || justify.view <= lock.view
                || justify.block_id != proposal.block_id
            {
                return Err(ConsensusError::LockedOnDifferentBlock);
            }
        }
        Ok(())
    }

    /// Locks a prepared block and emits the fallback round-two commit vote.
    ///
    /// # Errors
    ///
    /// Rejects invalid, stale, or conflicting prepare certificates.
    pub fn vote_to_commit(&mut self, prepare: &QuorumCertificate) -> Result<Vote, ConsensusError> {
        verify_certificate(prepare, &self.validators, self.scheme.as_ref())?;
        if prepare.kind != CertificateKind::Prepare
            || prepare.height != self.height
            || prepare.view != self.view
        {
            return Err(ConsensusError::CertificateRoundMismatch);
        }
        if self.lock.is_some_and(|lock| {
            lock.height == prepare.height
                && lock.block_id != prepare.block_id
                && lock.view >= prepare.view
        }) {
            return Err(ConsensusError::LockedOnDifferentBlock);
        }
        self.lock = Some(ReplicaLock {
            height: prepare.height,
            view: prepare.view,
            block_id: prepare.block_id,
        });
        self.sign_once(prepare.block_id, VotePhase::Commit)
    }

    /// Emits a timeout vote from a local timer; no global clock is consulted.
    ///
    /// Order and timeout are the mutually exclusive first-round choices from
    /// the 20+20 protocol. A replica that already notarized a proposal cannot
    /// also help certify that the same view timed out. This is what prevents an
    /// 80% fast certificate from coexisting with the 60% certificate needed to
    /// abandon its view.
    ///
    /// # Errors
    ///
    /// Returns no vote when this replica already cast an order vote in the
    /// current view. Key failures and conflicting durable votes remain errors.
    pub fn local_timeout(&mut self) -> Result<Option<Vote>, ConsensusError> {
        if self
            .votes
            .contains_key(&(self.height, self.view, VotePhase::Order))
        {
            return Ok(None);
        }
        self.sign_once(Hash::default(), VotePhase::Timeout)
            .map(Some)
    }

    /// Advances one view after a 60% timeout certificate.
    ///
    /// # Errors
    ///
    /// Rejects invalid or stale timeout certificates.
    pub fn advance_view(&mut self, timeout: &QuorumCertificate) -> Result<(), ConsensusError> {
        verify_certificate(timeout, &self.validators, self.scheme.as_ref())?;
        if timeout.kind != CertificateKind::Timeout
            || timeout.height != self.height
            || timeout.view != self.view
        {
            return Err(ConsensusError::CertificateRoundMismatch);
        }
        self.view = self
            .view
            .checked_add(1)
            .ok_or(ConsensusError::ViewOverflow)?;
        Ok(())
    }

    /// Finalizes an 80% fast certificate or a 60% second-round commit certificate.
    ///
    /// # Errors
    ///
    /// Any conflicting local finalization is a hard safety violation.
    pub fn finalize(&mut self, certificate: &QuorumCertificate) -> Result<Hash, ConsensusError> {
        verify_certificate(certificate, &self.validators, self.scheme.as_ref())?;
        if let Some(existing) = self.finalized.get(&certificate.height) {
            if *existing != certificate.block_id {
                return Err(ConsensusError::SafetyViolation {
                    height: certificate.height,
                    first: *existing,
                    second: certificate.block_id,
                });
            }
            return Ok(*existing);
        }
        if !matches!(
            certificate.kind,
            CertificateKind::Fast | CertificateKind::Commit
        ) || certificate.height != self.height
        {
            return Err(ConsensusError::CertificateRoundMismatch);
        }
        self.finalized
            .insert(certificate.height, certificate.block_id);
        self.parent_id = certificate.block_id;
        self.height = self
            .height
            .checked_add(1)
            .ok_or(ConsensusError::HeightOverflow)?;
        self.view = 0;
        self.lock = None;
        Ok(certificate.block_id)
    }

    fn sign_once(&mut self, block_id: Hash, phase: VotePhase) -> Result<Vote, ConsensusError> {
        let opposing_first_round = match phase {
            VotePhase::Order => Some(VotePhase::Timeout),
            VotePhase::Timeout => Some(VotePhase::Order),
            VotePhase::Commit => None,
        };
        if opposing_first_round
            .is_some_and(|opposing| self.votes.contains_key(&(self.height, self.view, opposing)))
        {
            return Err(ConsensusError::ConflictingFirstRoundVote);
        }
        let key = (self.height, self.view, phase);
        if let Some(previous) = self.votes.get(&key) {
            if *previous != block_id {
                return Err(ConsensusError::LocalDoubleVote);
            }
        } else {
            self.votes.insert(key, block_id);
        }
        Vote::sign(
            self.id,
            &self.private_key,
            self.height,
            self.view,
            block_id,
            phase,
            self.scheme.as_ref(),
        )
    }
}

/// Finalized consensus output. It commits only to transaction order.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FinalizedOrder {
    pub height: u64,
    pub block_id: Hash,
    pub transaction_ids: Vec<Hash>,
    pub fee_commitment: Hash,
    pub certificate: QuorumCertificate,
}

/// Consensus validation failures.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum ConsensusError {
    #[error(transparent)]
    Crypto(#[from] CryptoError),
    #[error("validator set requires at least four validators")]
    TooFewValidators,
    #[error("validator IDs must be unique and stake must be nonzero")]
    InvalidValidatorSet,
    #[error("validator stake sum overflowed")]
    StakeOverflow,
    #[error("unknown validator {0}")]
    UnknownValidator(Hash),
    #[error("private key does not match admitted validator key")]
    PrivateKeyMismatch,
    #[error("Byzantine stake must be strictly below 20%")]
    ByzantineBudgetExceeded,
    #[error("separate crash/offline stake must not exceed 20%")]
    OfflineBudgetExceeded,
    #[error("vote does not match its collector target")]
    VoteTargetMismatch,
    #[error("validator {0} equivocated within one collector")]
    Equivocation(Hash),
    #[error("equivocation evidence must contain two valid conflicting votes for one round")]
    InvalidEquivocationEvidence,
    #[error("equivocation evidence was already applied")]
    EvidenceAlreadyApplied,
    #[error("equivocation slash basis points must be between 1 and 10,000")]
    InvalidSlashBasisPoints,
    #[error("vote aggregation queue is full")]
    AggregationQueueFull,
    #[error("vote aggregation worker stopped")]
    AggregationWorkerStopped,
    #[error("failed to start vote aggregation worker: {0}")]
    AggregationWorker(String),
    #[error("certificate signer set must be nonempty, sorted, and unique")]
    InvalidSignerSet,
    #[error("certificate signed-stake field is incorrect")]
    IncorrectSignedStake,
    #[error("insufficient signed stake: required {required}, received {received}")]
    InsufficientStake { required: u128, received: u128 },
    #[error("proposal does not match the replica height or view")]
    ProposalRoundMismatch,
    #[error("proposal was not produced by the deterministic leader")]
    WrongLeader,
    #[error("proposal parent or block ID is invalid")]
    InvalidProposal,
    #[error("proposal conflicts with the replica lock without a higher prepare certificate")]
    LockedOnDifferentBlock,
    #[error("certificate has the wrong kind, height, or view")]
    CertificateRoundMismatch,
    #[error("honest replica refused to double vote")]
    LocalDoubleVote,
    #[error("honest replica refused to cast both order and timeout votes in one view")]
    ConflictingFirstRoundVote,
    #[error("durable replica snapshot is internally inconsistent")]
    InvalidReplicaSnapshot,
    #[error("view number overflowed")]
    ViewOverflow,
    #[error("height number overflowed")]
    HeightOverflow,
    #[error("conflicting blocks finalized at height {height}: {first} and {second}")]
    SafetyViolation {
        height: u64,
        first: Hash,
        second: Hash,
    },
}

fn threshold(total_stake: u128, percent: u128) -> u128 {
    total_stake
        .saturating_mul(percent)
        .div_ceil(PERCENT_DENOMINATOR)
}

fn vote_message(height: u64, view: u64, block_id: Hash, phase: VotePhase) -> Vec<u8> {
    let mut message = Vec::with_capacity(VOTE_DOMAIN.len() + 8 + 8 + 32 + 1);
    message.extend_from_slice(VOTE_DOMAIN);
    message.extend_from_slice(&height.to_be_bytes());
    message.extend_from_slice(&view.to_be_bytes());
    message.extend_from_slice(block_id.as_bytes());
    message.push(vote_phase_tag(phase));
    message
}

const fn vote_phase_tag(phase: VotePhase) -> u8 {
    match phase {
        VotePhase::Order => 0,
        VotePhase::Commit => 1,
        VotePhase::Timeout => 2,
    }
}

fn proposal_id(height: u64, parent_id: Hash, transactions: &[Hash], fee_commitment: Hash) -> Hash {
    let mut bytes = Vec::with_capacity(BLOCK_DOMAIN.len() + 80 + transactions.len() * 32);
    bytes.extend_from_slice(BLOCK_DOMAIN);
    bytes.extend_from_slice(&height.to_be_bytes());
    bytes.extend_from_slice(parent_id.as_bytes());
    bytes.extend_from_slice(&(transactions.len() as u64).to_be_bytes());
    for transaction in transactions {
        bytes.extend_from_slice(transaction.as_bytes());
    }
    bytes.extend_from_slice(fee_commitment.as_bytes());
    Hash::digest(bytes)
}

const FEE_COMMITMENT_DOMAIN: &[u8] = b"kestrel/consensus/fee-commitment/v1";

/// Canonical commitment to a leader's chosen per-transaction local base fee,
/// in the same order as the block's `transaction_ids`. Folding this into
/// [`Proposal::block_id`] makes the leader's price choice equivocation-safe:
/// callers that need a settled base fee (see `crates/node`'s `BlockLifecycle`)
/// must recompute this from the actual fees they received and compare it
/// against the certified value rather than trusting an unauthenticated
/// side-channel.
#[must_use]
pub fn fee_commitment(base_fees: &[u128]) -> Hash {
    let mut bytes = Vec::with_capacity(FEE_COMMITMENT_DOMAIN.len() + 8 + base_fees.len() * 16);
    bytes.extend_from_slice(FEE_COMMITMENT_DOMAIN);
    bytes.extend_from_slice(&(base_fees.len() as u64).to_be_bytes());
    for fee in base_fees {
        bytes.extend_from_slice(&fee.to_be_bytes());
    }
    Hash::digest(bytes)
}

fn proposal_signature_message(proposal: &Proposal) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        PROPOSAL_SIGNATURE_DOMAIN.len() + 120 + proposal.transaction_ids.len() * 32,
    );
    bytes.extend_from_slice(PROPOSAL_SIGNATURE_DOMAIN);
    bytes.extend_from_slice(&proposal.height.to_be_bytes());
    bytes.extend_from_slice(&proposal.view.to_be_bytes());
    bytes.extend_from_slice(proposal.parent_id.as_bytes());
    bytes.extend_from_slice(proposal.proposer.as_bytes());
    bytes.extend_from_slice(proposal.block_id.as_bytes());
    bytes.extend_from_slice(&(proposal.transaction_ids.len() as u64).to_be_bytes());
    for transaction in &proposal.transaction_ids {
        bytes.extend_from_slice(transaction.as_bytes());
    }
    bytes
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crypto::{AggregateSignatureScheme, Bls12381Scheme, SignatureScheme};

    use super::{
        ConsensusError, EquivocationEvidence, EvidenceBook, Proposal, Replica, ReplicaSnapshot,
        SignedProposal, SlashingPolicy, Validator, ValidatorSet, Vote, VotePhase,
        verify_equivocation_evidence,
    };
    use types::Hash;

    fn scheme() -> Arc<dyn AggregateSignatureScheme> {
        Arc::new(Bls12381Scheme)
    }

    #[test]
    fn thresholds_and_fault_model_are_exact() {
        let validators = validator_set(&[19, 21, 20, 20, 20]);
        assert_eq!(validators.fast_threshold(), 80);
        assert_eq!(validators.fallback_threshold(), 60);
        assert_eq!(validators.validate_fault_budget(19, 20), Ok(()));
        assert!(validators.validate_fault_budget(20, 0).is_err());
        assert!(validators.validate_fault_budget(0, 21).is_err());
    }

    #[test]
    fn only_valid_double_votes_create_replay_protected_slashing_directives() {
        let validators = validator_set(&[25, 25, 25, 25]);
        let validator = Hash::digest(0_usize.to_be_bytes());
        let private_key = [1_u8; 32];
        let first = Vote::sign(
            validator,
            &private_key,
            9,
            3,
            Hash::digest(b"first"),
            VotePhase::Order,
            scheme().as_ref(),
        )
        .unwrap();
        let second = Vote::sign(
            validator,
            &private_key,
            9,
            3,
            Hash::digest(b"second"),
            VotePhase::Order,
            scheme().as_ref(),
        )
        .unwrap();
        let evidence = EquivocationEvidence { first, second };
        verify_equivocation_evidence(&evidence, &validators, scheme().as_ref()).unwrap();

        let mut book = EvidenceBook::default();
        let directive = book
            .record(
                &evidence,
                &validators,
                SlashingPolicy::new(5_000).unwrap(),
                scheme().as_ref(),
            )
            .unwrap();
        assert_eq!(directive.validator, validator);
        assert_eq!(directive.slash_stake, 13);
        assert_eq!(
            book.record(
                &evidence,
                &validators,
                SlashingPolicy::new(5_000).unwrap(),
                scheme().as_ref(),
            ),
            Err(ConsensusError::EvidenceAlreadyApplied)
        );

        let mut non_conflicting = evidence;
        non_conflicting.second.block_id = non_conflicting.first.block_id;
        assert_eq!(
            verify_equivocation_evidence(&non_conflicting, &validators, scheme().as_ref()),
            Err(ConsensusError::InvalidEquivocationEvidence)
        );

        non_conflicting.second.block_id = Hash::digest(b"third");
        non_conflicting.second.signature[0] ^= 1;
        assert!(matches!(
            verify_equivocation_evidence(&non_conflicting, &validators, scheme().as_ref()),
            Err(ConsensusError::Crypto(_))
        ));
    }

    #[test]
    fn signed_proposals_and_replica_snapshots_survive_transport_restart() {
        let validators = validator_set(&[25, 25, 25, 25]);
        let leader = validators.leader(1, 0).id;
        let private_key = key_for(leader);
        let proposal = Proposal::new(
            1,
            0,
            Hash::digest(b"genesis"),
            leader,
            vec![Hash::digest(b"transaction")],
            Hash::default(),
            None,
        );
        let signed =
            SignedProposal::sign(proposal.clone(), &private_key, scheme().as_ref()).unwrap();
        signed.verify(&validators, scheme().as_ref()).unwrap();

        let mut replica = Replica::new(
            leader,
            private_key.clone(),
            validators.clone(),
            scheme(),
            1,
            proposal.parent_id,
        )
        .unwrap();
        let vote = replica.vote_for_proposal(&proposal).unwrap();
        let bytes = bcs::to_bytes(&replica.snapshot()).unwrap();
        let snapshot: ReplicaSnapshot = bcs::from_bytes(&bytes).unwrap();
        let mut restored =
            Replica::restore(leader, private_key, validators, scheme(), snapshot).unwrap();
        assert_eq!(restored.vote_for_proposal(&proposal).unwrap(), vote);
    }

    fn validator_set(stakes: &[u64]) -> ValidatorSet {
        let bls = Bls12381Scheme;
        let validators = stakes
            .iter()
            .enumerate()
            .map(|(index, stake)| {
                let key = [u8::try_from(index + 1).unwrap(); 32];
                Validator {
                    id: Hash::digest(index.to_be_bytes()),
                    stake: *stake,
                    public_key: bls.public_key(&key).unwrap(),
                    proof_of_possession: bls.proof_of_possession(&key).unwrap(),
                }
            })
            .collect();
        ValidatorSet::new(validators, scheme().as_ref()).unwrap()
    }

    fn key_for(id: Hash) -> Vec<u8> {
        (1_u8..=32)
            .find_map(|index| {
                let key = vec![index; 32];
                (Hash::digest(usize::from(index - 1).to_be_bytes()) == id).then_some(key)
            })
            .unwrap()
    }
}
