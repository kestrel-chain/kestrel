use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use consensus::{
    CertificateKind, ConsensusError, Validator, ValidatorSet, Vote, VoteCollector, VotePhase,
};
use crypto::{AggregateSignatureScheme, Bls12381Scheme};
use thiserror::Error;
use types::Hash;

const BASIS_POINTS: u16 = 10_000;

/// Validator and deterministic network timing used by the Phase 4 simulator.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusSimValidator {
    pub id: Hash,
    pub stake: u64,
    pub private_key: Vec<u8>,
    pub one_way_latency_ms: u64,
}

/// Byzantine and network failures injected independently into a height.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ConsensusFaults {
    pub byzantine: BTreeSet<Hash>,
    pub offline: BTreeSet<Hash>,
    pub withholding: BTreeSet<Hash>,
    pub equivocating_leaders: BTreeSet<Hash>,
    pub partitioned_links: BTreeSet<(Hash, Hash)>,
    pub additional_delay_ms: BTreeMap<Hash, u64>,
    pub drop_basis_points: u16,
}

/// Timing and termination bounds for one simulated height.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConsensusSimConfig {
    pub height: u64,
    pub local_timeout_ms: u64,
    pub max_view_changes: u64,
    pub random_seed: u64,
}

impl Default for ConsensusSimConfig {
    fn default() -> Self {
        Self {
            height: 1,
            local_timeout_ms: 120,
            max_view_changes: 8,
            random_seed: 0x4b45_5354_5245_4c04,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityPath {
    Fast,
    Fallback,
}

/// Acceptance evidence from one deterministic Kestrel-BFT simulation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsensusReport {
    pub block_id: Hash,
    pub finalized_height: u64,
    pub finalized_view: u64,
    pub view_changes: u64,
    pub path: FinalityPath,
    pub finality_latency_ms: u64,
    pub signed_stake: u128,
}

#[derive(Debug, Error)]
pub enum ConsensusSimulationError {
    #[error(transparent)]
    Consensus(#[from] ConsensusError),
    #[error("consensus simulator requires at least 20 validators")]
    TooFewValidators,
    #[error("fault identities must exist, and Byzantine/offline sets must be disjoint")]
    InvalidFaultIdentity,
    #[error("drop rate must not exceed 10,000 basis points")]
    InvalidDropRate,
    #[error("no 60% timeout certificate formed")]
    ViewChangeStalled,
    #[error("height did not finalize within the configured view-change bound")]
    FinalityBoundExceeded,
}

pub struct ConsensusSimulator;

impl ConsensusSimulator {
    /// Orders and finalizes one transaction list under deterministic delay/loss/fault injection.
    ///
    /// Equivocating leaders deliberately lose their view: honest nodes detect the
    /// conflicting proposal and use local timeouts. Vote withholding is charged
    /// to the Byzantine budget; crash/offline stake is checked separately.
    ///
    /// # Errors
    ///
    /// Rejects an invalid 20+20 scenario or failure to progress within the bound.
    pub fn finalize_height(
        validators: &[ConsensusSimValidator],
        parent_id: Hash,
        transaction_ids: &[Hash],
        faults: &ConsensusFaults,
        config: ConsensusSimConfig,
    ) -> Result<ConsensusReport, ConsensusSimulationError> {
        if validators.len() < 20 {
            return Err(ConsensusSimulationError::TooFewValidators);
        }
        if faults.drop_basis_points > BASIS_POINTS {
            return Err(ConsensusSimulationError::InvalidDropRate);
        }
        let context = build_validator_set(validators)?;
        validate_faults(&context.validators, faults)?;
        let mut elapsed = 0_u64;

        for view in 0..=config.max_view_changes {
            let leader = context.validators.leader(config.height, view).id;
            if faults.offline.contains(&leader) || faults.equivocating_leaders.contains(&leader) {
                elapsed =
                    perform_view_change(&context, faults, config, view, elapsed, &BTreeSet::new())?;
                continue;
            }
            let attempt = attempt_view(
                &context,
                parent_id,
                transaction_ids,
                faults,
                config,
                view,
                elapsed,
            )?;
            if let Some(report) = attempt.report {
                return Ok(report);
            }

            elapsed = perform_view_change(
                &context,
                faults,
                config,
                view,
                elapsed,
                &attempt.order_voters,
            )?;
        }
        Err(ConsensusSimulationError::FinalityBoundExceeded)
    }
}

struct SimulationContext {
    validators: ValidatorSet,
    scheme: Arc<dyn AggregateSignatureScheme>,
    keys: BTreeMap<Hash, Vec<u8>>,
    latency: BTreeMap<Hash, u64>,
}

struct ViewAttempt {
    report: Option<ConsensusReport>,
    order_voters: BTreeSet<Hash>,
}

struct FirstRoundOutcome {
    certificate: Option<(CertificateKind, u128, u64)>,
    voters: BTreeSet<Hash>,
}

fn build_validator_set(
    validators: &[ConsensusSimValidator],
) -> Result<SimulationContext, ConsensusError> {
    let scheme: Arc<dyn AggregateSignatureScheme> = Arc::new(Bls12381Scheme);
    let keys = validators
        .iter()
        .map(|validator| (validator.id, validator.private_key.clone()))
        .collect();
    let latency = validators
        .iter()
        .map(|validator| (validator.id, validator.one_way_latency_ms))
        .collect();
    let admitted = validators
        .iter()
        .map(|validator| {
            Ok(Validator {
                id: validator.id,
                stake: validator.stake,
                public_key: scheme.public_key(&validator.private_key)?,
                proof_of_possession: scheme.proof_of_possession(&validator.private_key)?,
            })
        })
        .collect::<Result<Vec<_>, ConsensusError>>()?;
    Ok(SimulationContext {
        validators: ValidatorSet::new(admitted, scheme.as_ref())?,
        scheme,
        keys,
        latency,
    })
}

fn attempt_view(
    context: &SimulationContext,
    parent_id: Hash,
    transaction_ids: &[Hash],
    faults: &ConsensusFaults,
    config: ConsensusSimConfig,
    view: u64,
    elapsed: u64,
) -> Result<ViewAttempt, ConsensusSimulationError> {
    let leader = context.validators.leader(config.height, view).id;
    let proposal = consensus::Proposal::new(
        config.height,
        view,
        parent_id,
        leader,
        transaction_ids.to_vec(),
        Hash::default(),
        None,
    );
    let first_round =
        collect_first_round(context, faults, config, view, leader, proposal.block_id)?;
    let Some((first_kind, first_stake, first_arrival)) = first_round.certificate else {
        return Ok(ViewAttempt {
            report: None,
            order_voters: first_round.voters,
        });
    };
    if first_kind == CertificateKind::Fast {
        return Ok(ViewAttempt {
            report: Some(ConsensusReport {
                block_id: proposal.block_id,
                finalized_height: config.height,
                finalized_view: view,
                view_changes: view,
                path: FinalityPath::Fast,
                finality_latency_ms: elapsed.saturating_add(first_arrival),
                signed_stake: first_stake,
            }),
            order_voters: first_round.voters,
        });
    }
    let Some((commit_stake, commit_arrival)) =
        collect_commit(context, faults, config, view, leader, proposal.block_id)?
    else {
        return Ok(ViewAttempt {
            report: None,
            order_voters: first_round.voters,
        });
    };
    let prepare_delay = context.latency[&leader]
        .saturating_mul(2)
        .saturating_add(*faults.additional_delay_ms.get(&leader).unwrap_or(&0));
    Ok(ViewAttempt {
        report: Some(ConsensusReport {
            block_id: proposal.block_id,
            finalized_height: config.height,
            finalized_view: view,
            view_changes: view,
            path: FinalityPath::Fallback,
            finality_latency_ms: elapsed
                .saturating_add(prepare_delay)
                .saturating_add(commit_arrival),
            signed_stake: commit_stake,
        }),
        order_voters: first_round.voters,
    })
}

fn collect_first_round(
    context: &SimulationContext,
    faults: &ConsensusFaults,
    config: ConsensusSimConfig,
    view: u64,
    leader: Hash,
    block_id: Hash,
) -> Result<FirstRoundOutcome, ConsensusError> {
    let mut arrivals = eligible_arrivals(
        &context.validators,
        &context.latency,
        faults,
        leader,
        config.random_seed,
        view,
        VotePhase::Order,
    );
    arrivals.sort_unstable_by_key(|(arrival, id)| (*arrival, *id));
    let mut fast = VoteCollector::new(
        &context.validators,
        Arc::clone(&context.scheme),
        CertificateKind::Fast,
        config.height,
        view,
        block_id,
    );
    let mut prepare = VoteCollector::new(
        &context.validators,
        Arc::clone(&context.scheme),
        CertificateKind::Prepare,
        config.height,
        view,
        block_id,
    );
    let mut prepared = None;
    let mut voters = BTreeSet::new();
    for (arrival, validator) in arrivals {
        voters.insert(validator);
        let vote = Vote::sign(
            validator,
            &context.keys[&validator],
            config.height,
            view,
            block_id,
            VotePhase::Order,
            context.scheme.as_ref(),
        )?;
        if let Some(certificate) = prepare.add_vote(vote.clone())? {
            prepared = Some((CertificateKind::Prepare, certificate.signed_stake, arrival));
        }
        if let Some(certificate) = fast.add_vote(vote)? {
            return Ok(FirstRoundOutcome {
                certificate: Some((CertificateKind::Fast, certificate.signed_stake, arrival)),
                voters,
            });
        }
    }
    Ok(FirstRoundOutcome {
        certificate: prepared,
        voters,
    })
}

fn collect_commit(
    context: &SimulationContext,
    faults: &ConsensusFaults,
    config: ConsensusSimConfig,
    view: u64,
    leader: Hash,
    block_id: Hash,
) -> Result<Option<(u128, u64)>, ConsensusError> {
    let mut collector = VoteCollector::new(
        &context.validators,
        Arc::clone(&context.scheme),
        CertificateKind::Commit,
        config.height,
        view,
        block_id,
    );
    let mut arrivals = eligible_arrivals(
        &context.validators,
        &context.latency,
        faults,
        leader,
        config.random_seed,
        view,
        VotePhase::Commit,
    );
    arrivals.sort_unstable_by_key(|(arrival, id)| (*arrival, *id));
    for (arrival, validator) in arrivals {
        let vote = Vote::sign(
            validator,
            &context.keys[&validator],
            config.height,
            view,
            block_id,
            VotePhase::Commit,
            context.scheme.as_ref(),
        )?;
        if let Some(certificate) = collector.add_vote(vote)? {
            return Ok(Some((certificate.signed_stake, arrival)));
        }
    }
    Ok(None)
}

fn validate_faults(
    validators: &ValidatorSet,
    faults: &ConsensusFaults,
) -> Result<(), ConsensusSimulationError> {
    if !faults.byzantine.is_disjoint(&faults.offline)
        || !faults.withholding.is_subset(&faults.byzantine)
        || !faults.equivocating_leaders.is_subset(&faults.byzantine)
    {
        return Err(ConsensusSimulationError::InvalidFaultIdentity);
    }
    let known: BTreeSet<_> = validators
        .validators()
        .iter()
        .map(|validator| validator.id)
        .collect();
    if !faults.byzantine.is_subset(&known) || !faults.offline.is_subset(&known) {
        return Err(ConsensusSimulationError::InvalidFaultIdentity);
    }
    let byzantine_stake = faults.byzantine.iter().try_fold(0_u128, |stake, id| {
        stake
            .checked_add(u128::from(validators.validator(*id).unwrap().stake))
            .ok_or(ConsensusError::StakeOverflow)
    })?;
    let offline_stake = faults.offline.iter().try_fold(0_u128, |stake, id| {
        stake
            .checked_add(u128::from(validators.validator(*id).unwrap().stake))
            .ok_or(ConsensusError::StakeOverflow)
    })?;
    validators.validate_fault_budget(byzantine_stake, offline_stake)?;
    Ok(())
}

fn perform_view_change(
    context: &SimulationContext,
    faults: &ConsensusFaults,
    config: ConsensusSimConfig,
    view: u64,
    elapsed: u64,
    order_voters: &BTreeSet<Hash>,
) -> Result<u64, ConsensusSimulationError> {
    let mut collector = VoteCollector::new(
        &context.validators,
        Arc::clone(&context.scheme),
        CertificateKind::Timeout,
        config.height,
        view,
        Hash::default(),
    );
    let mut arrivals = context
        .validators
        .validators()
        .iter()
        .filter(|validator| {
            !faults.offline.contains(&validator.id)
                && !faults.withholding.contains(&validator.id)
                && !order_voters.contains(&validator.id)
        })
        .map(|validator| {
            (
                context.latency[&validator.id]
                    .saturating_mul(2)
                    .saturating_add(*faults.additional_delay_ms.get(&validator.id).unwrap_or(&0)),
                validator.id,
            )
        })
        .collect::<Vec<_>>();
    arrivals.sort_unstable();
    for (arrival, validator) in arrivals {
        let vote = Vote::sign(
            validator,
            &context.keys[&validator],
            config.height,
            view,
            Hash::default(),
            VotePhase::Timeout,
            context.scheme.as_ref(),
        )?;
        if collector.add_vote(vote)?.is_some() {
            return Ok(elapsed
                .saturating_add(config.local_timeout_ms)
                .saturating_add(arrival));
        }
    }
    Err(ConsensusSimulationError::ViewChangeStalled)
}

fn eligible_arrivals(
    validators: &ValidatorSet,
    latency: &BTreeMap<Hash, u64>,
    faults: &ConsensusFaults,
    leader: Hash,
    seed: u64,
    view: u64,
    phase: VotePhase,
) -> Vec<(u64, Hash)> {
    validators
        .validators()
        .iter()
        .filter(|validator| {
            !faults.offline.contains(&validator.id)
                && !faults.withholding.contains(&validator.id)
                && !is_partitioned(leader, validator.id, &faults.partitioned_links)
                && !deterministically_dropped(
                    seed,
                    view,
                    validator.id,
                    phase,
                    faults.drop_basis_points,
                )
        })
        .map(|validator| {
            (
                latency[&validator.id]
                    .saturating_mul(2)
                    .saturating_add(*faults.additional_delay_ms.get(&validator.id).unwrap_or(&0)),
                validator.id,
            )
        })
        .collect()
}

fn is_partitioned(left: Hash, right: Hash, links: &BTreeSet<(Hash, Hash)>) -> bool {
    links.contains(&(left, right)) || links.contains(&(right, left))
}

fn deterministically_dropped(
    seed: u64,
    view: u64,
    validator: Hash,
    phase: VotePhase,
    basis_points: u16,
) -> bool {
    if basis_points == 0 {
        return false;
    }
    let mut input = Vec::with_capacity(49);
    input.extend_from_slice(&seed.to_be_bytes());
    input.extend_from_slice(&view.to_be_bytes());
    input.extend_from_slice(validator.as_bytes());
    input.push(match phase {
        VotePhase::Order => 0,
        VotePhase::Commit => 1,
        VotePhase::Timeout => 2,
    });
    let digest = Hash::digest(input);
    let sample = u16::from_be_bytes([digest.as_bytes()[0], digest.as_bytes()[1]]) % BASIS_POINTS;
    sample < basis_points
}

/// Generates deterministic equal-stake validators with realistic LAN latency.
#[must_use]
pub fn consensus_lan_validators(count: usize) -> Vec<ConsensusSimValidator> {
    (0..count)
        .map(|index| {
            let mut key = vec![0_u8; 32];
            key[..8].copy_from_slice(&u64::try_from(index + 1).unwrap_or(u64::MAX).to_be_bytes());
            ConsensusSimValidator {
                id: Hash::digest(
                    [b"consensus-validator".as_slice(), &index.to_be_bytes()].concat(),
                ),
                stake: 100,
                private_key: key,
                one_way_latency_ms: 4 + u64::try_from(index % 3).unwrap_or(0),
            }
        })
        .collect()
}
