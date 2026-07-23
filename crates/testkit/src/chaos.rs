use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use consensus::{
    CertificateKind, ConsensusError, Proposal, Replica, SignedProposal, Validator, ValidatorSet,
    Vote, VoteCollector, VotePhase,
};
use crypto::{AggregateSignatureScheme, Bls12381Scheme};
use rand::{Rng, SeedableRng, rngs::StdRng};
use thiserror::Error;
use types::Hash;

use crate::{ConsensusFaults, ConsensusSimConfig, ConsensusSimulator, consensus_lan_validators};

/// One externally orchestrated public-testnet fault. Implementations must be reversible.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChaosAction {
    KillValidator(Hash),
    IsolateValidator(Hash),
    SetMessageDropBasisPoints(u16),
    HealAll,
}

/// Minimum observation needed to enforce safety and bounded-liveness gates.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChaosObservation {
    pub finalized: BTreeMap<u64, Hash>,
    pub latest_finalized_height: u64,
}

/// Adapter boundary for a real process/Kubernetes/cloud testnet controller.
///
/// The harness deliberately does not embed provider credentials or a remote
/// command mechanism. Operators supply an authenticated adapter scoped to the
/// testnet they placed under chaos testing.
pub trait ChaosTarget {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Applies one reversible testnet fault.
    ///
    /// # Errors
    ///
    /// Returns an adapter-specific orchestration failure.
    fn apply(&mut self, action: &ChaosAction) -> Result<(), Self::Error>;

    /// Reads finalized-height/hash observations from independent nodes.
    ///
    /// # Errors
    ///
    /// Returns an adapter-specific observation failure.
    fn observe(&mut self) -> Result<ChaosObservation, Self::Error>;
}

/// Deterministic campaign configuration usable in CI or continuously by an operator.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChaosCampaign {
    pub iterations: u64,
    pub maximum_stalled_observations: u64,
    /// Randomized per-validator equivocating-leader safety trials (see
    /// [`explore_equivocating_proposal_safety`]). Each trial builds a fresh
    /// validator set and Byzantine subset, so more trials explore more of the
    /// state space; it is independent of `iterations`, which drives the
    /// aggregate fault/liveness sweep instead.
    pub equivocation_trials: u64,
}

impl Default for ChaosCampaign {
    fn default() -> Self {
        Self {
            iterations: 100,
            maximum_stalled_observations: 8,
            equivocation_trials: 200,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ChaosCampaignReport {
    pub iterations: u64,
    pub finalized_heights: u64,
    pub maximum_view_changes: u64,
    pub safety_violations: u64,
    pub liveness_failures: u64,
    /// Randomized equivocating-leader trials run (see
    /// [`explore_equivocating_proposal_safety`]); independent of `iterations`.
    pub equivocation_trials: u64,
    /// Trials in which an equivocating leader's split delivery reached a
    /// certificate on at least one side. Not itself a problem -- it only
    /// becomes one if `cross_replica_safety_violations` is also nonzero.
    pub equivocation_trials_forming_a_certificate: u64,
    /// Independent honest `Replica` instances that finalized different blocks
    /// at the same height under equivocating-leader exploration. Must be zero
    /// under the enforced <20% Byzantine budget; any nonzero value here is a
    /// genuine consensus safety bug, not a benign fault-injection outcome.
    pub cross_replica_safety_violations: u64,
}

impl ChaosCampaign {
    /// Runs the in-process safety/liveness suite across varied deterministic seeds.
    ///
    /// # Errors
    ///
    /// Rejects an empty campaign or any simulation setup error.
    pub fn run_simulated(self) -> Result<ChaosCampaignReport, ChaosCampaignError> {
        if self.iterations == 0 {
            return Err(ChaosCampaignError::EmptyCampaign);
        }
        let validators = consensus_lan_validators(20);
        let mut report = ChaosCampaignReport::default();
        if !production_cross_view_fast_safety(&validators)
            .map_err(|error| ChaosCampaignError::SafetyProbe(error.to_string()))?
        {
            report.safety_violations += 1;
        }
        let equivocation = explore_equivocating_proposal_safety(EquivocationExplorationConfig {
            iterations: self.equivocation_trials,
            random_seed: 0x4b45_5354_5245_4c05,
        })
        .map_err(|error| ChaosCampaignError::SafetyProbe(error.to_string()))?;
        report.equivocation_trials = equivocation.iterations;
        report.equivocation_trials_forming_a_certificate =
            equivocation.trials_forming_a_certificate;
        report.cross_replica_safety_violations = equivocation.cross_replica_safety_violations;
        let mut parent = Hash::digest(b"phase-six-chaos-genesis");
        let mut finalized = BTreeMap::new();
        for iteration in 0..self.iterations {
            let height = iteration + 1;
            let config = ConsensusSimConfig {
                height,
                random_seed: 0x4b45_5354_0000_0000_u64 ^ iteration,
                ..ConsensusSimConfig::default()
            };
            let mut ordered = validators
                .iter()
                .map(|validator| validator.id)
                .collect::<Vec<_>>();
            ordered.sort_unstable();
            let leader = ordered[usize::try_from(height).unwrap_or_default() % ordered.len()];
            let faults = scenario(iteration, leader, &ordered);
            match ConsensusSimulator::finalize_height(
                &validators,
                parent,
                &[Hash::digest(iteration.to_be_bytes())],
                &faults,
                config,
            ) {
                Ok(finality) => {
                    if finalized
                        .insert(finality.finalized_height, finality.block_id)
                        .is_some_and(|previous| previous != finality.block_id)
                    {
                        report.safety_violations += 1;
                    }
                    report.maximum_view_changes =
                        report.maximum_view_changes.max(finality.view_changes);
                    report.finalized_heights += 1;
                    parent = finality.block_id;
                }
                Err(_) => report.liveness_failures += 1,
            }
            report.iterations += 1;
        }
        Ok(report)
    }

    /// Applies a bounded campaign to an externally supplied public-testnet adapter.
    /// It heals all injected faults before returning, including on a failed gate.
    ///
    /// # Errors
    ///
    /// Returns adapter errors, conflicting finalized hashes, or a finality stall
    /// beyond the configured observation bound.
    pub fn run_external<T: ChaosTarget>(
        self,
        target: &mut T,
        schedule: &[ChaosAction],
    ) -> Result<ChaosCampaignReport, ChaosCampaignError> {
        if self.iterations == 0 || schedule.is_empty() {
            return Err(ChaosCampaignError::EmptyCampaign);
        }
        let mut report = ChaosCampaignReport::default();
        let mut known = BTreeMap::new();
        let mut previous_height = 0;
        let mut stalled = 0;
        for iteration in 0..self.iterations {
            let action = &schedule[usize::try_from(iteration).unwrap_or_default() % schedule.len()];
            if let Err(error) = target.apply(action) {
                let _ = target.apply(&ChaosAction::HealAll);
                return Err(ChaosCampaignError::Target(error.to_string()));
            }
            let observation = match target.observe() {
                Ok(observation) => observation,
                Err(error) => {
                    let _ = target.apply(&ChaosAction::HealAll);
                    return Err(ChaosCampaignError::Target(error.to_string()));
                }
            };
            for (height, block) in observation.finalized {
                if known
                    .insert(height, block)
                    .is_some_and(|previous| previous != block)
                {
                    let _ = target.apply(&ChaosAction::HealAll);
                    return Err(ChaosCampaignError::SafetyViolation(height));
                }
            }
            if observation.latest_finalized_height > previous_height {
                report.finalized_heights += observation.latest_finalized_height - previous_height;
                previous_height = observation.latest_finalized_height;
                stalled = 0;
            } else {
                stalled += 1;
                if stalled > self.maximum_stalled_observations {
                    let _ = target.apply(&ChaosAction::HealAll);
                    return Err(ChaosCampaignError::LivenessStall);
                }
            }
            report.iterations += 1;
        }
        target
            .apply(&ChaosAction::HealAll)
            .map_err(|error| ChaosCampaignError::Target(error.to_string()))?;
        Ok(report)
    }
}

#[allow(clippy::too_many_lines)] // Keep the full cross-view selective-delivery scenario auditable.
fn production_cross_view_fast_safety(
    validators: &[crate::ConsensusSimValidator],
) -> Result<bool, ConsensusError> {
    let scheme: Arc<dyn AggregateSignatureScheme> = Arc::new(Bls12381Scheme);
    let validator_set = ValidatorSet::new(
        validators
            .iter()
            .map(|validator| {
                Ok(Validator {
                    id: validator.id,
                    stake: validator.stake,
                    public_key: scheme.public_key(&validator.private_key)?,
                    proof_of_possession: scheme.proof_of_possession(&validator.private_key)?,
                })
            })
            .collect::<Result<Vec<_>, ConsensusError>>()?,
        scheme.as_ref(),
    )?;
    let parent = Hash::digest(b"chaos-cross-view-parent");
    let proposal = Proposal::new(
        1,
        0,
        parent,
        validator_set.leader(1, 0).id,
        vec![Hash::digest(b"selectively-delivered-fast-block")],
        None,
    );
    let mut replicas = validators
        .iter()
        .map(|validator| {
            Replica::new(
                validator.id,
                validator.private_key.clone(),
                validator_set.clone(),
                Arc::clone(&scheme),
                1,
                parent,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut fast = VoteCollector::new(
        &validator_set,
        Arc::clone(&scheme),
        CertificateKind::Fast,
        1,
        0,
        proposal.block_id,
    );
    let mut fast_certificate = None;
    for (index, validator) in validators.iter().take(16).enumerate() {
        let vote = if index < 3 {
            Vote::sign(
                validator.id,
                &validator.private_key,
                1,
                0,
                proposal.block_id,
                VotePhase::Order,
                scheme.as_ref(),
            )?
        } else {
            replicas[index].vote_for_proposal(&proposal)?
        };
        fast_certificate = fast.add_vote(vote)?;
    }
    let fast_certificate = fast_certificate.expect("16 of 20 equal-stake votes reach 80%");
    replicas[3].finalize(&fast_certificate)?;

    let mut timeout = VoteCollector::new(
        &validator_set,
        Arc::clone(&scheme),
        CertificateKind::Timeout,
        1,
        0,
        Hash::default(),
    );
    for validator in validators.iter().take(3) {
        let vote = Vote::sign(
            validator.id,
            &validator.private_key,
            1,
            0,
            Hash::default(),
            VotePhase::Timeout,
            scheme.as_ref(),
        )?;
        if timeout.add_vote(vote)?.is_some() {
            return Ok(false);
        }
    }
    for (index, replica) in replicas.iter_mut().enumerate().skip(4) {
        if let Some(vote) = replica.local_timeout()?
            && timeout.add_vote(vote)?.is_some()
        {
            return Ok(false);
        }
        if index == 15 {
            debug_assert!(
                replica
                    .snapshot()
                    .votes
                    .contains_key(&(1, 0, VotePhase::Order))
            );
        }
    }
    Ok(true)
}

/// Configuration for [`explore_equivocating_proposal_safety`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EquivocationExplorationConfig {
    pub iterations: u64,
    pub random_seed: u64,
}

impl Default for EquivocationExplorationConfig {
    fn default() -> Self {
        Self {
            iterations: 200,
            random_seed: 0x4b45_5354_5245_4c05,
        }
    }
}

/// Evidence from a randomized per-validator equivocating-leader exploration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EquivocationExplorationReport {
    pub iterations: u64,
    /// Trials in which at least one delivery group's votes reached an 80%
    /// fast certificate. Not itself a problem -- it only becomes one if
    /// `cross_replica_safety_violations` is also nonzero.
    pub trials_forming_a_certificate: u64,
    /// Independent honest `Replica` instances that finalized different
    /// blocks at the same height, or a trial where both delivery groups
    /// separately reached a certificate. Must be zero under the enforced
    /// <20% Byzantine budget; any nonzero value is a genuine consensus
    /// safety bug, not a benign fault-injection outcome.
    pub cross_replica_safety_violations: u64,
}

/// Explores whether an equivocating leader -- one that signs two different
/// proposals for the same height/view and delivers each to a different
/// randomized subset of honest validators -- can ever cause two independent
/// honest `Replica` instances to finalize different blocks at the same
/// height.
///
/// Every trial builds a fresh, randomized validator count, a randomized
/// Byzantine subset (bounded under the enforced <20% stake budget and always
/// including the leader), and a randomized delivery split of the honest
/// validators between the two conflicting proposals. Every honest
/// validator's vote and finalization decision is produced by the real,
/// production `Replica` type: this function only decides who receives which
/// message and never signs on an honest validator's behalf. That is what
/// lets it observe genuine cross-replica disagreement rather than one
/// aggregate outcome -- the gap tracked as TD-001 in `docs/TECH_DEBT.md`.
///
/// This never panics on a detected violation (an operator could be running
/// this as part of a long campaign); violations are counted in the returned
/// report for the caller to act on.
///
/// # Errors
///
/// Returns the underlying consensus error from constructing a validator set
/// or replica.
pub fn explore_equivocating_proposal_safety(
    config: EquivocationExplorationConfig,
) -> Result<EquivocationExplorationReport, ConsensusError> {
    let mut random = StdRng::seed_from_u64(config.random_seed);
    let mut report = EquivocationExplorationReport::default();
    for _ in 0..config.iterations {
        let trial = run_equivocation_trial(&mut random)?;
        report.iterations += 1;
        if trial.certificate_formed {
            report.trials_forming_a_certificate += 1;
        }
        report.cross_replica_safety_violations += trial.cross_replica_safety_violations;
    }
    Ok(report)
}

struct TrialOutcome {
    certificate_formed: bool,
    cross_replica_safety_violations: u64,
}

#[allow(clippy::too_many_lines)] // Keep the full selective-delivery trial auditable in one place.
fn run_equivocation_trial(random: &mut StdRng) -> Result<TrialOutcome, ConsensusError> {
    let scheme: Arc<dyn AggregateSignatureScheme> = Arc::new(Bls12381Scheme);
    let validator_count = random.gen_range(20..=40_usize);
    let keys = (0..validator_count)
        .map(|index| vec![u8::try_from(index + 1).unwrap_or(255); 32])
        .collect::<Vec<_>>();
    let ids = (0..validator_count)
        .map(|index| {
            Hash::digest([b"equivocation-explorer".as_slice(), &index.to_be_bytes()].concat())
        })
        .collect::<Vec<_>>();
    let admitted = (0..validator_count)
        .map(|index| {
            Ok(Validator {
                id: ids[index],
                stake: 100,
                public_key: scheme.public_key(&keys[index])?,
                proof_of_possession: scheme.proof_of_possession(&keys[index])?,
            })
        })
        .collect::<Result<Vec<_>, ConsensusError>>()?;
    let validator_set = ValidatorSet::new(admitted, scheme.as_ref())?;
    let key_by_id = ids.iter().copied().zip(keys).collect::<BTreeMap<_, _>>();

    let parent = Hash::digest(random.r#gen::<u64>().to_be_bytes());
    let leader = validator_set.leader(1, 0).id;

    // The Byzantine subset always includes the leader and stays strictly
    // under the 20% budget `validate_fault_budget` enforces.
    let max_byzantine = (validator_count - 1) / 5;
    let byzantine_count = random.gen_range(1..=max_byzantine.max(1));
    let mut byzantine = BTreeSet::from([leader]);
    let mut pool = ids
        .iter()
        .copied()
        .filter(|id| *id != leader)
        .collect::<Vec<_>>();
    while byzantine.len() < byzantine_count && !pool.is_empty() {
        let index = random.gen_range(0..pool.len());
        byzantine.insert(pool.swap_remove(index));
    }
    let byzantine_stake = u128::try_from(byzantine.len())
        .unwrap_or(u128::MAX)
        .saturating_mul(100);
    let total_stake = u128::try_from(validator_count)
        .unwrap_or(u128::MAX)
        .saturating_mul(100);
    validator_set.validate_fault_budget(byzantine_stake, 0)?;
    debug_assert!(byzantine_stake.saturating_mul(5) < total_stake);

    let honest = ids
        .iter()
        .copied()
        .filter(|id| !byzantine.contains(id))
        .collect::<Vec<_>>();

    let proposal_a = Proposal::new(
        1,
        0,
        parent,
        leader,
        vec![Hash::digest(random.r#gen::<u64>().to_be_bytes())],
        None,
    );
    let proposal_b = Proposal::new(
        1,
        0,
        parent,
        leader,
        vec![Hash::digest(random.r#gen::<u64>().to_be_bytes())],
        None,
    );
    let signed_a = SignedProposal::sign(proposal_a.clone(), &key_by_id[&leader], scheme.as_ref())?;
    let signed_b = SignedProposal::sign(proposal_b.clone(), &key_by_id[&leader], scheme.as_ref())?;
    signed_a.verify(&validator_set, scheme.as_ref())?;
    signed_b.verify(&validator_set, scheme.as_ref())?;

    // Split honest validators into two delivery groups: group A receives
    // proposal_a, everyone else (group B) receives proposal_b.
    let group_a = honest
        .iter()
        .copied()
        .filter(|_| random.gen_bool(0.5))
        .collect::<BTreeSet<_>>();
    let group_b = honest
        .iter()
        .copied()
        .filter(|id| !group_a.contains(id))
        .collect::<BTreeSet<_>>();

    let mut replicas = honest
        .iter()
        .map(|id| {
            Replica::new(
                *id,
                key_by_id[id].clone(),
                validator_set.clone(),
                Arc::clone(&scheme),
                1,
                parent,
            )
            .map(|replica| (*id, replica))
        })
        .collect::<Result<BTreeMap<_, _>, _>>()?;

    let mut votes_a = Vec::new();
    let mut votes_b = Vec::new();
    for id in &honest {
        let replica = replicas.get_mut(id).expect("constructed above");
        if group_a.contains(id) {
            votes_a.push(replica.vote_for_proposal(&proposal_a)?);
        } else {
            votes_b.push(replica.vote_for_proposal(&proposal_b)?);
        }
    }
    // A maximally adversarial Byzantine minority votes for both sides,
    // bypassing any Replica -- a real Byzantine validator is not bound by
    // the honest state machine's local safety checks.
    for id in &byzantine {
        votes_a.push(Vote::sign(
            *id,
            &key_by_id[id],
            1,
            0,
            proposal_a.block_id,
            VotePhase::Order,
            scheme.as_ref(),
        )?);
        votes_b.push(Vote::sign(
            *id,
            &key_by_id[id],
            1,
            0,
            proposal_b.block_id,
            VotePhase::Order,
            scheme.as_ref(),
        )?);
    }

    let mut collector_a = VoteCollector::new(
        &validator_set,
        Arc::clone(&scheme),
        CertificateKind::Fast,
        1,
        0,
        proposal_a.block_id,
    );
    let mut collector_b = VoteCollector::new(
        &validator_set,
        Arc::clone(&scheme),
        CertificateKind::Fast,
        1,
        0,
        proposal_b.block_id,
    );
    let mut certificate_a = None;
    for vote in votes_a {
        if let Some(certificate) = collector_a.add_vote(vote)? {
            certificate_a = Some(certificate);
        }
    }
    let mut certificate_b = None;
    for vote in votes_b {
        if let Some(certificate) = collector_b.add_vote(vote)? {
            certificate_b = Some(certificate);
        }
    }

    // Under the enforced <20% Byzantine budget, at most one side should ever
    // reach the 80% fast threshold: two disjoint 80% majorities would need
    // 60%+ overlapping stake, and that overlap can only be Byzantine. If
    // this ever fires, it is a genuine consensus safety bug.
    let mut violations = u64::from(certificate_a.is_some() && certificate_b.is_some());

    let mut finalized = BTreeMap::new();
    for (certificate, group) in [(&certificate_a, &group_a), (&certificate_b, &group_b)] {
        let Some(certificate) = certificate else {
            continue;
        };
        for id in group {
            let replica = replicas.get_mut(id).expect("constructed above");
            let block_id = replica.finalize(certificate)?;
            if finalized
                .insert(*id, block_id)
                .is_some_and(|previous| previous != block_id)
            {
                violations += 1;
            }
        }
    }
    // Cross-replica check: every independent honest replica that finalized
    // this height must agree with every other one, regardless of which
    // delivery group it was in.
    if finalized.values().collect::<BTreeSet<_>>().len() > 1 {
        violations += 1;
    }

    Ok(TrialOutcome {
        certificate_formed: certificate_a.is_some() || certificate_b.is_some(),
        cross_replica_safety_violations: violations,
    })
}

fn scenario(iteration: u64, leader: Hash, ordered: &[Hash]) -> ConsensusFaults {
    match iteration % 4 {
        0 => ConsensusFaults {
            offline: BTreeSet::from([leader]),
            ..ConsensusFaults::default()
        },
        1 => ConsensusFaults {
            byzantine: std::iter::once(leader)
                .chain(ordered.iter().copied().filter(|id| *id != leader).take(2))
                .collect(),
            equivocating_leaders: BTreeSet::from([leader]),
            ..ConsensusFaults::default()
        },
        2 => {
            let byzantine = ordered.iter().copied().take(3).collect::<BTreeSet<_>>();
            ConsensusFaults {
                withholding: byzantine.clone(),
                byzantine,
                offline: ordered.iter().copied().skip(3).take(4).collect(),
                ..ConsensusFaults::default()
            }
        }
        _ => ConsensusFaults {
            drop_basis_points: 5,
            additional_delay_ms: ordered.iter().copied().take(5).map(|id| (id, 25)).collect(),
            ..ConsensusFaults::default()
        },
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum ChaosCampaignError {
    #[error("chaos campaign and schedule must be nonempty")]
    EmptyCampaign,
    #[error("chaos target failed: {0}")]
    Target(String),
    #[error("conflicting finalized hashes observed at height {0}")]
    SafetyViolation(u64),
    #[error("production-backed consensus safety probe failed: {0}")]
    SafetyProbe(String),
    #[error("finality did not advance within the configured observation bound")]
    LivenessStall,
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, convert::Infallible};

    use types::Hash;

    use super::{ChaosAction, ChaosCampaign, ChaosObservation, ChaosTarget};

    #[test]
    fn hundred_scenario_campaign_has_no_safety_or_liveness_failure() {
        let report = ChaosCampaign::default().run_simulated().unwrap();
        assert_eq!(report.iterations, 100);
        assert_eq!(report.finalized_heights, 100);
        assert_eq!(report.safety_violations, 0);
        assert_eq!(report.liveness_failures, 0);
        assert!(report.maximum_view_changes <= 4);
        // Per-validator equivocating-leader exploration: 200 randomized
        // trials (fresh validator set, Byzantine subset, and delivery split
        // each time), each finalizing through the real production `Replica`
        // type per honest validator, must never observe two independent
        // honest replicas finalizing different blocks at the same height.
        assert_eq!(report.equivocation_trials, 200);
        assert_eq!(report.cross_replica_safety_violations, 0);
        assert!(report.equivocation_trials_forming_a_certificate > 0);
    }

    #[test]
    fn external_campaign_always_heals_the_target() {
        let mut target = AdvancingTarget::default();
        let report = ChaosCampaign {
            iterations: 4,
            maximum_stalled_observations: 1,
            equivocation_trials: 0,
        }
        .run_external(
            &mut target,
            &[
                ChaosAction::SetMessageDropBasisPoints(5),
                ChaosAction::IsolateValidator(Hash::digest(b"leader")),
            ],
        )
        .unwrap();
        assert_eq!(report.iterations, 4);
        assert_eq!(target.actions.last(), Some(&ChaosAction::HealAll));
    }

    #[derive(Default)]
    struct AdvancingTarget {
        height: u64,
        actions: Vec<ChaosAction>,
    }

    impl ChaosTarget for AdvancingTarget {
        type Error = Infallible;

        fn apply(&mut self, action: &ChaosAction) -> Result<(), Self::Error> {
            self.actions.push(action.clone());
            Ok(())
        }

        fn observe(&mut self) -> Result<ChaosObservation, Self::Error> {
            self.height += 1;
            Ok(ChaosObservation {
                finalized: BTreeMap::from([(self.height, Hash::digest(self.height.to_be_bytes()))]),
                latest_finalized_height: self.height,
            })
        }
    }
}
