use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use consensus::{
    CertificateKind, ConsensusError, Proposal, Replica, Validator, ValidatorSet, Vote,
    VoteCollector, VotePhase,
};
use crypto::{AggregateSignatureScheme, Bls12381Scheme};
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
}

impl Default for ChaosCampaign {
    fn default() -> Self {
        Self {
            iterations: 100,
            maximum_stalled_observations: 8,
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
    }

    #[test]
    fn external_campaign_always_heals_the_target() {
        let mut target = AdvancingTarget::default();
        let report = ChaosCampaign {
            iterations: 4,
            maximum_stalled_observations: 1,
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
