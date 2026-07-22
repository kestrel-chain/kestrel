//! Deterministic local-devnet simulation and cross-crate integration harnesses.

mod chaos;
mod consensus_sim;

pub use chaos::{
    ChaosAction, ChaosCampaign, ChaosCampaignError, ChaosCampaignReport, ChaosObservation,
    ChaosTarget,
};

pub use consensus_sim::{
    ConsensusFaults, ConsensusReport, ConsensusSimConfig, ConsensusSimValidator,
    ConsensusSimulator, FinalityPath, consensus_lan_validators,
};

use std::collections::{BTreeMap, BTreeSet};

use network::{KestrelCast, KestrelCastConfig, KestrelCastError, RelayCandidate, RelayPlan, Shred};
use rand::{Rng, SeedableRng, rngs::StdRng};
use thiserror::Error;
use types::Hash;

const BASIS_POINTS: u16 = 10_000;

/// One validator in the deterministic Phase 3 propagation model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SimulatedValidator {
    pub id: Hash,
    pub stake: u64,
    pub one_way_latency_ms: u64,
    pub jitter_ms: u64,
    pub loss_basis_points: u16,
}

/// Network and relay parameters for a single `KestrelCast` simulation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropagationConfig {
    pub coding: KestrelCastConfig,
    pub relay_count: usize,
    pub replication_factor: usize,
    pub relay_shred_spacing_ms: u64,
    pub killed_relays_at_ms: BTreeMap<Hash, u64>,
    pub random_seed: u64,
}

impl Default for PropagationConfig {
    fn default() -> Self {
        Self {
            coding: KestrelCastConfig::default(),
            relay_count: 16,
            replication_factor: 2,
            relay_shred_spacing_ms: 1,
            killed_relays_at_ms: BTreeMap::new(),
            random_seed: 0x004b_4553_5452_454c,
        }
    }
}

/// Deterministic acceptance evidence from a simulated block propagation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropagationReport {
    pub block_id: Hash,
    pub validator_count: usize,
    pub selected_relays: Vec<Hash>,
    pub killed_relays: Vec<Hash>,
    pub reconstruction_times_ms: BTreeMap<Hash, u64>,
    pub reconstructed_stake: u128,
    pub total_stake: u128,
    pub time_to_80_percent_stake_ms: Option<u64>,
}

impl PropagationReport {
    #[must_use]
    pub fn reconstructed_nodes(&self) -> usize {
        self.reconstruction_times_ms.len()
    }
}

/// Invalid simulation topology or propagation failure.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum SimulationError {
    #[error(transparent)]
    KestrelCast(#[from] KestrelCastError),
    #[error("simulation requires at least 20 validators")]
    TooFewValidators,
    #[error("validator IDs must be unique and stake must be nonzero")]
    InvalidValidator,
    #[error("loss must not exceed 10,000 basis points")]
    InvalidLoss,
    #[error("validator stake sum overflowed")]
    StakeOverflow,
}

/// Runs a deterministic, event-time model with one relay layer.
pub struct PropagationSimulator;

impl PropagationSimulator {
    /// Propagates one block through stake-weighted relays under latency, loss, and
    /// scheduled relay failure. Relays forward directly to all validators; there
    /// is no relay-to-relay tree.
    ///
    /// # Errors
    ///
    /// Rejects invalid topologies, invalid coding, or overflowed stake totals.
    pub fn propagate(
        block: &[u8],
        validators: &[SimulatedValidator],
        config: &PropagationConfig,
    ) -> Result<PropagationReport, SimulationError> {
        validate_validators(validators)?;
        let codec = KestrelCast::new(config.coding)?;
        let shreds = codec.encode(block)?;
        let block_id = shreds[0].block_id;
        let candidates: Vec<_> = validators
            .iter()
            .map(|validator| RelayCandidate {
                id: validator.id,
                stake: validator.stake,
            })
            .collect();
        let plan = KestrelCast::relay_plan(
            block_id,
            &shreds,
            &candidates,
            config.relay_count,
            config.replication_factor,
        )?;
        let by_id: BTreeMap<_, _> = validators
            .iter()
            .map(|validator| (validator.id, *validator))
            .collect();
        let deliveries = simulate_deliveries(
            validators,
            config,
            &plan,
            &by_id,
            &mut StdRng::seed_from_u64(config.random_seed),
        );

        let mut reconstruction_times_ms = BTreeMap::new();
        for validator in validators {
            let Some(node_deliveries) = deliveries.get(&validator.id) else {
                continue;
            };
            let mut arrivals: Vec<_> = node_deliveries.values().cloned().collect();
            arrivals.sort_unstable_by_key(|(time, shred)| (*time, shred.index));
            let mut received = Vec::with_capacity(config.coding.data_shards);
            for (time, shred) in arrivals {
                received.push(shred);
                if received.len() >= config.coding.data_shards
                    && KestrelCast::reconstruct(&received).is_ok()
                {
                    reconstruction_times_ms.insert(validator.id, time);
                    break;
                }
            }
        }

        let total_stake = validators.iter().try_fold(0_u128, |total, validator| {
            total
                .checked_add(u128::from(validator.stake))
                .ok_or(SimulationError::StakeOverflow)
        })?;
        let reconstructed_stake = validators.iter().try_fold(0_u128, |total, validator| {
            if reconstruction_times_ms.contains_key(&validator.id) {
                total
                    .checked_add(u128::from(validator.stake))
                    .ok_or(SimulationError::StakeOverflow)
            } else {
                Ok(total)
            }
        })?;
        let time_to_80_percent_stake_ms =
            stake_percentile_time(validators, &reconstruction_times_ms, total_stake, 80);
        let killed_relays = plan
            .relays
            .iter()
            .copied()
            .filter(|relay| config.killed_relays_at_ms.contains_key(relay))
            .collect();
        Ok(PropagationReport {
            block_id,
            validator_count: validators.len(),
            selected_relays: plan.relays,
            killed_relays,
            reconstruction_times_ms,
            reconstructed_stake,
            total_stake,
            time_to_80_percent_stake_ms,
        })
    }
}

type Deliveries = BTreeMap<Hash, BTreeMap<u16, (u64, Shred)>>;

fn simulate_deliveries(
    validators: &[SimulatedValidator],
    config: &PropagationConfig,
    plan: &RelayPlan,
    by_id: &BTreeMap<Hash, SimulatedValidator>,
    random: &mut StdRng,
) -> Deliveries {
    let mut deliveries = BTreeMap::new();
    for relay_id in &plan.relays {
        let relay = by_id[relay_id];
        let relay_arrival = sampled_latency(relay, random);
        if link_dropped(relay.loss_basis_points, random) {
            continue;
        }
        for (ordinal, shred) in plan.assignments[relay_id].iter().enumerate() {
            let send_time = relay_arrival.saturating_add(
                u64::try_from(ordinal)
                    .unwrap_or(u64::MAX)
                    .saturating_mul(config.relay_shred_spacing_ms),
            );
            if is_offline(relay_id, send_time, config) {
                continue;
            }
            deliver_shred(
                validators,
                config,
                shred,
                send_time,
                random,
                &mut deliveries,
            );
        }
    }
    deliveries
}

fn deliver_shred(
    validators: &[SimulatedValidator],
    config: &PropagationConfig,
    shred: &Shred,
    send_time: u64,
    random: &mut StdRng,
    deliveries: &mut Deliveries,
) {
    for validator in validators {
        if link_dropped(validator.loss_basis_points, random) {
            continue;
        }
        let delivered_at = send_time.saturating_add(sampled_latency(*validator, random));
        if is_offline(&validator.id, delivered_at, config) {
            continue;
        }
        let node_deliveries = deliveries.entry(validator.id).or_default();
        let candidate = (delivered_at, shred.clone());
        match node_deliveries.get_mut(&shred.index) {
            Some(existing) if existing.0 <= delivered_at => {}
            Some(existing) => *existing = candidate,
            None => {
                node_deliveries.insert(shred.index, candidate);
            }
        }
    }
}

fn is_offline(id: &Hash, at_ms: u64, config: &PropagationConfig) -> bool {
    config
        .killed_relays_at_ms
        .get(id)
        .is_some_and(|killed_at| at_ms >= *killed_at)
}

/// Generates a deterministic, uniformly staked LAN topology for acceptance tests.
#[must_use]
pub fn lan_validators(count: usize) -> Vec<SimulatedValidator> {
    (0..count)
        .map(|index| SimulatedValidator {
            id: Hash::digest(index.to_be_bytes()),
            stake: 100,
            one_way_latency_ms: 4 + u64::try_from(index % 3).unwrap_or(0),
            jitter_ms: 4,
            loss_basis_points: 200,
        })
        .collect()
}

fn validate_validators(validators: &[SimulatedValidator]) -> Result<(), SimulationError> {
    if validators.len() < 20 {
        return Err(SimulationError::TooFewValidators);
    }
    let mut ids = BTreeSet::new();
    for validator in validators {
        if validator.stake == 0 || !ids.insert(validator.id) {
            return Err(SimulationError::InvalidValidator);
        }
        if validator.loss_basis_points > BASIS_POINTS {
            return Err(SimulationError::InvalidLoss);
        }
    }
    Ok(())
}

fn sampled_latency(validator: SimulatedValidator, random: &mut StdRng) -> u64 {
    validator
        .one_way_latency_ms
        .saturating_add(if validator.jitter_ms == 0 {
            0
        } else {
            random.gen_range(0..=validator.jitter_ms)
        })
}

fn link_dropped(loss_basis_points: u16, random: &mut StdRng) -> bool {
    random.gen_range(0..BASIS_POINTS) < loss_basis_points
}

fn stake_percentile_time(
    validators: &[SimulatedValidator],
    reconstruction_times: &BTreeMap<Hash, u64>,
    total_stake: u128,
    percent: u128,
) -> Option<u64> {
    let threshold = total_stake.saturating_mul(percent).div_ceil(100);
    let stakes: BTreeMap<_, _> = validators
        .iter()
        .map(|validator| (validator.id, validator.stake))
        .collect();
    let mut times: Vec<_> = reconstruction_times
        .iter()
        .map(|(id, time)| (*time, *id))
        .collect();
    times.sort_unstable();
    let mut accumulated = 0_u128;
    for (time, id) in times {
        accumulated = accumulated.saturating_add(u128::from(stakes[&id]));
        if accumulated >= threshold {
            return Some(time);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::{PropagationConfig, PropagationSimulator, SimulationError, lan_validators};

    #[test]
    fn rejects_topologies_smaller_than_phase_three_minimum() {
        assert_eq!(
            PropagationSimulator::propagate(
                b"block",
                &lan_validators(19),
                &PropagationConfig::default()
            ),
            Err(SimulationError::TooFewValidators)
        );
    }
}
