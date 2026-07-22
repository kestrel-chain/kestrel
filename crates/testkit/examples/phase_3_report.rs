use network::{KestrelCast, KestrelCastConfig, RelayCandidate};
use testkit::{PropagationConfig, PropagationSimulator, lan_validators};

fn main() {
    let block = vec![0x5a; 512 * 1024];
    let baseline_validators = lan_validators(32);
    let baseline = PropagationSimulator::propagate(
        &block,
        &baseline_validators,
        &PropagationConfig::default(),
    )
    .expect("the checked-in LAN profile is valid");
    println!(
        "baseline: nodes={}/{}, stake={}/{}, p80_ms={:?}",
        baseline.reconstructed_nodes(),
        baseline.validator_count,
        baseline.reconstructed_stake,
        baseline.total_stake,
        baseline.time_to_80_percent_stake_ms
    );

    let failure_validators = lan_validators(25);
    let codec = KestrelCast::new(KestrelCastConfig::default()).expect("coding is valid");
    let shreds = codec.encode(&block).expect("block encoding succeeds");
    let candidates: Vec<_> = failure_validators
        .iter()
        .map(|validator| RelayCandidate {
            id: validator.id,
            stake: validator.stake,
        })
        .collect();
    let plan = KestrelCast::relay_plan(shreds[0].block_id, &shreds, &candidates, 16, 2)
        .expect("relay plan is valid");
    let mut failure_config = PropagationConfig::default();
    for relay in plan.relays.iter().take(4) {
        failure_config.killed_relays_at_ms.insert(*relay, 9);
    }
    let failure = PropagationSimulator::propagate(&block, &failure_validators, &failure_config)
        .expect("failure profile is valid");
    println!(
        "four-relay failure: nodes={}/{}, stake={}/{}, p80_ms={:?}",
        failure.reconstructed_nodes(),
        failure.validator_count,
        failure.reconstructed_stake,
        failure.total_stake,
        failure.time_to_80_percent_stake_ms
    );
}
