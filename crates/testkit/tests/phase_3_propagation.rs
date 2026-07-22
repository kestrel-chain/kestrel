use network::{KestrelCast, KestrelCastConfig, RelayCandidate};
use testkit::{PropagationConfig, PropagationSimulator, lan_validators};

#[test]
fn twenty_five_nodes_reconstruct_after_relays_die_mid_propagation() {
    let validators = lan_validators(25);
    let block = vec![0x5a; 256 * 1024];
    let codec = KestrelCast::new(KestrelCastConfig::default()).unwrap();
    let shreds = codec.encode(&block).unwrap();
    let candidates: Vec<_> = validators
        .iter()
        .map(|validator| RelayCandidate {
            id: validator.id,
            stake: validator.stake,
        })
        .collect();
    let plan = KestrelCast::relay_plan(shreds[0].block_id, &shreds, &candidates, 16, 2).unwrap();
    let mut config = PropagationConfig::default();
    for relay in plan.relays.iter().take(4) {
        config.killed_relays_at_ms.insert(*relay, 9);
    }

    let report = PropagationSimulator::propagate(&block, &validators, &config).unwrap();
    assert_eq!(report.validator_count, 25);
    assert_eq!(report.killed_relays.len(), 4);
    assert!(report.reconstructed_nodes() >= 20);
    assert!(report.reconstructed_stake * 100 >= report.total_stake * 80);
    assert!(
        report
            .time_to_80_percent_stake_ms
            .is_some_and(|time| time < 50)
    );
}

#[test]
fn realistic_lan_profile_reaches_eighty_percent_stake_under_fifty_ms() {
    let validators = lan_validators(32);
    let block: Vec<_> = (0_u8..=255).cycle().take(512 * 1024).collect();
    let report =
        PropagationSimulator::propagate(&block, &validators, &PropagationConfig::default())
            .unwrap();
    assert!(report.reconstructed_stake * 100 >= report.total_stake * 80);
    assert!(
        report
            .time_to_80_percent_stake_ms
            .is_some_and(|time| time < 50)
    );
}
