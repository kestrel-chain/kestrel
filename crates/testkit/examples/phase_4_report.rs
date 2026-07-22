use std::collections::BTreeSet;

use testkit::{ConsensusFaults, ConsensusSimConfig, ConsensusSimulator, consensus_lan_validators};
use types::Hash;

fn main() {
    let validators = consensus_lan_validators(20);
    let parent = Hash::digest(b"phase-4-parent");
    let transactions = (0_u64..256)
        .map(|index| Hash::digest(index.to_be_bytes()))
        .collect::<Vec<_>>();
    let baseline = ConsensusSimulator::finalize_height(
        &validators,
        parent,
        &transactions,
        &ConsensusFaults::default(),
        ConsensusSimConfig::default(),
    )
    .unwrap();

    let byzantine = validators
        .iter()
        .take(3)
        .map(|validator| validator.id)
        .collect::<BTreeSet<_>>();
    let faults = ConsensusFaults {
        withholding: byzantine.clone(),
        byzantine,
        offline: validators
            .iter()
            .skip(3)
            .take(4)
            .map(|validator| validator.id)
            .collect(),
        ..ConsensusFaults::default()
    };
    let fallback = ConsensusSimulator::finalize_height(
        &validators,
        parent,
        &transactions,
        &faults,
        ConsensusSimConfig::default(),
    )
    .unwrap();

    println!(
        "baseline: path={:?}, finality={}ms, view_changes={}",
        baseline.path, baseline.finality_latency_ms, baseline.view_changes
    );
    println!(
        "15% Byzantine withholding + 20% offline: path={:?}, finality={}ms, view_changes={}",
        fallback.path, fallback.finality_latency_ms, fallback.view_changes
    );
}
