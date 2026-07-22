use testkit::ChaosCampaign;

fn main() {
    let report = ChaosCampaign::default()
        .run_simulated()
        .expect("checked-in chaos campaign must be valid");
    println!(
        "iterations={} finalized={} max_view_changes={} safety_violations={} liveness_failures={}",
        report.iterations,
        report.finalized_heights,
        report.maximum_view_changes,
        report.safety_violations,
        report.liveness_failures
    );
}
