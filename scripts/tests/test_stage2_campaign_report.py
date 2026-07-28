import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "stage2_campaign_report.py"
SPEC = importlib.util.spec_from_file_location("stage2_campaign_report", SCRIPT)
assert SPEC and SPEC.loader
campaign = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = campaign
SPEC.loader.exec_module(campaign)


class Stage2CampaignReportTests(unittest.TestCase):
    def write_fixture(self, root: Path, conflicting: bool = False) -> Path:
        validators = []
        genesis_validators = []
        for index in range(4):
            name = f"v{index + 1}"
            validator_id = f"{index + 1:02x}" * 32
            log = root / f"{name}.jsonl"
            block = "block-b" if conflicting and index == 3 else "block-a"
            events = [
                {
                    "timestamp": f"2026-07-27T12:00:00.00{index}Z",
                    "level": "INFO",
                    "fields": {
                        "message": "validator RPC ready",
                        "genesis": "genesis-a",
                        "validator_id": validator_id,
                    },
                    "target": "node",
                },
                {
                    "timestamp": f"2026-07-27T12:00:00.01{index}Z",
                    "level": "TRACE",
                    "fields": {
                        "message": "admitted transaction",
                        "transaction_id": "tx-a",
                    },
                    "target": "node::pipeline",
                },
                {
                    "timestamp": f"2026-07-27T12:00:00.10{index}Z",
                    "level": "DEBUG",
                    "fields": {
                        "message": "finalized height",
                        "height": 1,
                        "view": 0,
                        "block": block,
                        "latency_ms": 100 + index,
                    },
                    "target": "node::coordinator",
                },
                {
                    "timestamp": f"2026-07-27T12:00:00.11{index}Z",
                    "level": "DEBUG",
                    "fields": {
                        "message": "committed block",
                        "height": 1,
                    },
                    "target": "node::lifecycle",
                },
            ]
            log.write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )
            validators.append(
                {
                    "name": name,
                    "validator_id": validator_id,
                    "stake": 20,
                    "log": log.name,
                }
            )
            genesis_validators.append(
                {
                    "name": name,
                    "validator": {
                        "id": [index + 1] * 32,
                        "stake": 20,
                    },
                }
            )
        (root / "genesis.json").write_text(
            json.dumps(
                {
                    "chain_id": "fixture-chain",
                    "validators": genesis_validators,
                }
            ),
            encoding="utf-8",
        )
        manifest = {
            "schema_version": 1,
            "campaign": "fixture",
            "chain_id": "fixture-chain",
            "genesis": "genesis.json",
            "genesis_hash": "genesis-a",
            "propagation_stake_threshold": 0.8,
            "measured_maximum_clock_skew_ms": 2,
            "validators": validators,
            "gates": {
                "min_propagation_samples": 1,
                "min_finality_samples": 4,
                "max_incomplete_propagation": 0,
                "max_malformed_log_lines": 0,
                "max_clock_skew_ms": 5,
                "max_execution_lag": 0,
            },
        }
        path = root / "manifest.json"
        path.write_text(json.dumps(manifest), encoding="utf-8")
        return path

    def report(self, manifest_path: Path):
        manifest, validators = campaign.load_manifest(manifest_path)
        parsed, checksums = campaign.parse_logs(validators)
        return campaign.build_report(manifest, validators, parsed, checksums)

    def test_computes_stake_weighted_propagation_and_finality(self):
        with tempfile.TemporaryDirectory() as directory:
            report = self.report(self.write_fixture(Path(directory)))
        self.assertTrue(report["passed"])
        self.assertEqual(
            report["metrics"]["propagation_to_stake_ms"]["count"],
            1,
        )
        self.assertAlmostEqual(
            report["metrics"]["propagation_to_stake_ms"]["p50"],
            3.0,
        )
        self.assertEqual(report["metrics"]["finality_latency_ms"]["count"], 4)
        self.assertEqual(report["metrics"]["validator_progress"]["v4"]["execution_lag"], 0)
        self.assertFalse(report["safety_violations"])

    def test_conflicting_finalized_blocks_fail_the_report(self):
        with tempfile.TemporaryDirectory() as directory:
            report = self.report(self.write_fixture(Path(directory), conflicting=True))
        self.assertFalse(report["passed"])
        self.assertEqual(report["safety_violations"][0]["height"], 1)

    def test_transaction_that_never_reaches_threshold_fails_the_gate(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self.write_fixture(root)
            events = [
                json.loads(line)
                for line in (root / "v4.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            events = [
                event
                for event in events
                if event["fields"].get("message") != "admitted transaction"
            ]
            (root / "v4.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )
            report = self.report(manifest)
        self.assertFalse(report["passed"])
        self.assertEqual(report["metrics"]["incomplete_propagation_count"], 1)

    def test_cli_writes_json_and_markdown_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self.write_fixture(root)
            output = root / "report.json"
            markdown = root / "report.md"
            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--manifest",
                    str(manifest),
                    "--output",
                    str(output),
                    "--markdown",
                    str(markdown),
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertTrue(json.loads(output.read_text(encoding="utf-8"))["passed"])
            self.assertIn("**Verdict:** PASS", markdown.read_text(encoding="utf-8"))

    def test_manifest_stake_must_match_genesis(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest_path = self.write_fixture(root)
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["validators"][0]["stake"] = 21
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(campaign.CampaignError, "stake"):
                campaign.load_manifest(manifest_path)

    def test_log_validator_id_must_match_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self.write_fixture(root)
            events = [
                json.loads(line)
                for line in (root / "v4.jsonl").read_text(encoding="utf-8").splitlines()
            ]
            events[0]["fields"]["validator_id"] = "ff" * 32
            (root / "v4.jsonl").write_text(
                "".join(json.dumps(event) + "\n" for event in events),
                encoding="utf-8",
            )
            report = self.report(manifest)
        self.assertFalse(report["passed"])
        self.assertEqual(report["validator_id_mismatches"][0]["validator"], "v4")


if __name__ == "__main__":
    unittest.main()
