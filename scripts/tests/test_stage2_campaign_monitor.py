import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).parents[1] / "stage2_campaign_monitor.py"
SPEC = importlib.util.spec_from_file_location("stage2_campaign_monitor", SCRIPT)
assert SPEC and SPEC.loader
monitor_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = monitor_module
SPEC.loader.exec_module(monitor_module)


def status(height: int, block: str) -> dict:
    return {
        "chainId": "chain-a",
        "genesisHash": "genesis-a",
        "finalizedHeight": height,
        "finalizedBlock": block,
    }


class CampaignMonitorTests(unittest.TestCase):
    def test_tracks_progress_and_accepts_agreement(self):
        monitor = monitor_module.CampaignMonitor("chain-a", "genesis-a", 10)
        monitor.observe("v1", status(7, "block-7"), 0)
        monitor.observe("v2", status(7, "block-7"), 1)
        monitor.observe("v1", status(8, "block-8"), 2)
        monitor.check_liveness(11)
        self.assertEqual(monitor.maximum_height, 8)
        self.assertEqual(monitor.observations, 3)

    def test_rejects_conflicting_blocks_at_one_height(self):
        monitor = monitor_module.CampaignMonitor("chain-a", "genesis-a", 10)
        monitor.observe("v1", status(7, "block-a"), 0)
        with self.assertRaisesRegex(monitor_module.MonitorError, "safety violation"):
            monitor.observe("v2", status(7, "block-b"), 1)

    def test_rejects_a_stalled_tip(self):
        monitor = monitor_module.CampaignMonitor("chain-a", "genesis-a", 10)
        monitor.observe("v1", status(7, "block-a"), 0)
        with self.assertRaisesRegex(monitor_module.MonitorError, "liveness failure"):
            monitor.check_liveness(11)

    def test_rejects_foreign_genesis(self):
        monitor = monitor_module.CampaignMonitor("chain-a", "genesis-a", 10)
        foreign = status(7, "block-a")
        foreign["genesisHash"] = "genesis-b"
        with self.assertRaisesRegex(monitor_module.MonitorError, "reported genesis"):
            monitor.observe("v1", foreign, 0)

    def test_local_monitor_run_writes_observations_and_summary(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = root / "manifest.json"
            manifest.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "campaign": "local-fixture",
                        "chain_id": "chain-a",
                        "genesis_hash": "genesis-a",
                        "validators": [
                            {"name": f"v{index}", "rpc": f"http://v{index}/"}
                            for index in range(1, 5)
                        ],
                    }
                ),
                encoding="utf-8",
            )
            output = root / "observations.jsonl"
            summary = root / "summary.json"
            args = argparse.Namespace(
                manifest=manifest,
                duration_seconds=0.02,
                interval_seconds=0.005,
                stall_seconds=1,
                rpc_timeout_seconds=1,
                output=output,
                summary=summary,
            )
            with mock.patch.object(
                monitor_module,
                "fetch_status",
                return_value=status(7, "block-a"),
            ):
                result = monitor_module.run(args)
            evidence = json.loads(summary.read_text(encoding="utf-8"))
            self.assertEqual(result, 0)
            self.assertTrue(evidence["passed"])
            self.assertGreater(evidence["observations"], 0)
            self.assertGreater(len(output.read_text(encoding="utf-8").splitlines()), 0)


if __name__ == "__main__":
    unittest.main()
