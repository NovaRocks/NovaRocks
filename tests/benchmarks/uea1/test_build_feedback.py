import importlib.util
import pathlib
import sys
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("build_feedback.py")
SPEC = importlib.util.spec_from_file_location("uea1_build_feedback", MODULE_PATH)
BUILD_FEEDBACK = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(BUILD_FEEDBACK)


class BuildFeedbackTest(unittest.TestCase):
    def test_aggregates_same_tick_descendant_tree_rss(self):
        rows = [
            (10, 1, 10, 1_000),
            (11, 10, 10, 2_000),
            (12, 11, 99, 3_000),
            (13, 1, 77, 100_000),
        ]
        rss, pids = BUILD_FEEDBACK._aggregate_process_tree(rows, 10, 10)
        self.assertEqual(rss, 6_000)
        self.assertEqual(pids, [10, 11, 12])

    def test_run_command_observes_allocating_descendant(self):
        script = (
            "import subprocess,sys,time; "
            "p=subprocess.Popen([sys.executable,'-c',"
            "'import time; x=bytearray(24*1024*1024); time.sleep(0.35)']); "
            "time.sleep(0.2); p.wait()"
        )
        return_code, report = BUILD_FEEDBACK.run_command(
            [sys.executable, "-c", script], sample_interval_millis=20
        )
        self.assertEqual(return_code, 0)
        self.assertGreater(report["peak_process_tree_rss_bytes"], 24 * 1024 * 1024)
        self.assertTrue(
            any(sample["process_count"] >= 2 for sample in report["rss_samples"])
        )
        self.assertGreater(report["sampling"]["successful_samples"], 0)
        self.assertIn("missed_deadlines", report["sampling"])

    def test_unavailable_samples_are_explicit(self):
        def unavailable():
            return [], "synthetic unavailable"

        return_code, report = BUILD_FEEDBACK.run_command(
            [sys.executable, "-c", "import time; time.sleep(0.03)"],
            sample_interval_millis=10,
            snapshot=unavailable,
        )
        self.assertEqual(return_code, 0)
        self.assertIsNone(report["peak_process_tree_rss_bytes"])
        self.assertGreater(report["sampling"]["unavailable_samples"], 0)
        self.assertEqual(
            report["sampling"]["unavailable_reasons"], ["synthetic unavailable"]
        )


if __name__ == "__main__":
    unittest.main()
