import importlib.util
import pathlib
import unittest


MODULE_PATH = pathlib.Path(__file__).with_name("compare.py")
SPEC = importlib.util.spec_from_file_location("uea1_compare", MODULE_PATH)
COMPARE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(COMPARE)


class CompareTest(unittest.TestCase):
    def test_zero_noise_baseline_accepts_equal_candidate(self):
        result = COMPARE.compare([100.0, 100.0], [100.0, 100.0], [100.0], 1.0, False)
        self.assertTrue(result["valid"])
        self.assertTrue(result["passed"])
        self.assertEqual(result["epsilon"], 0.02)

    def test_unstable_baseline_is_invalid(self):
        result = COMPARE.compare([80.0, 120.0], [80.0, 120.0], [100.0], 0.001, False)
        self.assertFalse(result["valid"])
        self.assertIn("noise", result["reason"])

    def test_positive_metric_rejects_zero(self):
        with self.assertRaises(ValueError):
            COMPARE.compare([0.0], [1.0], [1.0], 0.001, False)


if __name__ == "__main__":
    unittest.main()
