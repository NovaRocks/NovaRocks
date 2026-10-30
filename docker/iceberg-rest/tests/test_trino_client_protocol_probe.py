# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""G1 comparison rejects protocol drift; released-engine execution stays explicit."""
import sys
from pathlib import Path
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import trino_client_protocol_probe as probe


def signature(name, *arguments):
    return {"rawType": name, "arguments": list(arguments)}


def number(text):
    return probe.JsonNumber(text)


def result(sig, values):
    return {"columns": [{"name": "value", "typeSignature": sig}], "data": [[value] for value in values]}


class ComparisonTests(unittest.TestCase):
    def test_finite_json_floats_require_float_lexical_form_on_both_engines(self):
        floating = signature("double")
        probe.compare_value(number("1.0"), number("1e0"), floating)
        for a, b in ((number("1"), number("1.0")), (number("1.0"), number("1"))):
            with self.subTest(reference=a, candidate=b), self.assertRaisesRegex(probe.ProbeFailure, "integer token"):
                probe.compare_value(a, b, floating)

    def test_nonfinite_float_strings_are_exact_and_not_numeric_extensions(self):
        floating = signature("real")
        for value in ("NaN", "Infinity", "-Infinity"):
            probe.compare_value(value, value, floating)
        with self.assertRaises(probe.ProbeFailure):
            probe.compare_value("Infinity", "-Infinity", floating)
        for raw in ('{"data":[NaN]}', '{"data":[Infinity]}'):
            with self.assertRaisesRegex(probe.ProbeFailure, "non-JSON"):
                probe.parse_page(raw)

    def test_decimal_numbers_compare_without_binary_float_rounding(self):
        sig = signature("decimal", {"kind": "LONG", "value": 38}, {"kind": "LONG", "value": 18})
        probe.compare_value("-0.125000000000000000", "-0.125000000000000000", sig)
        with self.assertRaises(probe.ProbeFailure):
            probe.compare_value("0.000000000000000001", "0.000000000000000002", sig)
        probe.compare_value(number("9007199254740993"), number("9007199254740993.0"), signature("bigint"))
        with self.assertRaises(probe.ProbeFailure):
            probe.compare_value(number("9007199254740993"), number("9007199254740992"), signature("bigint"))

    def test_map_key_bytes_and_nested_floating_values_are_checked(self):
        sig = signature("map", {"kind": "TYPE", "value": signature("varbinary")},
                        {"kind": "TYPE", "value": signature("array", {"kind": "TYPE", "value": signature("double")})})
        reference = {"AAH/": [number("0.125"), None, "NaN"]}
        probe.compare_value(reference, {"AAH/": [number("1.25e-1"), None, "NaN"]}, sig)
        with self.assertRaisesRegex(probe.ProbeFailure, "map keys"):
            probe.compare_value(reference, {"00 01 FF": [number("0.125"), None, "NaN"]}, sig)
        with self.assertRaisesRegex(probe.ProbeFailure, "integer token"):
            probe.compare_value(reference, {"AAH/": [number("0.125"), None, number("1")]}, sig)

    def test_unicode_string_values_compare_exactly_without_normalization(self):
        with self.assertRaises(probe.ProbeFailure):
            probe.compare_value("é", "e\u0301", signature("varchar"))
        with self.assertRaises(probe.ProbeFailure):
            probe.compare_value("雪\n", "雪\\n", signature("varchar"))

    def test_row_shape_is_positional_and_null_is_not_a_string(self):
        sig = signature("row", {"kind": "NAMED_TYPE", "value": {"fieldName": {"name": "x"}, "typeSignature": signature("varchar")}})
        probe.compare_value([None], [None], sig)
        for candidate in (["null"], [], {"x": None}):
            with self.assertRaises(probe.ProbeFailure):
                probe.compare_value([None], candidate, sig)

    def test_variant_generic_values_preserve_json_types_recursively(self):
        sig = signature("variant")
        probe.compare_value({"a": [number("1.25"), True]}, {"a": [number("1.250"), True]}, sig)
        with self.assertRaises(probe.ProbeFailure):
            probe.compare_value({"a": True}, {"a": number("1")}, sig)

    def test_same_signature_mismatch_always_fails_even_with_future_owner(self):
        sig = signature("timestamp")
        with self.assertRaises(probe.ProbeFailure):
            probe.compare_column("ts_zoned", result(sig, ["2024-01-01 00:00:00"]), result(sig, ["2024-01-01 00:00:01"]))

    def test_only_explicit_type_differences_can_enter_g1b(self):
        a, b = result(signature("timestamp"), ["a"]), result(signature("timestamp with time zone"), ["a UTC"])
        difference = probe.compare_column("ts_zoned", a, b)
        self.assertEqual(difference["owner"], "TRC-6c")
        with self.assertRaisesRegex(probe.ProbeFailure, "unregistered"):
            probe.compare_column("text_value", a, b)
        with self.assertRaisesRegex(probe.ProbeFailure, "unregistered"):
            probe.compare_column("binary_value", a, {"error": {"errorName": "NOT_SUPPORTED"}})

    def test_oracle_failure_cannot_be_registered_as_a_candidate_difference(self):
        with self.assertRaisesRegex(probe.ProbeFailure, "oracle rejected"):
            probe.compare_column("ts_zoned", {"error": {"errorName": "REMOTE_TASK_ERROR"}}, result(signature("timestamp"), ["a"]))


if __name__ == "__main__":
    unittest.main()
