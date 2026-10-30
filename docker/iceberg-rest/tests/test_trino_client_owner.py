# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""Client intent uses an exact process and image-local programs, never guessed PIDs."""
import sys
from pathlib import Path
from unittest.mock import patch
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import trino_client_owner as owner
import fixture_runtime


class ClientIntentTests(unittest.TestCase):
    def test_cli_file_and_literal_arguments_keep_image_local_program(self):
        with patch.object(owner, "run", return_value="result") as run:
            result = owner.cli("fixture.env", "exact-image", "evidence", ["--probe-sql-file", "/original/session.sql", "--execute", "SELECT '$(literal)'"], "original-metrics")
        self.assertEqual(result, "result")
        args, kwargs = run.call_args
        self.assertEqual(args[:3], ("fixture.env", "exact-image", "evidence"))
        command = args[3]
        self.assertEqual(command[:2], ["/bin/sh", "-c"])
        self.assertIn('exec /usr/bin/trino "$@"', command[2])
        self.assertIn('/tmp/trc1-cli.pid', command[2])
        self.assertEqual(command[-2:], ["--execute", "SELECT '$(literal)'"])
        self.assertEqual(kwargs["mounts"], [("/original/session.sql", "/probe/session.sql")])
        self.assertIsNone(kwargs["interrupt_metrics"])

    def test_interrupt_requires_original_metrics_before_any_child_side_effect(self):
        with patch.object(owner, "run") as run:
            with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "original FE metrics"):
                owner.cli("fixture.env", "exact-image", "evidence", ["--probe-interrupt", "--execute", "SELECT 1"], None)
            run.assert_not_called()
        with patch.object(owner, "run") as run:
            owner.cli("fixture.env", "exact-image", "evidence", ["--probe-interrupt", "--execute", "SELECT 1"], "original-metrics")
        self.assertEqual(run.call_args.kwargs["interrupt_metrics"], "original-metrics")
        self.assertNotIn("--probe-interrupt", run.call_args.args[3])

    def test_jdbc_uses_only_recorded_image_driver_and_readonly_source(self):
        with patch.object(owner, "run") as run:
            owner.jdbc("fixture.env", "exact-image", "evidence", "/source/JdbcProbe.java", "/usr/lib/trino/trino-jdbc-483.jar", ["http://127.0.0.1:8080", "root"])
        self.assertEqual(run.call_args.args[3], ["java", "--class-path", "/usr/lib/trino/trino-jdbc-483.jar", "/probe/JdbcProbe.java", "http://127.0.0.1:8080", "root"])
        self.assertEqual(run.call_args.kwargs["mounts"], [("/source/JdbcProbe.java", "/probe/JdbcProbe.java")])


if __name__ == "__main__":
    unittest.main()
