# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""Runtime tooling checks; suspended Paimon product suites stay suspended."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / "fixture.py"
spec = importlib.util.spec_from_file_location("paimon_runtime_binding", path)
fixture = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = fixture
spec.loader.exec_module(fixture)


class RuntimeBindingTests(unittest.TestCase):
    def test_shared_cleanup_retains_objects_for_original_run_barrier(self):
        runtime = SimpleNamespace(run_budget=object())
        with patch.object(fixture, "cleanup_prefix") as cleanup, patch.object(fixture,"collect_objects") as objects:
            self.assertFalse(fixture.cleanup_and_assert_empty(runtime,"s3://novarocks/worktree/scratch/original/paimon"))
        cleanup.assert_not_called(); objects.assert_not_called()

    def test_standalone_cleanup_uses_original_run_barrier_and_retains_report(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ,{},clear=True):
            report_path = Path(directory)/"cleanup-report.json"
            budget = object()
            runtime = SimpleNamespace(run_budget=budget, owns_fixture_run=True,env_file=Path(directory)/"env.sh")
            for complete in (True,False):
                report={"complete":complete,"catalog_barrier":{"complete":complete},"errors":[]}
                with patch.object(fixture.governed_run,"cleanup_run",return_value=report) as cleanup:
                    if complete:
                        self.assertTrue(fixture.cleanup_and_assert_empty(runtime,"s3://owned/paimon",report_path=report_path))
                    else:
                        with self.assertRaisesRegex(fixture.FixtureError,"catalog barrier"):
                            fixture.cleanup_and_assert_empty(runtime,"s3://owned/paimon",report_path=report_path)
                cleanup.assert_called_once_with(runtime.env_file,budget=budget)
                self.assertEqual(json.loads(report_path.read_text()), report)
            self.assertNotIn("NOVA_FIXTURE_CLEANUP_MODE",os.environ)

    def test_ready_prepare_is_read_only_without_rebinding_saved_scope(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root=Path(directory); output=root/"output"; output.mkdir()
            saved={"run_id":"human-label","fixture_definition_sha256":"definition",
                   "fixture_input_lock_sha256":"input-lock","last_stage":fixture.STAGES[0],
                   "warehouse_uri":"s3://novarocks/worktree/scratch/nr_scratch_old/paimon-read"}
            (output/"manifest.json").write_text(json.dumps(saved)); (output/"READY").touch()
            args=SimpleNamespace(output_dir=str(output),env_file=str(root/"env.sh"),dry_run=False,
                                 stop_after=fixture.STAGES[0],fixture_store="unused",run_id="human-label")
            runtime=object()
            with patch.object(fixture,"load_versions",return_value={}), \
                    patch.object(fixture,"fixture_definition_sha256",return_value="definition"), \
                    patch.object(fixture,"parse_env_file",return_value={"NOVA_ENV_ID":"worktree"}), \
                    patch.object(fixture,"load_runtime",return_value=runtime), \
                    patch.object(fixture,"prepare_writer_image",side_effect=AssertionError("existing READY must not prepare images")), \
                    patch.object(fixture,"verify_ready",return_value=saved), \
                    patch.object(fixture,"verify_remote_inventory") as inventory, \
                    patch.object(fixture,"run_spark_stage") as writer, \
                    patch.object(fixture,"cleanup_and_assert_empty") as cleanup, \
                    patch.object(fixture.governed_run,"startup_sweep") as sweep:
                self.assertEqual(fixture.execute_prepare(args),0)
            inventory.assert_called_once_with(output.resolve(),runtime,saved["warehouse_uri"])
            writer.assert_not_called(); cleanup.assert_not_called()
            sweep.assert_not_called()
            self.assertEqual(json.loads((output/"manifest.json").read_text()),saved)

    def test_partial_prepare_restores_context_before_any_runtime_or_writer(self):
        original={"schema":1,"identity":{"run_id":"original"},"deadline_unix_ms":1}
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root=Path(directory); output=root/"output"; output.mkdir()
            (output/"state.json").write_text(json.dumps({"run_context":original,"last_stage":None}))
            args=SimpleNamespace(output_dir=str(output),env_file=str(root/"env.sh"),dry_run=False,
                                 stop_after=fixture.STAGES[0],run_id="human-label")
            def rejected_runtime(path):
                self.assertEqual(json.loads(os.environ["NOVA_FIXTURE_RUN_CONTEXT"]),original)
                raise fixture.FixtureError("original shared fixture deadline expired")
            with patch.object(fixture,"load_versions",return_value={}), \
                    patch.object(fixture,"fixture_definition_sha256",return_value="definition"), \
                    patch.object(fixture,"parse_env_file",return_value={"NOVA_ENV_ID":"worktree"}), \
                    patch.object(fixture,"load_runtime",side_effect=rejected_runtime), \
                    patch.object(fixture,"run_spark_stage") as writer:
                with self.assertRaisesRegex(fixture.FixtureError,"original.*expired"):
                    fixture.execute_prepare(args)
                writer.assert_not_called()

    def test_new_shared_prepare_sweeps_with_original_budget_before_creating_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "not-created"
            original = object()
            runtime = SimpleNamespace(run_budget=original, env_file=root / "env.sh")
            args = SimpleNamespace(output_dir=str(output), env_file=str(runtime.env_file), dry_run=False,
                                   stop_after=fixture.STAGES[0], run_id="human-label")
            def sweep(env_file, *, budget):
                self.assertEqual(env_file, runtime.env_file)
                self.assertIs(budget, original)
                self.assertFalse(output.exists())
                raise fixture.FixtureError("native sweep failure")
            with patch.object(fixture, "load_runtime", return_value=runtime), \
                    patch.object(fixture.governed_run, "startup_sweep", side_effect=sweep) as called, \
                    patch.object(fixture, "run_spark_stage") as writer:
                with self.assertRaisesRegex(fixture.FixtureError, "native sweep failure"):
                    fixture.execute_prepare(args)
                called.assert_called_once()
                writer.assert_not_called()
            self.assertFalse(output.exists())

    def test_shared_scope_uses_the_original_scratch_identity(self):
        identity = fixture.ScratchIdentity.create("s3://novarocks/worktree")
        with patch.dict(os.environ, {"NOVA_FIXTURE_RUN_CONTEXT": json.dumps({"identity": identity.as_dict()})}):
            scope = fixture.make_scope("caller-label", "worktree", "definition")
        self.assertEqual(scope.warehouse_uri, identity.scope_root + "/paimon-read")
        self.assertEqual(fixture.validate_warehouse_uri(scope.warehouse_uri),
                         ("novarocks", scope.prefix))

    def test_private_scope_remains_an_exact_run_owned_prefix(self):
        with patch.dict(os.environ, {}, clear=True):
            first = fixture.make_scope("run-one", "worktree", "definition")
            second = fixture.make_scope("run-two", "worktree", "definition")
        self.assertNotEqual(first.warehouse_uri, second.warehouse_uri)
        self.assertEqual(fixture.validate_warehouse_uri(first.warehouse_uri),
                         ("novarocks", first.prefix))

    def test_path_traversal_and_benchmark_locations_remain_rejected(self):
        for uri in ("s3://novarocks/shared/benchmarks", "s3://novarocks/worktree/scratch/../paimon-read"):
            with self.subTest(uri=uri), self.assertRaises(fixture.FixtureError):
                fixture.validate_warehouse_uri(uri)

    def test_expired_original_budget_does_not_dispatch_a_host_command(self):
        context = {"deadline_unix_ms": 1, "cleanup_grace_seconds": 300}
        with patch.dict(os.environ, {"NOVA_FIXTURE_RUN_CONTEXT": json.dumps(context)}), \
                patch.object(subprocess, "run") as run:
            with self.assertRaises(fixture.FixtureError):
                fixture.run_command(["docker", "anything"])
        run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
