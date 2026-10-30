# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""Writer startup hooks are explicit and preserve the admitted budget."""
import importlib.util
import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "docker/iceberg-rest"))
import fixture_run
from scratch import ScratchIdentity


def load(name, relative):
    spec = importlib.util.spec_from_file_location(name, REPO / relative)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


iceberg = load("startup_uea4a4_iceberg", "tests/benchmarks/uea4a4/iceberg_fixture.py")
hms = load("startup_hms_fixture", "tests/sql/fixtures/iceberg-hms/fixture.py")


class WriterStartupTests(unittest.TestCase):
    def test_shared_iceberg_namespace_has_finite_suffix_and_cleanup_defers_files(self):
        identity = ScratchIdentity.create("s3://novarocks/worktree")
        prefix = identity.scope_root.removeprefix("s3://novarocks/") + "/uea4a4"
        self.assertEqual(iceberg.database_for(prefix), identity.run_id + "_a4")
        with patch.object(iceberg.support, "compose_mc") as native:
            iceberg.remove_prefix(SimpleNamespace(run_budget=object()),"s3://novarocks/" + prefix + "/table")
        native.assert_not_called()

    def test_standalone_iceberg_cleanup_retains_full_run_report_after_writer_exit(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ,{},clear=True):
            root=Path(directory); (root/"READY").touch()
            identity=ScratchIdentity.create("s3://novarocks/worktree")
            location=identity.scope_root+"/uea4a4/table"
            manifest={"run_id":"label","table_location":location,"database":identity.run_id+"_a4","owns_fixture_run":True}
            runtime=SimpleNamespace(run_budget=object(),env_file=root/"env.sh")
            args=SimpleNamespace(output_dir=str(root),env_file=str(runtime.env_file),fixture_store="unused",run_id="label")
            for complete in (True,False):
                (root/"READY").touch()
                report={"complete":complete,"catalog_barrier":{"complete":complete},"errors":[]}
                exited=[]
                def clean(*args,**kwargs):
                    self.assertEqual(exited,[True],"cleanup must follow actual Spark wrapper return")
                    return report
                with patch.object(iceberg.support.governed_run,"restore_context",return_value=True), \
                        patch.object(iceberg.support,"load_runtime",return_value=runtime), \
                        patch.object(iceberg,"install_verified_images"),patch.object(iceberg,"verify_ready",return_value=manifest), \
                        patch.object(iceberg,"spark_build",side_effect=lambda *args:exited.append(True)), \
                        patch.object(iceberg.support.governed_run,"cleanup_run",side_effect=clean), \
                        patch.object(iceberg,"remove_prefix") as remove:
                    if complete:
                        self.assertEqual(iceberg.cleanup(args),0)
                    else:
                        with self.assertRaisesRegex(iceberg.support.FixtureError,"catalog barrier"):
                            iceberg.cleanup(args)
                self.assertEqual(json.loads((root/"cleanup-report.json").read_text()),report)
                remove.assert_not_called()
                if not complete: self.assertTrue((root/"READY").exists())

    def test_iceberg_rejected_inputs_cannot_reach_running_image_inspection(self):
        with patch.object(iceberg.support, "run_command", side_effect=iceberg.support.FixtureError("rejected input")) as command, \
                patch.object(iceberg.support, "read_json") as read:
            with self.assertRaisesRegex(iceberg.support.FixtureError, "rejected input"):
                iceberg.install_verified_images(Path("/unused/store"), object())
            command.assert_called_once()
            self.assertEqual(command.call_args.args[0][-2:], ["--consumer", "iceberg-rest"])
            read.assert_not_called()

    def test_iceberg_consumer_still_rejects_a_different_running_spark_image(self):
        bom = {"images": {},
               "derived_images": {"minio-mc": {"image_id": "sha256:verified-mc"}, "iceberg-spark": {"image_id": "sha256:expected"}},
               "lock_sha256": "locked"}
        publication = {"shared_docker": True,
                       "runtime": {"machine": {"containers": {"spark": "exact-spark"}}}}
        runtime = SimpleNamespace(env_file=Path("/unused/publication/env.sh"))
        with patch.dict(os.environ, {}, clear=True), \
                patch.object(iceberg.support, "read_json", side_effect=[bom, publication]), \
                patch.object(iceberg.support, "run_command", side_effect=[
                    SimpleNamespace(stdout="verified"), SimpleNamespace(stdout="sha256:foreign\n")]) as command:
            with self.assertRaisesRegex(iceberg.support.FixtureError, "running Spark container differs"):
                iceberg.install_verified_images(Path("/unused/store"), runtime)
            self.assertEqual(command.call_args_list[0].args[0][-2:], ["--consumer", "iceberg-rest"])
            self.assertEqual(command.call_args_list[1].args[0],
                             ["docker", "inspect", "exact-spark", "--format", "{{.Image}}"])

    def test_iceberg_prepare_passes_original_budget_before_output_creation(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {}, clear=True):
            root = Path(directory)
            output = root / "output"
            budget = object()
            runtime = SimpleNamespace(env_id="worktree", run_budget=budget)
            args = SimpleNamespace(output_dir=str(output), env_file=str(root / "env.sh"),
                files_per_table=24, run_id="new-run", dry_run=False)
            def sweep(env_file, *, budget):
                self.assertIs(budget, runtime.run_budget)
                self.assertEqual(env_file, (root / "env.sh").resolve())
                self.assertFalse(output.exists())
                raise RuntimeError("native sweep failed")
            with patch.object(iceberg.support, "parse_env_file", return_value={
                    "NOVAROCKS_ICEBERG_REST_URI":"saved", "NOVAROCKS_ICEBERG_REST_WAREHOUSE":"saved"}), \
                    patch.object(iceberg.support, "load_runtime", return_value=runtime), \
                    patch.object(iceberg.support.governed_run, "startup_sweep", side_effect=sweep) as called:
                with self.assertRaisesRegex(RuntimeError, "native sweep failed"):
                    iceberg.prepare(args)
                called.assert_called_once()
            self.assertFalse(output.exists())

    def test_iceberg_existing_ready_is_read_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "READY").touch()
            args = SimpleNamespace(output_dir=str(root), env_file=str(root / "env.sh"),
                files_per_table=24, run_id="same-run", dry_run=False)
            with patch.object(iceberg.support, "parse_env_file", return_value={}), \
                    patch.object(iceberg.support, "load_runtime", return_value=object()), \
                    patch.object(iceberg, "verify_ready", return_value={"run_id":"same-run",
                        "definition_sha256":iceberg.definition_sha256(24)}), \
                    patch.object(iceberg.support.governed_run, "startup_sweep") as sweep:
                self.assertEqual(iceberg.prepare(args), 0)
                sweep.assert_not_called()

    def test_hms_prepare_sweeps_original_budget_before_evidence_or_spark(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            identity = ScratchIdentity.create("s3://novarocks/worktree")
            budget = SimpleNamespace(identity=identity, as_dict=lambda: {"identity":identity.as_dict()},
                                     remaining_seconds=lambda **kwargs: 3600)
            environment = {"NOVA_ENV_REST_ENV_FILE":str(root / "env.sh"),
                "NOVAROCKS_WORKSPACE_ROOT":str(root), "NOVAROCKS_ICEBERG_HMS_WAREHOUSE":"s3://warehouse/old"}
            def sweep(env_file, *, budget):
                self.assertIs(budget.identity, identity)
                self.assertEqual(env_file, root / "env.sh")
                self.assertFalse((root / "tests/sql/.runtime/hms-evidence").exists())
                raise RuntimeError("native sweep failed")
            with patch.dict(os.environ, environment, clear=True), \
                    patch.object(sys, "argv", ["fixture.py", "prepare", identity.resource_name("hms"),
                                               "--new-namespace", identity.resource_name("new")]), \
                    patch.object(fixture_run, "admit", return_value=({}, {}, budget)), \
                    patch.object(fixture_run, "startup_sweep", side_effect=sweep) as called, \
                    patch.object(hms.signal, "signal"), patch.object(hms.signal, "setitimer"), \
                    patch.object(hms, "run_spark") as spark:
                with self.assertRaisesRegex(RuntimeError, "native sweep failed"):
                    hms.main()
                called.assert_called_once()
                spark.assert_not_called()

    def test_hms_snapshot_does_not_sweep(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "NOVA_ENV_REST_ENV_FILE":str(Path(directory) / "env.sh"),
                "NOVAROCKS_WORKSPACE_ROOT":directory}, clear=True), \
                patch.object(sys, "argv", ["fixture.py", "before", "iru7_readonly"]), \
                patch.object(fixture_run, "admit", return_value=({}, {}, None)), \
                patch.object(fixture_run, "startup_sweep") as sweep, \
                patch.object(hms, "run_spark", return_value={"metadata_location":["v1", "v2", "v3"]}), \
                patch.object(hms, "inventory", return_value=(["a", "b", "c"], 6)):
            hms.main()
            sweep.assert_not_called()


class HmsNamespaceFamilyTests(unittest.TestCase):
    def test_shared_modes_pass_the_original_pair_and_original_budget(self):
        for action in ["prepare", "before", "after", "cleanup"]:
            with self.subTest(action=action), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                identity = ScratchIdentity.create("s3://custom-bucket/worktree")
                namespace, sibling = identity.resource_name("h0000"), identity.resource_name("h0001")
                remaining_calls = []
                def remaining_seconds(*, work):
                    remaining_calls.append(work)
                    return 17 if work else 23
                original = {"identity": identity.as_dict(), "deadline": 123456.75}
                budget = SimpleNamespace(identity=identity, as_dict=lambda: original,
                                         remaining_seconds=remaining_seconds)
                catalog = {"metadata_location": ["v1", "v2", "v3"]}
                objects = {f"s3://custom-bucket/owned-{i}": {"size": 1} for i in range(3)}
                snapshot = {"namespace_family": [namespace, sibling], "catalog": catalog,
                            "objects": objects}
                evidence = root / "tests/sql/.runtime/hms-evidence" / namespace
                if action == "after":
                    evidence.mkdir(parents=True)
                    (evidence / "before.json").write_text(json.dumps(snapshot))
                environment = {"NOVA_ENV_REST_ENV_FILE": str(root / "env.sh"),
                               "NOVAROCKS_WORKSPACE_ROOT": directory,
                               "NOVAROCKS_ICEBERG_HMS_WAREHOUSE": "s3://warehouse/original",
                               "NOVA_FIXTURE_RUN_CONTEXT": json.dumps(original)}
                inventories = [({}, 9), ({}, 9)] if action == "cleanup" else [(objects, 9)]
                with patch.dict(os.environ, environment, clear=True), \
                        patch.object(sys, "argv", ["fixture.py", action, namespace,
                                                  "--new-namespace", sibling]), \
                        patch.object(fixture_run, "admit", return_value=({}, {}, budget)) as admit, \
                        patch.object(fixture_run, "startup_sweep") as sweep, \
                        patch.object(hms.signal, "signal"), patch.object(hms.signal, "setitimer") as timer, \
                        patch.object(hms, "run_spark", return_value=catalog) as spark, \
                        patch.object(hms, "inventory", side_effect=inventories) as inventory:
                    hms.main()
                    self.assertEqual(json.loads(os.environ["NOVA_FIXTURE_RUN_CONTEXT"]), original)
                    self.assertEqual(os.environ["NOVAROCKS_ICEBERG_HMS_WAREHOUSE"],
                                     identity.scope_root + "/hms")
                    if action == "cleanup":
                        self.assertEqual(os.environ["NOVA_FIXTURE_CLEANUP_MODE"], "true")
                    admit.assert_called_once_with(root / "env.sh")
                spark.assert_called_once_with(root, "snapshot" if action in ["before", "after"] else action,
                                              namespace, new_namespace=sibling)
                for call in inventory.call_args_list:
                    self.assertEqual(call.args, (namespace,))
                    self.assertEqual(call.kwargs, {"new_namespace": sibling})
                self.assertEqual(remaining_calls, [action != "cleanup"])
                timer.assert_called_once_with(hms.signal.ITIMER_REAL, 23 if action == "cleanup" else 17)
                if action == "prepare":
                    sweep.assert_called_once_with(root / "env.sh", budget=budget)
                else:
                    sweep.assert_not_called()

    def test_shared_rejects_unbound_ambiguous_or_foreign_pairs_before_side_effects(self):
        identity = ScratchIdentity.create("s3://novarocks/worktree")
        namespace = identity.resource_name("h0000")
        foreign = ScratchIdentity.create("s3://novarocks/worktree").resource_name("h0001")
        for action in ["prepare", "before", "after", "cleanup"]:
            for sibling in [None, namespace, foreign, identity.run_id + "_UPPER",
                            identity.run_id + "_abcdef", identity.run_id + "_%"]:
                with self.subTest(action=action, sibling=sibling), tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    argv = ["fixture.py", action, namespace]
                    if sibling is not None:
                        argv += ["--new-namespace", sibling]
                    budget = SimpleNamespace(identity=identity)
                    with patch.dict(os.environ, {"NOVA_ENV_REST_ENV_FILE": str(root / "env.sh"),
                                                "NOVAROCKS_WORKSPACE_ROOT": directory}, clear=True), \
                            patch.object(sys, "argv", argv), \
                            patch.object(fixture_run, "admit", return_value=({}, {}, budget)), \
                            patch.object(fixture_run, "startup_sweep") as sweep, \
                            patch.object(hms, "run_spark") as spark, \
                            patch.object(hms, "inventory") as inventory, \
                            contextlib.redirect_stderr(io.StringIO()):
                        with self.assertRaises(SystemExit) as error:
                            hms.main()
                        self.assertEqual(error.exception.code, 2)
                    self.assertFalse((root / "tests/sql/.runtime/hms-evidence").exists())
                    sweep.assert_not_called()
                    spark.assert_not_called()
                    inventory.assert_not_called()

    def test_private_default_pair_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "NOVA_ENV_REST_ENV_FILE": str(Path(directory) / "env.sh"),
                "NOVAROCKS_WORKSPACE_ROOT": directory}, clear=True), \
                patch.object(sys, "argv", ["fixture.py", "prepare", "iru7_private"]), \
                patch.object(fixture_run, "admit", return_value=({}, {}, None)), \
                patch.object(fixture_run, "startup_sweep") as sweep, \
                patch.object(hms, "run_spark") as spark:
            hms.main()
            spark.assert_called_once_with(Path(directory), "prepare", "iru7_private",
                                          new_namespace="iru7_private_new")
            sweep.assert_not_called()

    def test_private_explicit_canonical_pair_preserves_private_properties(self):
        identity = ScratchIdentity.create("s3://custom-bucket/private")
        namespace, sibling = identity.resource_name("h0000"), identity.resource_name("h0001")
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "NOVA_ENV_REST_ENV_FILE": str(Path(directory) / "env.sh"),
                "NOVAROCKS_WORKSPACE_ROOT": directory, "NOVA_FIXTURE_RUN_CONTEXT": "null",
                "NOVAROCKS_ICEBERG_HMS_WAREHOUSE": "s3://private-bucket/original"}, clear=True), \
                patch.object(sys, "argv", ["fixture.py", "prepare", namespace,
                                          "--new-namespace", sibling]), \
                patch.object(fixture_run, "admit", return_value=({}, {}, None)), \
                patch.object(fixture_run, "startup_sweep") as sweep, \
                patch.object(hms.signal, "signal") as signal, \
                patch.object(hms.signal, "setitimer") as timer, \
                patch.object(hms, "run_spark") as spark:
            hms.main()
            spark.assert_called_once_with(Path(directory), "prepare", namespace, new_namespace=sibling)
            self.assertEqual(os.environ["NOVAROCKS_ICEBERG_HMS_WAREHOUSE"], "s3://private-bucket/original")
            self.assertEqual(os.environ["NOVA_FIXTURE_RUN_CONTEXT"], "null")
            self.assertNotIn("NOVA_FIXTURE_HMS_ORIGINAL_WAREHOUSE", os.environ)
            self.assertNotIn("NOVA_FIXTURE_CLEANUP_MODE", os.environ)
            sweep.assert_not_called()
            signal.assert_not_called()
            timer.assert_not_called()

    def test_private_explicit_pair_rejects_foreign_or_invalid_run_before_spark(self):
        identity = ScratchIdentity.create("s3://novarocks/private")
        namespace = identity.resource_name("h0000")
        foreign = ScratchIdentity.create("s3://novarocks/private").resource_name("h0001")
        for sibling in [foreign, namespace, identity.run_id + "_abcdef", "iru7_other"]:
            with self.subTest(sibling=sibling), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                with patch.dict(os.environ, {"NOVA_ENV_REST_ENV_FILE": str(root / "env.sh"),
                                            "NOVAROCKS_WORKSPACE_ROOT": directory}, clear=True), \
                        patch.object(sys, "argv", ["fixture.py", "prepare", namespace,
                                                  "--new-namespace", sibling]), \
                        patch.object(fixture_run, "admit", return_value=({}, {}, None)), \
                        patch.object(fixture_run, "startup_sweep") as sweep, \
                        patch.object(hms, "run_spark") as spark, \
                        contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as error:
                        hms.main()
                    self.assertEqual(error.exception.code, 2)
                self.assertFalse((root / "tests/sql/.runtime/hms-evidence").exists())
                sweep.assert_not_called()
                spark.assert_not_called()

    def test_inventory_uses_only_exact_pair_in_both_warehouses(self):
        identity = ScratchIdentity.create("s3://custom-bucket/worktree")
        namespace, sibling = identity.resource_name("h0000"), identity.resource_name("h0001")
        calls = []
        def request(bucket, parameters):
            calls.append((bucket, parameters["prefix"]))
            return ET.fromstring('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                                 '<IsTruncated>false</IsTruncated></ListBucketResult>')
        with patch.dict(os.environ, {"NOVAROCKS_ICEBERG_HMS_WAREHOUSE": "s3://custom-bucket/run/hms",
                                    "NOVA_FIXTURE_HMS_ORIGINAL_WAREHOUSE": "s3://warehouse/original"}, clear=True), \
                patch.object(hms, "s3_request", side_effect=request):
            self.assertEqual(hms.inventory(namespace, new_namespace=sibling), ({}, 9))
        expected = {("custom-bucket", "run/hms/iru7/" + namespace + "/")}
        expected |= {(bucket, base + "/" + name + suffix)
                     for bucket, base in [("custom-bucket", "run/hms"), ("warehouse", "original")]
                     for name in [namespace, sibling] for suffix in ["/", ".db/"]}
        self.assertEqual(set(calls), expected)
        self.assertEqual(len(calls), len(expected))
        self.assertFalse(any(namespace + "_new" in prefix for _, prefix in calls))

    def test_inventory_rejects_an_object_outside_the_exact_sibling_prefix(self):
        response = ET.fromstring('<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                                 '<Contents><Key>hms/unrelated/object</Key><ETag>etag</ETag>'
                                 '<Size>1</Size></Contents><IsTruncated>false</IsTruncated></ListBucketResult>')
        with patch.dict(os.environ, {"NOVAROCKS_ICEBERG_HMS_WAREHOUSE": "s3://bucket/hms"}, clear=True), \
                patch.object(hms, "s3_request", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "invalid or duplicate key"):
                hms.inventory("base", new_namespace="sibling")

    def test_after_rejects_a_changed_family_even_when_catalog_and_objects_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence = root / "tests/sql/.runtime/hms-evidence/iru7_private"
            evidence.mkdir(parents=True)
            (evidence / "before.json").write_text(json.dumps({
                "namespace_family": ["iru7_private", "iru7_original"], "catalog": {}, "objects": {}}))
            with patch.dict(os.environ, {"NOVA_ENV_REST_ENV_FILE": str(root / "env.sh"),
                                        "NOVAROCKS_WORKSPACE_ROOT": directory}, clear=True), \
                    patch.object(sys, "argv", ["fixture.py", "after", "iru7_private",
                                              "--new-namespace", "iru7_changed"]), \
                    patch.object(fixture_run, "admit", return_value=({}, {}, None)), \
                    patch.object(hms, "run_spark", return_value={}), \
                    patch.object(hms, "inventory", return_value=({}, 9)):
                with self.assertRaisesRegex(RuntimeError, "inventory changed"):
                    hms.main()

    def test_spark_snapshot_and_cleanup_preserve_only_exact_family(self):
        namespace, sibling = "base_namespace", "independent_sibling"
        for action in ["snapshot", "cleanup"]:
            queries = []
            class Spark:
                def sql(self, query):
                    queries.append(query)
                    if query == "SHOW NAMESPACES IN hms_catalog":
                        rows = [(namespace,), (sibling,), (namespace + "_new",), (sibling + "extra",)]
                    else:
                        rows = []
                    return SimpleNamespace(collect=lambda: rows)
                def stop(self):
                    queries.append("STOP")
            spark = Spark()
            module = SimpleNamespace(SparkSession=SimpleNamespace(
                builder=SimpleNamespace(getOrCreate=lambda: spark)))
            with patch.dict(sys.modules, {"pyspark": SimpleNamespace(), "pyspark.sql": module}), \
                    patch.object(sys, "argv", ["fixture.py", action, namespace, sibling,
                                              "s3://custom-bucket/run/hms"]), \
                    contextlib.redirect_stdout(io.StringIO()) as output:
                exec(compile(hms.SPARK_PROGRAM, "fake-hms-spark", "exec"), {})
            facts = json.loads(output.getvalue().removeprefix("IRU7_FACTS="))
            if action == "snapshot":
                self.assertEqual(facts["namespaces"], sorted([namespace, sibling]))
            else:
                self.assertEqual([q for q in queries if q.startswith("DROP NAMESPACE")],
                                 ["DROP NAMESPACE hms_catalog." + namespace,
                                  "DROP NAMESPACE hms_catalog." + sibling])
            self.assertFalse(any(namespace + "_new" in query or sibling + "extra" in query
                                 for query in queries))

    def test_run_spark_passes_explicit_family_to_actual_submit_argv(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            defaults = root / "defaults.conf"
            defaults.write_text("original-defaults")
            env = {key: "saved" for key in ["NOVA_ENV_COMPOSE_ENV", "NOVA_ENV_COMPOSE_PROJECT",
                                          "NOVA_ENV_COMPOSE_FILE", "NOVA_ENV_REST_ENV_FILE"]}
            env.update({"NOVAROCKS_SPARK_DEFAULTS": str(defaults),
                        "NOVAROCKS_SPARK_HMS_DEFAULTS": str(defaults),
                        "NOVAROCKS_ICEBERG_HMS_WAREHOUSE": "s3://custom-bucket/run/hms"})
            def run(argv, **kwargs):
                return SimpleNamespace(returncode=0, stdout='IRU7_FACTS={"ok":true}\n', stderr="")
            with patch.dict(os.environ, env, clear=True), \
                    patch.object(hms.subprocess, "check_output", return_value="/tmp/owned\n"), \
                    patch.object(hms.subprocess, "run", side_effect=run) as called, \
                    patch.object(hms.signal, "setitimer", return_value=(0, 0)):
                self.assertEqual(hms.run_spark(root, "snapshot", "base", new_namespace="explicit"), {"ok": True})
            submit = [call.args[0] for call in called.call_args_list
                      if "/opt/spark/bin/spark-submit" in call.args[0]]
            self.assertEqual(len(submit), 1)
            self.assertEqual(submit[0][-4:], ["snapshot", "base", "explicit", "s3://custom-bucket/run/hms"])


if __name__ == "__main__":
    unittest.main()
