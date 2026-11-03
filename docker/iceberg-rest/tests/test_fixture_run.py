# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
import json
import hashlib
import contextlib
import io
from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_run
import java_rest_inputs
import fixture_runtime


class RunTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.env = self.root / "env.sh"
        self.manifest = {"ready": True, "runtime": {"machine": {"id":"exact-machine","control_root":"/owned/machine","daemon_id":"exact-daemon","version":"exact-version","generation":"exact-generation", "config":{"minio_volume":"exact-volume"},"server_warehouses":{"rest":"s3://warehouse/machine/rest","rest-mv":"s3://warehouse/machine/rest-mv"},
            "containers": {"spark": "exact-spark"},
            "policy": {"max_run_seconds": 86400, "cleanup_grace_seconds": 300,
                       "scratch_ttl_seconds": 604800}}}}
        self.write_manifest()
        self.env.write_text("export NOVA_ENV_ID='worktree-id'\n")

    def write_manifest(self):
        (self.root / "manifest.json").write_text(json.dumps(self.manifest))

    def sweep_input(self):
        self.manifest["runtime"]["machine"].update(control_root=str(self.root/"machine"), daemon_id="exact-daemon", version="exact-version", generation="exact-generation")
        self.write_manifest()
        return fixture_run.admit(self.env)[2]

    def test_communicate_preserves_capture_and_redirects_real_child_with_original_cwd(self):
        command = [sys.executable, '-c',
                   'import os,sys;print(os.getcwd());print("child-error",file=sys.stderr)']
        captured = fixture_run.communicate(command, timeout=5, capture=True, cwd=self.root)
        self.assertEqual(captured.returncode, 0)
        self.assertEqual(captured.stdout.decode().strip(), str(self.root.resolve()))
        self.assertEqual(captured.stderr.decode().strip(), 'child-error')
        with (self.root / 'child.log').open('wb') as output:
            redirected = fixture_run.communicate(command, timeout=5, cwd=self.root,
                stdout=output, stderr=subprocess.STDOUT)
        self.assertEqual(redirected.returncode, 0)
        self.assertEqual(set((self.root / 'child.log').read_text().splitlines()),
                         {str(self.root.resolve()), 'child-error'})
        with patch.object(fixture_run.subprocess, 'Popen') as process:
            with self.assertRaisesRegex(ValueError, 'mutually exclusive'):
                fixture_run.communicate(command, timeout=5, capture=True, stdout=subprocess.PIPE)
            process.assert_not_called()

    def java_publication(self):
        repo = self.root.resolve() / 'repo'
        publication = repo / 'docker/iceberg-rest/runtime/current/published'
        publication.mkdir(parents=True)
        bom = self.root / 'bom.json'
        bom.write_text(json.dumps({'lock_sha256':'exact-lock'}))
        manifest = dict(self.manifest, fixture_inputs={'verified':True, 'bom':str(bom),
                                                     'lock_sha256':'exact-lock'})
        (publication / 'manifest.json').write_text(json.dumps(manifest))
        values = dict(NOVA_ENV_ID='worktree-id', NOVA_ENV_REST_ENV_FILE=str(publication/'env.sh'),
            NOVA_ENV_MANIFEST=str(publication/'manifest.json'), NOVA_ENV_RUNTIME_DIR=str(self.root),
            NOVAROCKS_ICEBERG_REST_URI='http://fixture.invalid',
            NOVAROCKS_ICEBERG_REST_WAREHOUSE='s3://fixture/rest', AWS_S3_ENDPOINT='http://s3.invalid',
            AWS_S3_ACCESS_KEY_ID='fixture-key', AWS_S3_SECRET_ACCESS_KEY='fixture-secret')
        import shlex
        (publication/'env.sh').write_text('unset AWS_SESSION_TOKEN\n' +
            ''.join('export '+key+'='+shlex.quote(value)+'\n' for key,value in values.items()))
        return repo, publication

    def test_java_owner_inherits_same_budget_storage_without_cleanup_rights(self):
        repo, publication = self.java_publication()
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(publication/'env.sh')[2].as_dict()
            storage = json.loads(os.environ['NOVA_FIXTURE_RUN_STORAGE'])
            inherited = dict(os.environ, AWS_SESSION_TOKEN='foreign')
            root = self.root.resolve()/'inherited-artifacts'
            def sweep(*args, **kwargs):
                self.assertFalse(root.exists())
                self.assertEqual(kwargs['budget'].as_dict(), original)
            with patch.object(java_rest_inputs.subprocess, 'run', return_value=subprocess.CompletedProcess([],0)), \
                    patch.object(fixture_run, 'startup_sweep', side_effect=sweep), \
                    patch.object(fixture_run, 'cleanup_run') as cleanup:
                with java_rest_inputs.own_inputs(repo, root, inherited) as owner:
                    self.assertFalse(owner.owns_run)
                    self.assertEqual(json.loads(owner.environment['NOVA_FIXTURE_RUN_CONTEXT']), original)
                    self.assertEqual(json.loads(owner.environment['NOVA_FIXTURE_RUN_STORAGE']), storage)
                    self.assertNotIn('AWS_SESSION_TOKEN', owner.environment)
                    with patch.object(fixture_run, 'communicate', return_value=subprocess.CompletedProcess([],0)) as command:
                        self.assertEqual(owner.run(['original-command']), 0)
                    self.assertEqual(command.call_args.kwargs['cwd'], repo)
                    self.assertEqual(command.call_args.kwargs['environment'], owner.environment)
                cleanup.assert_not_called()
            result = json.loads((root/'owner-result.json').read_text())
            self.assertEqual(result['cleanup']['disposition'], 'inherited_owner')
            self.assertFalse(result['cleanup']['cleanup_rights'])

    def test_java_owner_cleans_only_owned_success_after_command_return_and_retains_failures(self):
        repo, publication = self.java_publication()
        for status in (0, 7):
            with self.subTest(status=status), patch.dict(os.environ, {}, clear=True):
                root = self.root.resolve()/('artifacts-'+str(status))
                def cleanup(*args, **kwargs):
                    observed = json.loads((root/'owner-result.json').read_text())
                    self.assertTrue(observed['command_returned'])
                    self.assertEqual(observed['exit_code'], 0)
                    self.assertEqual(kwargs['budget'].as_dict(),
                                     json.loads((root/'fixture.json').read_text())['run_context'])
                    return {'complete':True}
                with patch.object(java_rest_inputs.subprocess, 'run', return_value=subprocess.CompletedProcess([],0)), \
                        patch.object(fixture_run, 'startup_sweep'), \
                        patch.object(fixture_run, 'cleanup_run', side_effect=cleanup) as cleaned:
                    with java_rest_inputs.own_inputs(repo, root, {}) as owner:
                        with patch.object(fixture_run, 'communicate', return_value=subprocess.CompletedProcess([],status)):
                            self.assertEqual(owner.run(['original-command']), status)
                    self.assertEqual(cleaned.call_count, 1 if status == 0 else 0)
                facts = json.loads((root/'fixture.json').read_text())
                self.assertEqual(facts['publication'], str(publication))
                self.assertEqual(json.loads((root/'owner-result.json').read_text())['exit_code'], status)
                for family in ('IRU5','IRU13A','IRU13B'):
                    self.assertTrue((root/family).is_dir())

    def test_java_owner_cancel_and_incomplete_cleanup_keep_original_failure_facts(self):
        repo, publication = self.java_publication()
        for mode in ('cancel', 'cleanup', 'collection'):
            with self.subTest(mode=mode), patch.dict(os.environ, {}, clear=True):
                root=self.root.resolve()/('cancel-'+mode)
                with patch.object(java_rest_inputs.subprocess, 'run', return_value=subprocess.CompletedProcess([],0)), \
                        patch.object(fixture_run, 'startup_sweep'), \
                        patch.object(fixture_run, 'cleanup_run', return_value={'complete':False,'errors':['original']}) as cleanup:
                    if mode == 'cancel':
                        with self.assertRaises(KeyboardInterrupt):
                            with java_rest_inputs.own_inputs(repo, root, {}) as owner:
                                with patch.object(fixture_run, 'communicate', side_effect=KeyboardInterrupt):
                                    owner.run(['original-command'])
                        cleanup.assert_not_called()
                    elif mode == 'collection':
                        with self.assertRaisesRegex(ValueError,'native results absent'):
                            with java_rest_inputs.own_inputs(repo, root, {}) as owner:
                                with patch.object(fixture_run, 'communicate', return_value=subprocess.CompletedProcess([],0)):
                                    owner.run(['original-command'])
                                raise ValueError('native results absent')
                        cleanup.assert_not_called()
                    else:
                        with self.assertRaisesRegex(ValueError,'cleanup is incomplete'):
                            with java_rest_inputs.own_inputs(repo, root, {}) as owner:
                                with patch.object(fixture_run, 'communicate', return_value=subprocess.CompletedProcess([],0)):
                                    owner.run(['original-command'])
                facts=json.loads((root/'owner-result.json').read_text())
                self.assertEqual(facts['command_returned'],mode != 'cancel')
                self.assertEqual(facts['cleanup']['summary'],'violation' if mode == 'cleanup' else 'unknown')
                if mode == 'cancel':
                    self.assertEqual(facts['interrupted'],'KeyboardInterrupt')
                elif mode == 'collection':
                    self.assertEqual(facts['owner_scope_error'],'ValueError')
                    self.assertEqual(facts['exit_code'],0)
                else:
                    self.assertEqual(facts['cleanup']['report']['errors'],['original'])

    def cloud_publication(self):
        cloud = dict(kind="aliyun-oss", region="cn-test", bucket="fixture-bucket",
                     role_name="fixture-role", root_prefix="development/")
        self.manifest["env_id"] = "worktree-id"
        record = self.manifest["runtime"]["machine"]
        record["config"] = {"object_store": cloud, "benchmark_root": "s3://fixture-bucket/development/benchmarks"}
        record["storage_owner"] = dict(provider="aliyun-oss", endpoint="https://s3.oss-cn-test-internal.aliyuncs.com",
            region="cn-test", bucket="fixture-bucket", root_prefix="development/")
        record["server_warehouses"] = {service: "s3://fixture-bucket/development/machines/exact-machine/exact-generation/" + service
                                       for service in ("rest", "rest-mv")}
        self.write_manifest()
        # This broad, foreign location must never become the new run's root.
        self.env.write_text("export NOVA_ENV_ID='worktree-id'\nexport NOVA_ENV_OBJECT_STORE_KIND='aliyun-oss'\n"
                            "export CATALOG_WAREHOUSE_URI='s3://foreign-bucket/ancestor'\n")
        return record

    def test_cloud_new_admission_uses_exact_machine_worktree_domain_without_static_credentials(self):
        record = self.cloud_publication()
        with patch.dict(os.environ, {}, clear=True), patch("machine_runtime.MachineOwner") as machine:
            values, _, budget = fixture_run.admit(self.env)
            storage = json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"])
            self.assertEqual("s3://fixture-bucket/development/worktrees/worktree-id/scratch/" + budget.identity.run_id,
                             budget.identity.scope_root)
            self.assertEqual(2, storage["schema"])
            self.assertEqual(record["storage_owner"], storage["storage_owner"])
            fixture_run.RunStorageScope.from_dict(storage).validate_against(budget.identity, record)
            for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "MINIO_ROOT_USER"):
                self.assertNotIn(key, values)
                self.assertNotIn(key, os.environ)
            machine.assert_not_called()

    def test_cloud_inheritance_keeps_original_identity_scope_and_deadline(self):
        self.cloud_publication()
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2]
            storage = os.environ["NOVA_FIXTURE_RUN_STORAGE"]
            with patch.object(fixture_run.ScratchIdentity, "create", side_effect=AssertionError("must keep original run")):
                nested = fixture_run.admit(self.env, inherited=original.as_dict())[2]
            self.assertEqual(original.as_dict(), nested.as_dict())
            self.assertEqual(json.loads(storage), json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"]))

    def test_cloud_changed_owner_and_inherited_scope_are_rejected_before_publication(self):
        record = self.cloud_publication()
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2]
            context = os.environ["NOVA_FIXTURE_RUN_CONTEXT"]
            raw = os.environ["NOVA_FIXTURE_RUN_STORAGE"]
            record["storage_owner"]["bucket"] = "foreign-bucket"
            self.write_manifest()
            with self.assertRaises(fixture_runtime.RuntimeFailure):
                fixture_run.admit(self.env, inherited=original.as_dict())
            self.assertEqual(context, os.environ["NOVA_FIXTURE_RUN_CONTEXT"])
            self.assertEqual(raw, os.environ["NOVA_FIXTURE_RUN_STORAGE"])
            record["storage_owner"]["bucket"] = "fixture-bucket"
            self.write_manifest()
            wrong = json.loads(raw)
            wrong["storage_owner"]["root_prefix"] = "another-prefix/"
            os.environ["NOVA_FIXTURE_RUN_STORAGE"] = json.dumps(wrong)
            with self.assertRaises(fixture_runtime.RuntimeFailure):
                fixture_run.admit(self.env, inherited=original.as_dict())
            os.environ["NOVA_FIXTURE_RUN_STORAGE"] = raw
            record["config"]["object_store"] = None
            self.write_manifest()
            with patch.object(fixture_run.ScratchIdentity, "create", side_effect=AssertionError("must not create a replacement")):
                with self.assertRaises(fixture_runtime.RuntimeFailure):
                    fixture_run.admit(self.env, inherited=original.as_dict())
            self.assertEqual(context, os.environ["NOVA_FIXTURE_RUN_CONTEXT"])
            self.assertEqual(raw, os.environ["NOVA_FIXTURE_RUN_STORAGE"])

    def test_cloud_configuration_is_closed_and_never_falls_back_to_minio(self):
        import copy
        record = self.cloud_publication()
        valid = copy.deepcopy(record["config"]["object_store"])
        for cloud in (None, {}, {**valid, "kind": "minio"}, {**valid, "bucket": ""},
                      {**valid, "root_prefix": "development"}, {**valid, "access_key": "forbidden"}):
            with self.subTest(cloud=cloud), patch.dict(os.environ, {}, clear=True):
                record["config"]["object_store"] = cloud
                record["config"]["minio_volume"] = "must-not-fallback"
                self.write_manifest()
                with self.assertRaises(fixture_runtime.RuntimeFailure):
                    fixture_run.admit(self.env)
                self.assertNotIn("NOVA_FIXTURE_RUN_CONTEXT", os.environ)
                self.assertNotIn("NOVA_FIXTURE_RUN_STORAGE", os.environ)
        record["config"] = {"minio_volume": "must-not-fallback"}
        self.write_manifest()
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(fixture_runtime.RuntimeFailure):
            fixture_run.admit(self.env)

    def test_cloud_env_id_cannot_escape_its_exact_worktree_component(self):
        self.cloud_publication()
        for env_id in ("foreign-worktree", "../foreign", "worktree-id/other", ".", "..", "", "worktree-id\nforeign"):
            import shlex
            self.env.write_text("export NOVA_ENV_OBJECT_STORE_KIND='aliyun-oss'\nexport NOVA_ENV_ID=" + shlex.quote(env_id) + "\n")
            with self.subTest(env_id=env_id), patch.dict(os.environ, {}, clear=True), self.assertRaises((fixture_runtime.RuntimeFailure, ValueError)):
                fixture_run.admit(self.env)
        self.env.write_text("export NOVA_ENV_OBJECT_STORE_KIND='aliyun-oss'\n")
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(fixture_runtime.RuntimeFailure):
            fixture_run.admit(self.env)
        self.env.write_text("export NOVA_ENV_ID='worktree-id'\nexport NOVA_ENV_OBJECT_STORE_KIND='aliyun-oss'\n")
        del self.manifest["env_id"]
        self.write_manifest()
        with patch.dict(os.environ, {}, clear=True), self.assertRaises(fixture_runtime.RuntimeFailure):
            fixture_run.admit(self.env)

    def test_storage_projection_is_fixed_and_revalidated_before_native_effects(self):
        with patch.dict(os.environ, {}, clear=True):
            _, manifest, original = fixture_run.admit(self.env)
            storage = json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"])
            self.assertEqual(storage["identity"], original.identity.as_dict())
            self.assertEqual(set(original.as_dict()), {"schema", "identity", "started_unix_ms", "deadline_unix_ms", "cleanup_grace_seconds"})
            _, _, nested = fixture_run.admit(self.env)
            self.assertEqual(nested.as_dict(), original.as_dict())
            self.assertEqual(json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"]), storage)
            storage["ranges"]["rest"]["warehouse"] = "s3://warehouse/foreign/"
            os.environ["NOVA_FIXTURE_RUN_STORAGE"] = json.dumps(storage)
            with patch.object(fixture_run, "communicate") as native:
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "differs from the machine"):
                    fixture_run.compose(self.env, ["exec", "spark", "true"])
                native.assert_not_called()

    def test_persisted_storage_restores_with_original_context(self):
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2].as_dict()
            storage = json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"])
            saved = self.root / "state-storage.json"
            saved.write_text(json.dumps({"run_context": original, "run_storage": storage}))
            os.environ.pop("NOVA_FIXTURE_RUN_CONTEXT")
            os.environ.pop("NOVA_FIXTURE_RUN_STORAGE")
            self.assertTrue(fixture_run.restore_context(saved))
            self.assertEqual(fixture_run.admit(self.env)[2].as_dict(), original)
            self.assertEqual(json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"]), storage)

    def test_cleanup_run_uses_original_reserve_full_scope_and_retains_failure_report(self):
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2].as_dict()
            original["started_unix_ms"] -= 3400000
            original["deadline_unix_ms"] -= 3400000
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(original)
            storage = json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"])
            for complete in (True, False):
                report = {"complete":complete,"catalog_barrier":{"complete":complete},"errors":[] if complete else [{"code":"ScratchRestUnavailable"}]}
                with patch("machine_runtime.MachineOwner") as owner, patch("scratch.ScratchSweeper") as sweeper, \
                        patch.object(fixture_runtime,"install_signal_handlers"), contextlib.redirect_stdout(io.StringIO()) as output:
                    owner.return_value.operation_snapshot.return_value.__enter__.return_value = self.manifest["runtime"]["machine"]
                    sweeper.return_value.cleanup_run.return_value = report
                    self.assertEqual(fixture_run.main(["cleanup-run","--env-file",str(self.env)]),0 if complete else 1)
                self.assertEqual(json.loads(output.getvalue()), report)
                scope, pairs, deadline = sweeper.return_value.cleanup_run.call_args.args
                self.assertEqual(scope.as_dict(), storage)
                self.assertEqual(pairs, [])
                self.assertAlmostEqual(deadline-time.monotonic(), original["deadline_unix_ms"]/1000-time.time(), delta=0.1)
                self.assertEqual(json.loads(os.environ["NOVA_FIXTURE_RUN_CONTEXT"]),original)

    def test_command_owner_propagates_original_storage_and_deadline_to_native_driver(self):
        with patch.dict(os.environ,{},clear=True):
            original = fixture_run.admit(self.env)[2]
            storage = json.loads(os.environ["NOVA_FIXTURE_RUN_STORAGE"])
            os.environ["NOVA_FIXTURE_JAVA_DRIVER_OWNER"] = "true"
            with patch.object(fixture_run, "communicate",return_value=subprocess.CompletedProcess([],0)) as native, \
                    patch.object(fixture_runtime,"install_signal_handlers"):
                self.assertEqual(fixture_run.main(["command","--env-file",str(self.env),"--","bash","native-driver.sh"]),0)
            self.assertEqual(native.call_args.args[0],["bash","native-driver.sh"])
            kwargs=native.call_args.kwargs
            self.assertEqual(json.loads(kwargs["environment"]["NOVA_FIXTURE_RUN_CONTEXT"]),original.as_dict())
            self.assertEqual(json.loads(kwargs["environment"]["NOVA_FIXTURE_RUN_STORAGE"]),storage)
            self.assertEqual(kwargs["environment"]["NOVA_FIXTURE_JAVA_DRIVER_OWNER"],"true")
            self.assertLessEqual(kwargs["timeout"],original.remaining_seconds(work=True)+0.05)

    def test_explicit_startup_sweep_keeps_original_clock_and_pins_machine_identity(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input(); original=budget.as_dict()
            budget=replace(budget,deadline=time.monotonic()+budget.cleanup_grace_seconds+2)
            before=time.monotonic()
            with patch("machine_runtime.MachineOwner") as owner, patch.object(fixture_run.ScratchIdentity,"create",side_effect=AssertionError("must inherit original identity")):
                owner.return_value.sweep.return_value={"complete":True,"removed":[],"errors":[],"observed_counts":{"s3-prefix":3}}
                report=fixture_run.startup_sweep(self.env,budget=budget)
            self.assertEqual(report["observed_counts"],{"s3-prefix":3})
            self.assertTrue(report["complete"]); self.assertFalse(report["pending"])
            self.assertEqual(budget.as_dict(),original)
            self.assertEqual(owner.call_args.args,(str(self.root/"machine"),))
            self.assertEqual(owner.call_args.kwargs["daemon"],"exact-daemon")
            kwargs=owner.return_value.sweep.call_args.kwargs
            self.assertNotIn("expected_version", kwargs)
            self.assertEqual(kwargs["expected_generation"],"exact-generation")
            self.assertLessEqual(kwargs["deadline"],before+2.05)
            self.assertGreater(kwargs["deadline"],before)

    def test_startup_sweep_allows_only_bounded_native_scratch_timeout_as_partial(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            for value in ({"complete":False,"removed":[],"errors":[{"code":"ScratchSweepTimeout"}]}, fixture_runtime.RuntimeFailure("ScratchSweepTimeout","bounded cleanup settled")):
                with patch("machine_runtime.MachineOwner") as owner:
                    if isinstance(value,Exception): owner.return_value.sweep.side_effect=value
                    else: owner.return_value.sweep.return_value=value
                    report=fixture_run.startup_sweep(self.env,budget=budget)
                self.assertFalse(report["complete"]); self.assertTrue(report["pending"])
                self.assertEqual(report["errors"][0]["code"],"ScratchSweepTimeout")

    def ready_owner(self):
        from machine_runtime import MachineOwner
        backend = SimpleNamespace(
            daemon_id=Mock(return_value="exact-daemon"),
            unsettled_operations=Mock(return_value=[]),
            validate_minio_volume=Mock(), healthy=Mock(return_value=True),
            settle_operations=Mock())
        owner = MachineOwner(self.root / "machine", daemon="exact-daemon", backend=backend)
        state = json.loads(json.dumps(self.manifest["runtime"]["machine"]))
        state.update(schema=1, control_root=str(owner.root), phase="ready")
        owner._save(state)
        return owner, backend

    def test_competing_startup_sweep_rechecks_operation_scope_without_restarting_gc_or_budget(self):
        import machine_runtime as machine
        with patch.dict(os.environ, {}, clear=True):
            admitted = self.sweep_input()
            clock = [100.0]
            budget = replace(admitted, deadline=500.0, monotonic=lambda: clock[0])
            original = budget.as_dict()
            holder, _ = self.ready_owner()
            sweep_owner, sweep_backend = self.ready_owner()
            sweep_owner.timeout = 5
            ready_owner, ready_backend = self.ready_owner()
            held, release = threading.Event(), threading.Event()
            holder_errors = []
            def hold_lock():
                try:
                    with holder.maintenance_lock():
                        held.set()
                        if not release.wait(10):
                            raise AssertionError("test lock holder was not released")
                except BaseException as error:
                    holder_errors.append(error)
            thread = threading.Thread(target=hold_lock)
            thread.start()
            self.assertTrue(held.wait(2))
            def elapse(seconds):
                clock[0] += seconds
                # The peer uses its five-second GC window plus its existing
                # settlement window. The operation lock keeps its original cap.
                if clock[0] >= 110 and not release.is_set():
                    release.set()
                    thread.join(2)
                    self.assertFalse(thread.is_alive())
            observed_deadlines = []
            def unsettled(base):
                self.assertIsNotNone(ready_backend.maintenance_fd)
                observed_deadlines.append(ready_backend.operation_deadline)
                return []
            ready_backend.unsettled_operations.side_effect = unsettled
            try:
                with patch.object(machine.time, "monotonic", side_effect=lambda: clock[0]), \
                        patch.object(machine.time, "sleep", side_effect=elapse), \
                        patch.object(fixture_run, "admit", return_value=({}, self.manifest, budget)) as admit, \
                        patch.object(fixture_run.RunBudget, "admit", side_effect=AssertionError("must not renew the run")), \
                        patch.object(machine, "MachineOwner", side_effect=[sweep_owner, ready_owner]) as owners:
                    report = fixture_run.startup_sweep(self.env, budget=budget)
                self.assertFalse(report["complete"])
                self.assertTrue(report["pending"])
                self.assertTrue(report["operation_scope_complete"])
                self.assertEqual(report["errors"], [{"operation":"startup-sweep-lock", "code":"ScratchSweepTimeout"}])
                self.assertEqual(report["removed"], [])
                self.assertEqual(budget.as_dict(), original)
                self.assertEqual(admit.call_count, 1)
                self.assertEqual(owners.call_count, 2)
                self.assertAlmostEqual(owners.call_args_list[0].kwargs["timeout"], 5)
                self.assertNotIn("timeout", owners.call_args_list[1].kwargs)
                self.assertAlmostEqual(observed_deadlines[0], 195.0)
                self.assertGreaterEqual(clock[0], 110)
                sweep_backend.daemon_id.assert_not_called()
                sweep_backend.validate_minio_volume.assert_not_called()
                sweep_backend.settle_operations.assert_not_called()
                sweep_backend.healthy.assert_not_called()
                ready_backend.settle_operations.assert_not_called()
                ready_backend.healthy.assert_not_called()
                self.assertIsNone(getattr(sweep_backend, "operation_deadline", None))
                self.assertIsNone(getattr(ready_backend, "operation_deadline", None))
            finally:
                release.set()
                thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(holder_errors, [])

    def test_lock_pending_operation_scope_keeps_maintenance_identity_unknown_and_deadline_failures(self):
        import machine_runtime as machine
        cases = [
            ("maintenance", "MachineUnavailable"),
            ("generation", "MachineUnavailable"), ("unknown", "MachineUnavailable"),
            ("control-unknown", "MachineUnavailable"),
            ("io", "ScratchSweepIOFailed"), ("expired", "RunBudgetExpired"),
        ]
        for condition, code in cases:
            with self.subTest(condition=condition), patch.dict(os.environ, {}, clear=True):
                admitted = self.sweep_input()
                clock = [100.0]
                budget = replace(admitted, deadline=410.0, monotonic=lambda: clock[0])
                original = budget.as_dict()
                readiness, backend = self.ready_owner()
                state = readiness.read_saved_state()
                if condition in {"maintenance", "generation"}:
                    state[{"maintenance":"phase", "generation":"generation"}[condition]] = condition if condition=="maintenance" else "changed"
                    readiness._save(state)
                if condition in {"unknown", "control-unknown"}:
                    backend.unsettled_operations.return_value = [("owned", {"phase":"create-unknown" if condition=="unknown" else "command-unknown"})]
                backend.healthy.side_effect = AssertionError("operation scope must not inspect health")
                if condition=="io": backend.unsettled_operations.side_effect = OSError("private diagnostics")
                if condition=="expired": backend.unsettled_operations.side_effect = lambda _: clock.__setitem__(0, 110.0) or []
                saved = (readiness.base / "state.json").read_bytes()
                sweep = Mock()
                def lock_timeout(**kwargs):
                    self.assertNotIn("expected_version", kwargs)
                    self.assertEqual(kwargs["expected_generation"], "exact-generation")
                    self.assertEqual(kwargs["deadline"], 105.0)
                    clock[0] = 105.0
                    raise machine.SweepLockTimeout()
                sweep.sweep.side_effect = lock_timeout
                with patch.object(machine.time, "monotonic", side_effect=lambda: clock[0]), \
                        patch.object(fixture_run, "admit", return_value=({}, self.manifest, budget)), \
                        patch.object(machine, "MachineOwner", side_effect=[sweep, readiness]):
                    with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                        fixture_run.startup_sweep(self.env, budget=budget)
                self.assertEqual(caught.exception.code, code)
                self.assertEqual(caught.exception.report["errors"][-1], {"operation":"startup-operation-scope", "code":code})
                self.assertEqual(caught.exception.report["removed"], [])
                self.assertEqual(budget.as_dict(), original)
                self.assertEqual((readiness.base / "state.json").read_bytes(), saved)
                sweep.sweep.assert_called_once()
                backend.settle_operations.assert_not_called()

    def test_pending_sweep_ignores_historical_version_and_unrelated_health(self):
        import machine_runtime as machine
        with patch.dict(os.environ, {}, clear=True):
            budget = self.sweep_input()
            operation_owner, backend = self.ready_owner()
            state = operation_owner.read_saved_state()
            state["version"] = "historical-version"
            operation_owner._save(state)
            backend.healthy.side_effect = AssertionError("no environment health precheck")
            backend.validate_minio_volume.side_effect = AssertionError("no unrelated volume precheck")
            sweep_owner = Mock()
            sweep_owner.sweep.side_effect = machine.SweepLockTimeout()
            with patch.object(machine, "MachineOwner", side_effect=[sweep_owner, operation_owner]):
                report = fixture_run.startup_sweep(self.env, budget=budget)
            self.assertTrue(report["pending"])
            self.assertTrue(report["operation_scope_complete"])
            backend.unsettled_operations.assert_called_once()
            backend.healthy.assert_not_called()
            backend.validate_minio_volume.assert_not_called()
            backend.settle_operations.assert_not_called()

    def test_lock_pending_operation_scope_is_capped_by_original_work_end(self):
        import machine_runtime as machine
        with patch.dict(os.environ, {}, clear=True):
            admitted = self.sweep_input()
            clock = [100.0]
            budget = replace(admitted, deadline=410.0, monotonic=lambda: clock[0])
            readiness, backend = self.ready_owner()
            def unsettled(base):
                self.assertEqual(backend.operation_deadline, 110.0)
                return []
            backend.unsettled_operations.side_effect = unsettled
            sweep = Mock()
            def lock_timeout(**kwargs):
                clock[0] = 105.0
                raise machine.SweepLockTimeout()
            sweep.sweep.side_effect = lock_timeout
            with patch.object(machine.time, "monotonic", side_effect=lambda: clock[0]), \
                    patch.object(fixture_run, "admit", return_value=({}, self.manifest, budget)), \
                    patch.object(machine, "MachineOwner", side_effect=[sweep, readiness]):
                report = fixture_run.startup_sweep(self.env, budget=budget)
            self.assertTrue(report["pending"])
            self.assertTrue(report["operation_scope_complete"])
            backend.healthy.assert_not_called()
            backend.settle_operations.assert_not_called()

    def test_inventory_timeout_rechecks_original_operation_scope(self):
        import machine_runtime as machine
        for pending in (False, True):
            with self.subTest(pending_mutation=pending), patch.dict(os.environ, {}, clear=True):
                admitted = self.sweep_input()
                clock = [100.0]
                budget = replace(admitted, deadline=410.0, monotonic=lambda: clock[0])
                original = budget.as_dict()
                readiness, backend = self.ready_owner()
                backend.unsettled_operations.return_value = [("unknown", {"phase": "command-unknown"})] if pending else []
                saved = (readiness.base / "state.json").read_bytes()
                sweep = Mock()

                def inventory_timeout(**kwargs):
                    self.assertEqual(kwargs["deadline"], 105.0)
                    clock[0] = 105.0
                    raise machine.SweepInventoryTimeout()

                sweep.sweep.side_effect = inventory_timeout
                with patch.object(machine.time, "monotonic", side_effect=lambda: clock[0]), \
                        patch.object(fixture_run, "admit", return_value=({}, self.manifest, budget)), \
                        patch.object(machine, "MachineOwner", side_effect=[sweep, readiness]):
                    if pending:
                        with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "MachineUnavailable") as caught:
                            fixture_run.startup_sweep(self.env, budget=budget)
                        self.assertEqual(caught.exception.report["errors"][-1]["operation"], "startup-operation-scope")
                        backend.healthy.assert_not_called()
                    else:
                        report = fixture_run.startup_sweep(self.env, budget=budget)
                        self.assertTrue(report["pending"])
                        self.assertFalse(report["complete"])
                        self.assertTrue(report["operation_scope_complete"])
                        self.assertEqual(report["errors"], [{"operation": "startup-sweep-inventory", "code": "ScratchSweepTimeout"}])
                        backend.healthy.assert_not_called()
                backend.unsettled_operations.assert_called_once()
                backend.settle_operations.assert_not_called()
                self.assertEqual(budget.as_dict(), original)
                self.assertEqual((readiness.base / "state.json").read_bytes(), saved)

    def test_untyped_machine_maintaining_never_becomes_pending_scope(self):
        with patch.dict(os.environ, {}, clear=True):
            budget = self.sweep_input()
            error = fixture_runtime.RuntimeFailure("MachineMaintaining", "not a proven lock timeout")
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect = error
                with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                    fixture_run.startup_sweep(self.env, budget=budget)
            self.assertIs(caught.exception, error)
            self.assertEqual(owner.call_count, 1)
            owner.return_value.operation_snapshot.assert_not_called()

    def test_expired_run_after_lock_wait_never_dispatches_scope_check(self):
        import machine_runtime as machine
        with patch.dict(os.environ, {}, clear=True):
            admitted = self.sweep_input()
            clock = [100.0]
            budget = replace(admitted, deadline=402.0, monotonic=lambda: clock[0])
            original = budget.as_dict()
            def lock_timeout(**kwargs):
                self.assertEqual(kwargs["deadline"], 102.0)
                clock[0] = 102.0
                raise machine.SweepLockTimeout()
            with patch.object(machine.time, "monotonic", side_effect=lambda: clock[0]), \
                    patch.object(fixture_run, "admit", return_value=({}, self.manifest, budget)), \
                    patch.object(machine, "MachineOwner") as owner:
                owner.return_value.sweep.side_effect = lock_timeout
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "RunBudgetExpired") as caught:
                    fixture_run.startup_sweep(self.env, budget=budget)
            self.assertEqual(owner.call_count, 1)
            owner.return_value.operation_snapshot.assert_not_called()
            self.assertEqual(budget.as_dict(), original)
            self.assertEqual(caught.exception.report["removed"], [])

    def test_startup_sweep_never_hides_io_cleanup_or_unclassified_partial_errors(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            for code in ("ScratchAccessDenied","ContainerCleanupFailed","DockerOperationTimeout","ScratchSweepTraversalLimit"):
                report={"complete":False,"removed":[],"errors":[{"code":code},{"code":"ScratchSweepTimeout"}]}
                with patch("machine_runtime.MachineOwner") as owner:
                    owner.return_value.sweep.return_value=report
                    with self.assertRaises(fixture_runtime.RuntimeFailure) as caught: fixture_run.startup_sweep(self.env,budget=budget)
                self.assertEqual(caught.exception.code,code)
                self.assertEqual(caught.exception.report["errors"],report["errors"])
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect=fixture_runtime.RuntimeFailure("MachineUnavailable","original activation changed")
                with self.assertRaises(fixture_runtime.RuntimeFailure) as caught: fixture_run.startup_sweep(self.env,budget=budget)
                self.assertEqual(caught.exception.code,"MachineUnavailable")
                self.assertEqual(caught.exception.report["errors"][0]["code"],"MachineUnavailable")
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect=OSError("private native diagnostics")
                with self.assertRaises(fixture_runtime.RuntimeFailure) as caught: fixture_run.startup_sweep(self.env,budget=budget)
                self.assertEqual(caught.exception.code,"ScratchSweepIOFailed")
                self.assertNotIn("private native diagnostics",str(caught.exception))
                self.assertTrue(caught.exception.report["observations_unavailable"])

    def test_startup_sweep_expiration_is_run_failure_before_or_after_sweep(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            budget=replace(budget,deadline=time.monotonic()+budget.cleanup_grace_seconds-0.1)
            with patch("machine_runtime.MachineOwner") as owner:
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"RunBudgetExpired"): fixture_run.startup_sweep(self.env,budget=budget)
                owner.assert_not_called()
            clock=[time.monotonic()]
            budget=replace(budget,deadline=clock[0]+budget.cleanup_grace_seconds+2,monotonic=lambda:clock[0])
            def expire(**kwargs):
                clock[0]+=3
                return {"complete":True,"removed":[],"errors":[]}
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect=expire
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"RunBudgetExpired") as caught: fixture_run.startup_sweep(self.env,budget=budget)
            self.assertEqual(caught.exception.report,{"complete":True,"removed":[],"errors":[]})
            clock[0] = time.monotonic()
            budget = replace(budget, deadline=clock[0]+budget.cleanup_grace_seconds+2)
            partial = {"complete":False,"removed":[],"errors":[{"code":"ScratchSweepTimeout"}]}
            def expire_partial(**kwargs):
                clock[0] += 3
                return partial
            with patch("machine_runtime.MachineOwner") as owner:
                owner.return_value.sweep.side_effect = expire_partial
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"RunBudgetExpired") as caught:
                    fixture_run.startup_sweep(self.env,budget=budget)
            self.assertEqual(caught.exception.report,partial)

    def test_generic_admission_and_context_remain_read_only_and_cli_retains_failed_report(self):
        with patch.dict(os.environ,{},clear=True):
            budget=self.sweep_input()
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"]=json.dumps(budget.as_dict())
            with patch("machine_runtime.MachineOwner") as owner, patch.object(fixture_runtime,"install_signal_handlers"), contextlib.redirect_stdout(io.StringIO()) as output:
                fixture_run.admit(self.env)
                self.assertEqual(fixture_run.main(["context","--env-file",str(self.env)]),0)
                owner.assert_not_called()
            self.assertEqual(json.loads(output.getvalue()),budget.as_dict())
            with patch("machine_runtime.MachineOwner") as owner, patch.object(fixture_runtime,"install_signal_handlers"), contextlib.redirect_stdout(io.StringIO()) as output:
                owner.return_value.sweep.return_value={"complete":False,"errors":[{"code":"ScratchAccessDenied"}],"removed":[]}
                with self.assertRaises(fixture_runtime.RuntimeFailure): fixture_run.main(["sweep-start","--env-file",str(self.env)])
            self.assertEqual(json.loads(output.getvalue())["errors"][0]["code"],"ScratchAccessDenied")
            self.manifest["runtime"]={"catalog":{"id":"private"}}; self.write_manifest()
            with patch("machine_runtime.MachineOwner") as owner:
                self.assertTrue(fixture_run.startup_sweep(self.env)["skipped"])
                owner.assert_not_called()

    def test_copy_targets_captured_spark_in_shared_and_hybrid_publications(self):
        for shared in (True, False):
            self.manifest["shared_docker"] = shared
            self.manifest["runtime"]["catalog"] = {"containers": {"spark": "private-exact"}}
            self.write_manifest()
            with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate") as run:
                fixture_run.compose(self.env, ["-p", "published", "cp", "--archive", "input", "spark:/tmp/output"])
            self.assertEqual(run.call_args.args[0], ["docker", "cp", "--archive", "input",
                ("exact-spark" if shared else "private-exact") + ":/tmp/output"])

    def test_persisted_context_keeps_deadline_and_refuses_replacement(self):
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2].as_dict()
            saved = self.root / "state.json"
            saved.write_text(json.dumps({"run_context": original}))
            self.assertTrue(fixture_run.restore_context(saved))
            self.assertEqual(fixture_run.admit(self.env)[2].as_dict(), original)
            replacement = fixture_run.admit(self.env, inherited={**original, "deadline_unix_ms": original["deadline_unix_ms"] - 1000})[2]
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(replacement.as_dict())
            with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "persisted original"):
                fixture_run.restore_context(saved)
            original["started_unix_ms"] -= 10000000
            original["deadline_unix_ms"] -= 10000000
            saved.write_text(json.dumps({"run_context": original}))
            os.environ.pop("NOVA_FIXTURE_RUN_CONTEXT")
            fixture_run.restore_context(saved)
            with self.assertRaises(fixture_runtime.RuntimeFailure):
                fixture_run.admit(self.env)

    def test_temp_marker_cleanup_uses_original_reserve_and_native_status(self):
        self.env.write_text("export NOVA_ENV_ID=worktree-id\nexport NOVA_ENV_COMPOSE_ENV=/published/compose.env\nexport NOVA_ENV_COMPOSE_PROJECT=published\nexport NOVA_ENV_COMPOSE_FILE=/published/compose.yml\n")
        with patch.dict(os.environ, {}, clear=True):
            original = fixture_run.admit(self.env)[2].as_dict()
            original["started_unix_ms"] -= 3400000
            original["deadline_unix_ms"] -= 3400000
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"] = json.dumps(original)
            with self.assertRaises(fixture_runtime.RuntimeFailure):
                fixture_run.admit(self.env)
            os.environ["NOVA_FIXTURE_CLEANUP_MODE"] = "true"
            folder = "/tmp/novarocks-fixture-data-" + original["identity"]["run_id"] + "-" + "a" * 32
            with patch.object(fixture_run, "compose", return_value=subprocess.CompletedProcess([], 0)) as run:
                fixture_run.cleanup_temp(self.env, folder)
                marker = json.loads(run.call_args.args[1][-1])
                self.assertEqual(marker["context"], original)
                self.assertIn("marker.read_text()", run.call_args.args[1][-3])
            with patch.object(fixture_run, "compose", return_value=subprocess.CompletedProcess([], 13)):
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure, "removal exited 13"):
                    fixture_run.cleanup_temp(self.env, folder)
            with patch.object(fixture_run, "compose") as run:
                with self.assertRaises(fixture_runtime.RuntimeFailure):
                    fixture_run.cleanup_temp(self.env, folder.replace(original["identity"]["run_id"], "foreign"))
                run.assert_not_called()

    def test_nested_budget_keeps_original_identity_and_deadline(self):
        with patch.dict(os.environ, {}, clear=True):
            first = fixture_run.admit(self.env)[2]
            nested = fixture_run.admit(self.env, inherited=first.as_dict())[2]
        self.assertEqual(first.as_dict(), nested.as_dict())
        self.assertLessEqual(nested.remaining_seconds(), first.remaining_seconds() + 0.01)

    def test_overlarge_budget_is_rejected_before_docker(self):
        with patch.object(fixture_run, "communicate") as run:
            for seconds in (0, -1, 86401):
                with self.subTest(seconds=seconds), self.assertRaises(fixture_runtime.RuntimeFailure):
                    fixture_run.admit(self.env, seconds)
        run.assert_not_called()

    def test_private_publication_does_not_gain_shared_identity(self):
        self.manifest["runtime"] = {"catalog": {"id": "private"}}
        self.write_manifest()
        self.assertIsNone(fixture_run.admit(self.env)[2])

    def test_shell_values_are_decoded_without_execution(self):
        self.env.write_text("export NOVA_ENV_ID='$(exit 9)'\nunset OLD\n")
        self.assertEqual(fixture_run.env_values(self.env)["NOVA_ENV_ID"], "$(exit 9)")

    def _settlement_export_fact(self, returncode=0):
        return dict(child_waited=True, returncode=returncode,
                    cleanup_failed=False, never_started=False)

    def _settlement_export_responses(self, fact, *, cli_code=0, settle_code=0, remove_code=0):
        # Only the existing communicate boundary is mocked. No Docker or
        # replacement owner/settlement state machine is implemented here.
        raw = fact if isinstance(fact, bytes) else json.dumps(fact).encode()
        return [subprocess.CompletedProcess([], 0, b"exact-spark", b""),
                subprocess.CompletedProcess([], cli_code, b"original-output", b"original-error"),
                subprocess.CompletedProcess([], settle_code, raw, b""),
                subprocess.CompletedProcess([], remove_code, b"", b"")]

    def test_settlement_export_preserves_original_success_and_safe_projection(self):
        receipt = self.root / "settlement.json"
        fact = self._settlement_export_fact()
        # The export must project the original admission, not trust extra
        # identity-shaped fields from an untyped remote JSON object.
        fact.update(context={"foreign": True}, container_id="foreign-container",
                    unexpected_private="do-not-export-remote-extra")
        responses = self._settlement_export_responses(fact)
        arguments = ["-p", "fixed", "exec", "-T", "-e", "FLAG=do-not-export-env",
                     "spark", "bash", "-lc", "printf do-not-export-payload"]
        with patch.dict(os.environ, {}, clear=True), patch.object(
                fixture_run, "communicate", side_effect=responses) as run:
            original = fixture_run.admit(self.env)[2].as_dict()
            result = fixture_run.compose(self.env, arguments, data=b"do-not-export-stdin",
                                         capture=True, settlement_receipt=receipt)
        self.assertIs(result, responses[1])
        self.assertEqual(result.returncode, 0)
        self.assertEqual(result.stdout, b"original-output")
        self.assertEqual(result.stderr, b"original-error")
        self.assertEqual(run.call_count, 4, "export must not add a Docker operation")
        commands = [call.args[0] for call in run.call_args_list]
        worker = commands[1]
        owner = worker.index(fixture_run.CONTAINER_OWNER)
        folder = worker[owner + 1]
        self.assertEqual(worker[owner - 3:owner], ["exact-spark", "python3", "-c"])
        self.assertEqual(worker[-3:], ["bash", "-lc", "printf do-not-export-payload"])
        self.assertEqual(json.loads(worker[owner + 4]), original)
        self.assertEqual(worker[owner + 2], str(original["deadline_unix_ms"] / 1000))
        settle = commands[2]
        self.assertEqual(settle[:4], ["docker", "exec", "-i", "exact-spark"])
        index = settle.index(fixture_run.CONTAINER_SETTLE)
        self.assertEqual(settle[index + 1], folder)
        self.assertEqual(settle[index + 2], worker[owner + 2])
        self.assertEqual(json.loads(settle[index + 3]), original)
        self.assertEqual(commands[3], ["docker", "exec", "-i", "exact-spark", "rm", "-rf", folder])
        exported = json.loads(receipt.read_text())
        self.assertEqual(set(exported), {"schema", "context", "container_id", "owner_folder",
            "payload_sha256", "child_waited", "returncode", "cleanup_failed", "never_started",
            "cli_returncode", "temporary_owner_removed"})
        self.assertEqual(exported["schema"], "fixture-compose-settlement-v1")
        self.assertEqual(exported["context"], original)
        self.assertEqual(exported["container_id"], "exact-spark")
        self.assertEqual(exported["owner_folder"], folder)
        self.assertTrue(folder.startswith("/tmp/novarocks-fixture-run-" + original["identity"]["run_id"] + "-"))
        self.assertEqual(exported["payload_sha256"], hashlib.sha256(
            b'["bash","-lc","printf do-not-export-payload"]').hexdigest())
        self.assertIs(exported["child_waited"], True)
        self.assertEqual(exported["returncode"], 0)
        self.assertEqual(exported["cli_returncode"], 0)
        self.assertIs(exported["cleanup_failed"], False)
        self.assertIs(exported["never_started"], False)
        self.assertIs(exported["temporary_owner_removed"], True)
        self.assertEqual(receipt.stat().st_mode & 0o777, 0o600)
        for private in ["do-not-export-env", "do-not-export-payload", "do-not-export-stdin",
                        "do-not-export-remote-extra", "original-output", "original-error"]:
            self.assertNotIn(private, receipt.read_text())

    def test_settlement_export_retains_matching_nonzero_and_signaled_exit(self):
        for remote, cli in [(9, 9), (-15, 143)]:
            with self.subTest(remote=remote, cli=cli):
                receipt = self.root / ("failed-" + str(cli) + ".json")
                responses = self._settlement_export_responses(
                    self._settlement_export_fact(remote), cli_code=cli)
                with patch.dict(os.environ, {}, clear=True), patch.object(
                        fixture_run, "communicate", side_effect=responses) as run:
                    result = fixture_run.compose(self.env, ["exec", "-T", "spark", "false"],
                                                 capture=True, settlement_receipt=receipt)
                self.assertIs(result, responses[1])
                self.assertEqual(result.returncode, cli)
                exported = json.loads(receipt.read_text())
                self.assertEqual(exported["returncode"], remote)
                self.assertEqual(exported["cli_returncode"], cli)
                self.assertIs(exported["child_waited"], True)
                self.assertIs(exported["temporary_owner_removed"], True)
                self.assertEqual(run.call_count, 4)

    def test_settlement_export_refuses_unconfirmed_or_malformed_exit(self):
        cases = [
            ("settle-command-failed", self._settlement_export_fact(), 125, 0, 0, 3),
            ("missing-settlement", b"", 0, 0, 0, 3),
            ("malformed-json", b"not-json", 0, 0, 0, 3),
            ("wrong-json-shape", [], 0, 0, 0, 3),
            ("missing-exit", dict(child_waited=True), 0, 0, 0, 3),
            ("unknown-group", dict(child_waited=False, returncode=0), 0, 0, 0, 3),
            ("boolean-exit", dict(child_waited=True, returncode=True), 0, 0, 0, 3),
            ("cleanup-failed", dict(child_waited=True, returncode=0, cleanup_failed=True), 0, 0, 0, 3),
            ("cli-worker-disagree", self._settlement_export_fact(9), 0, 0, 0, 3),
            ("tmp-removal-failed", self._settlement_export_fact(), 0, 0, 8, 4),
        ]
        for name, fact, settle, cli, remove, expected_calls in cases:
            with self.subTest(name=name):
                receipt = self.root / (name + ".json")
                responses = self._settlement_export_responses(
                    fact, cli_code=cli, settle_code=settle, remove_code=remove)
                with patch.dict(os.environ, {}, clear=True), patch.object(
                        fixture_run, "communicate", side_effect=responses) as run:
                    with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                        fixture_run.compose(self.env, ["exec", "-T", "spark", "true"],
                                            capture=True, settlement_receipt=receipt)
                self.assertEqual(caught.exception.code, "ContainerCleanupFailed")
                self.assertFalse(receipt.exists(), "unconfirmed settlement must not publish an exit")
                self.assertEqual(run.call_count, expected_calls, "no retry or extra remote observation")

    def test_settlement_export_preserves_transport_failure_and_unknown_cli(self):
        for never_started in [False, True]:
            with self.subTest(never_started=never_started):
                receipt = self.root / ("transport-" + str(never_started) + ".json")
                fact = self._settlement_export_fact(125 if never_started else 0)
                fact["never_started"] = never_started
                original_error = subprocess.TimeoutExpired(["original-worker"], 1)
                responses = self._settlement_export_responses(fact)
                responses[1] = original_error
                with patch.dict(os.environ, {}, clear=True), patch.object(
                        fixture_run, "communicate", side_effect=responses) as run:
                    with self.assertRaises(subprocess.TimeoutExpired) as caught:
                        fixture_run.compose(self.env, ["exec", "-T", "spark", "true"],
                                            capture=True, settlement_receipt=receipt)
                self.assertIs(caught.exception, original_error)
                exported = json.loads(receipt.read_text())
                self.assertIsNone(exported["cli_returncode"], "missing worker result is not CLI success")
                self.assertEqual(exported["returncode"], fact["returncode"])
                self.assertIs(exported["never_started"], never_started)
                self.assertIs(exported["temporary_owner_removed"], not never_started)
                self.assertEqual(run.call_count, 3 if never_started else 4)

    def test_settlement_export_does_not_invent_missing_cleanup_or_start_facts(self):
        receipt = self.root / "incomplete-control-facts.json"
        # The old compose API accepts this minimal original result. Optional
        # export preserves absent fields rather than manufacturing False.
        responses = self._settlement_export_responses(dict(child_waited=True, returncode=0))
        with patch.dict(os.environ, {}, clear=True), patch.object(
                fixture_run, "communicate", side_effect=responses) as run:
            result = fixture_run.compose(self.env, ["exec", "-T", "spark", "true"],
                                         capture=True, settlement_receipt=receipt)
        self.assertIs(result, responses[1])
        exported = json.loads(receipt.read_text())
        self.assertIsNone(exported["cleanup_failed"])
        self.assertIsNone(exported["never_started"])
        self.assertEqual(run.call_count, 4)

    def test_settlement_export_keeps_settlement_transport_unknown_unpublished(self):
        receipt = self.root / "settlement-unknown.json"
        original_error = subprocess.TimeoutExpired(["original-settlement"], 1)
        responses = self._settlement_export_responses(self._settlement_export_fact())
        responses[2] = original_error
        with patch.dict(os.environ, {}, clear=True), patch.object(
                fixture_run, "communicate", side_effect=responses) as run:
            with self.assertRaises(subprocess.TimeoutExpired) as caught:
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"],
                                    capture=True, settlement_receipt=receipt)
        self.assertIs(caught.exception, original_error)
        self.assertEqual(run.call_count, 3)
        self.assertFalse(receipt.exists())

    def test_settlement_export_rejects_foreign_context_storage_before_dispatch(self):
        receipt = self.root / "foreign-context.json"
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate") as run:
            original = fixture_run.admit(self.env)[2].as_dict()
            original_storage = os.environ[fixture_run.RUN_STORAGE_ENV]
            os.environ.pop("NOVA_FIXTURE_RUN_CONTEXT")
            os.environ.pop(fixture_run.RUN_STORAGE_ENV)
            foreign = fixture_run.admit(self.env)[2].as_dict()
            self.assertNotEqual(original["identity"], foreign["identity"])
            # Keep a valid new context but the original owner's storage. The
            # existing admission boundary must reject the cross-owner pair.
            os.environ[fixture_run.RUN_STORAGE_ENV] = original_storage
            with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"],
                                    capture=True, settlement_receipt=receipt)
        self.assertEqual(caught.exception.code, "InvalidScratchScope")
        run.assert_not_called()
        self.assertFalse(receipt.exists())

    def test_settlement_export_refuses_replaced_container_before_worker_start(self):
        receipt = self.root / "wrong-container.json"
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate",
                return_value=subprocess.CompletedProcess([], 0, b"replacement-spark", b"")) as run:
            with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"],
                                    capture=True, settlement_receipt=receipt)
        self.assertEqual(caught.exception.code, "MachineUnavailable")
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0][-3:], ["ps", "-q", "spark"])
        self.assertFalse(receipt.exists())

    def test_settlement_export_never_overwrites_an_existing_sidecar(self):
        receipt = self.root / "existing.json"
        original = b"immutable-original-receipt\n"
        receipt.write_bytes(original)
        receipt.chmod(0o640)
        responses = self._settlement_export_responses(self._settlement_export_fact())
        with patch.dict(os.environ, {}, clear=True), patch.object(
                fixture_run, "communicate", side_effect=responses) as run:
            with self.assertRaises(FileExistsError):
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"],
                                    capture=True, settlement_receipt=receipt)
        self.assertEqual(receipt.read_bytes(), original)
        self.assertEqual(receipt.stat().st_mode & 0o777, 0o640)
        self.assertEqual(run.call_count, 4, "exclusive export happens after original settlement/removal")
        self.assertEqual(run.call_args.args[0][-3:-1], ["rm", "-rf"])

    def test_settlement_export_requires_admitted_exec(self):
        receipt = self.root / "not-an-owned-exec.json"
        original_manifest = json.loads(json.dumps(self.manifest))
        for private, arguments in [(True, ["exec", "-T", "spark", "true"]),
                                   (False, ["ps", "spark"])]:
            with self.subTest(private=private):
                self.manifest = json.loads(json.dumps(original_manifest))
                if private:
                    self.manifest["runtime"] = {"catalog": {"id": "private"}}
                self.write_manifest()
                with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate") as run:
                    with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                        fixture_run.compose(self.env, arguments, capture=True, settlement_receipt=receipt)
                self.assertEqual(caught.exception.code, "InvalidConfiguration")
                run.assert_not_called()
                self.assertFalse(receipt.exists())

    def test_settlement_export_does_not_change_default_command_or_return(self):
        arguments = ["-p", "fixed", "exec", "-T", "-e", "FLAG=1", "spark", "true"]
        for code in [0, 9]:
            with self.subTest(code=code), patch.dict(os.environ, {}, clear=True):
                original = fixture_run.admit(self.env)[2].as_dict()
                traces = []
                for export in [False, True]:
                    receipt = self.root / ("observational-" + str(code) + ".json")
                    responses = self._settlement_export_responses(self._settlement_export_fact(code), cli_code=code)
                    with patch.object(fixture_run, "communicate", side_effect=responses) as run:
                        if export:
                            result = fixture_run.compose(self.env, arguments, data=b"original-stdin",
                                                         capture=True, settlement_receipt=receipt)
                        else:
                            # Deliberately omit the keyword, exercising the old API.
                            result = fixture_run.compose(self.env, arguments, data=b"original-stdin", capture=True)
                            self.assertFalse(receipt.exists())
                    self.assertIs(result, responses[1])
                    self.assertEqual((result.returncode, result.stdout, result.stderr),
                                     (code, b"original-output", b"original-error"))
                    self.assertEqual(run.call_count, 4)
                    worker = run.call_args_list[1].args[0]
                    folder = worker[worker.index(fixture_run.CONTAINER_OWNER) + 1]
                    traces.append([[word.replace(folder, "<ORIGINAL_OWNER_FOLDER>") for word in call.args[0]]
                                   for call in run.call_args_list])
                    self.assertEqual(run.call_args_list[1].kwargs["data"], b"original-stdin")
                    self.assertTrue(run.call_args_list[1].kwargs["capture"])
                    if export:
                        self.assertEqual(json.loads(receipt.read_text())["context"], original)
                self.assertEqual(traces[0], traces[1], "export cannot add/reorder/change remote commands")

    def test_spark_exec_preserves_flags_and_confirms_actual_wait(self):
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            output = b'{"child_waited":true,"returncode":0}' if fixture_run.CONTAINER_SETTLE in command else (b"exact-spark" if command[-3:] == ["ps", "-q", "spark"] else b"query-output")
            return subprocess.CompletedProcess(command, 0, output, b"")
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=run):
            result = fixture_run.compose(self.env, ["-p", "fixed", "exec", "-T", "-e", "FLAG=1", "spark", "bash", "-lc", "echo hi"], capture=True)
        self.assertEqual(result.stdout, b"query-output")
        self.assertIn("FLAG=1", calls[1])
        self.assertEqual(calls[1][-3:], ["bash", "-lc", "echo hi"])
        self.assertIn(fixture_run.CONTAINER_SETTLE, calls[2])
        self.assertEqual(calls[-1][-3:-1], ["rm", "-rf"])

    def test_failed_worker_cleanup_is_not_reported_as_success(self):
        responses = [subprocess.CompletedProcess([], 0, b"exact-spark", b""),
                     subprocess.CompletedProcess([], 0, b"ok", b""),
                     subprocess.CompletedProcess([], 125, b"", b"")]
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=responses):
            with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"], capture=True)
        self.assertEqual(caught.exception.code, "ContainerCleanupFailed")

    def test_replaced_spark_is_rejected_before_exec(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate",
                return_value=subprocess.CompletedProcess([], 0, b"replacement-spark", b"")) as run:
            with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                fixture_run.compose(self.env, ["exec", "-T", "spark", "true"])
        self.assertEqual(caught.exception.code, "MachineUnavailable")
        self.assertEqual(run.call_count, 1)
        self.assertEqual(run.call_args.args[0][-3:], ["ps", "-q", "spark"])

    def test_private_spark_with_shared_minio_uses_its_own_exact_container(self):
        self.manifest["shared_docker"] = False
        self.manifest["runtime"]["catalog"] = {"containers": {"spark": "private-spark"}}
        self.write_manifest()
        calls = []
        def run(command, **kwargs):
            calls.append(command)
            output = b"private-spark" if command[-3:] == ["ps", "-q", "spark"] else b'{"returncode":0,"child_waited":true}'
            return subprocess.CompletedProcess(command, 0, output, b"")
        with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=run):
            fixture_run.compose(self.env, ["exec", "-T", "spark", "true"])
        for command in calls[1:]:
            self.assertIn("private-spark", command)
            self.assertNotIn("exact-spark", command)

    def test_successful_cli_cannot_hide_failed_worker_or_tmp_cleanup(self):
        cases = [({"returncode": 9, "child_waited": True}, 0),
                 ({"returncode": 0, "child_waited": False}, 0),
                 ({"returncode": 0, "child_waited": True}, 8)]
        for receipt, cleanup_code in cases:
            with self.subTest(receipt=receipt, cleanup_code=cleanup_code):
                responses = [subprocess.CompletedProcess([], 0, b"exact-spark", b""),
                             subprocess.CompletedProcess([], 0, b"", b""),
                             subprocess.CompletedProcess([], 0, json.dumps(receipt).encode(), b""),
                             subprocess.CompletedProcess([], cleanup_code, b"", b"")]
                with patch.dict(os.environ, {}, clear=True), patch.object(fixture_run, "communicate", side_effect=responses):
                    with self.assertRaises(fixture_runtime.RuntimeFailure) as caught:
                        fixture_run.compose(self.env, ["exec", "-T", "spark", "true"])
                self.assertEqual(caught.exception.code, "ContainerCleanupFailed")

    @unittest.skipUnless(sys.platform.startswith("linux"), "worker runs inside the Linux Spark image")
    def test_worker_retains_actual_exit_and_removes_a_timed_out_process(self):
        for script, grace, expected in (("raise SystemExit(17)", 0, 17),
                                        ("import time; time.sleep(30)", 0.2, 124)):
            with self.subTest(expected=expected):
                folder = self.root / str(expected)
                result = subprocess.run([sys.executable, "-c", fixture_run.CONTAINER_OWNER,
                    str(folder), str(time.time() + 0.5), str(grace), "{}",
                    sys.executable, "-c", script], timeout=4)
                self.assertEqual(result.returncode, expected)
                self.assertEqual(json.loads((folder / "result.json").read_text()),
                                 {"returncode": expected, "child_waited": True})

    def test_expired_settlement_retains_owner_until_observed_group_exit(self):
        folder=self.root/"late-settlement"
        release=self.root/"release-group"
        proc=self.root/"fake-proc"; proc.mkdir()
        entry=proc/"1"; entry.mkdir()
        # Exercise the actual owner/waitid state machine on this OS. Only
        # Linux /proc group observation and killpg are simulated here.
        observation = "\n".join([
            "nr_original_iterdir=pathlib.Path.iterdir",
            "def nr_observation(path):",
            "    if str(path) != '/proc': return nr_original_iterdir(path)",
            "    if pathlib.Path("+repr(str(release))+").exists(): return iter([])",
            "    entry=pathlib.Path("+repr(str(entry))+")",
            "    (entry/'stat').write_text('1 (simulated descendant) S 0 '+str(child.pid)+' 0')",
            "    return iter([entry])",
            "pathlib.Path.iterdir=nr_observation",
            "os.killpg=lambda pid,number: None",
        ])
        code=fixture_run.CONTAINER_OWNER.replace('pathlib.Path("/tmp")','pathlib.Path('+repr(str(self.root))+')')
        code=code.replace('# Keep the leader unreaped',observation+'\n# Keep the leader unreaped')
        worker=subprocess.Popen([sys.executable,"-c",code,str(folder),str(time.time()+0.3),"0","{}",
                                 sys.executable,"-c","raise SystemExit(17)"],stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            end=time.monotonic()+3
            while time.monotonic()<end and not (folder/"result.json").exists(): time.sleep(0.01)
            fact=json.loads((folder/"result.json").read_text())
            self.assertFalse(fact["child_waited"])
            self.assertTrue(fact["cleanup_failed"])
            self.assertTrue(fact["cleanup_deadline_expired"])
            self.assertIsNone(worker.poll(),"owner must retain the unreaped leader past failed deadline")
            release.touch()
            _,error=worker.communicate(timeout=3)
            self.assertEqual(worker.returncode,125,error)
            final=json.loads((folder/"result.json").read_text())
            self.assertTrue(final["child_waited"])
            self.assertTrue(final["cleanup_failed"],"late actual exit must not erase deadline failure")
        finally:
            release.touch()
            if worker.poll() is None: worker.kill()
            worker.communicate()

    def test_cancellation_tombstone_prevents_a_late_worker_start(self):
        folder = self.root / "late"
        marker = self.root / "must-not-exist"
        stopped = subprocess.run([sys.executable, "-c", fixture_run.CONTAINER_SETTLE,
                                  str(folder), str(time.time() + 1), "{}"], capture_output=True)
        self.assertEqual(stopped.returncode, 0)
        late = subprocess.run([sys.executable, "-c", fixture_run.CONTAINER_OWNER,
            str(folder), str(time.time() + 1), "0", "{}", sys.executable, "-c",
            "from pathlib import Path; Path(" + repr(str(marker)) + ").touch()"],
            capture_output=True)
        self.assertNotEqual(late.returncode, 0)
        self.assertFalse(marker.exists())

    def test_expired_temp_gc_requires_exact_private_marker_and_settled_run(self):
        identity = fixture_run.ScratchIdentity.create("s3://novarocks/test", now_ms=int(time.time() * 1000) - 10000)
        context = {"schema": 1, "identity": identity.as_dict(),
                   "started_unix_ms": identity.as_dict()["created_unix_ms"],
                   "deadline_unix_ms": int(time.time() * 1000) - 5000}
        folders = []
        for suffix, owner in (("1" * 32, "novarocks-fixture-run-v1"), ("2" * 32, "foreign")):
            folder = self.root / ("novarocks-fixture-data-" + identity.run_id + "-" + suffix)
            folder.mkdir(mode=0o700)
            (folder / ".fixture-context.json").write_text(json.dumps({"schema": 1, "owner": owner, "context": context}))
            folders.append(folder)
        unsettled = fixture_run.ScratchIdentity.create("s3://novarocks/test", now_ms=int(time.time() * 1000) - 10000,
                                                       token="3" * 32)
        unsafe_context = {**context, "identity": unsettled.as_dict()}
        retained = []
        for kind in ("data", "run"):
            folder = self.root / ("novarocks-fixture-" + kind + "-" + unsettled.run_id + "-" + "4" * 32)
            folder.mkdir(mode=0o700)
            (folder / ".fixture-context.json").write_text(json.dumps({"schema": 1, "owner": "novarocks-fixture-run-v1", "context": unsafe_context}))
            if kind == "run":
                (folder / "result.json").write_text('{"child_waited":false,"cleanup_failed":true}')
            retained.append(folder)
        # Remap only the temporary filesystem root for the hermetic carrier;
        # execute the actual classifier and leave the host's /tmp untouched.
        code = fixture_run.CONTAINER_OWNER.replace('pathlib.Path("/tmp")', 'pathlib.Path(' + repr(str(self.root)) + ')')
        result = subprocess.run([sys.executable, "-c", code, str(self.root / "expired-owner"),
            str(time.time() - 1), "0", "{}", "must-not-start"], capture_output=True)
        self.assertEqual(result.returncode, 124, result.stderr)
        self.assertFalse(folders[0].exists())
        self.assertTrue(folders[1].exists())
        self.assertTrue(all(folder.exists() for folder in retained))


class HmsPreparedInputTests(unittest.TestCase):
    """The fixed verifier reads only hermetic files; no Spark or network."""

    def setUp(self):
        import importlib.util
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        source = Path(__file__).resolve().parents[3] / 'tests/sql/fixtures/iceberg-hms/fixture.py'
        spec = importlib.util.spec_from_file_location('hms_fixed_verifier_for_test', source)
        self.fixture = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.fixture)
        self.namespace = 'iru7_local_verifier_test'
        self.sibling = self.namespace + '_new'
        self.evidence = self.root / 'tests/sql/.runtime/hms-evidence' / self.namespace
        self.evidence.mkdir(parents=True)
        publication = self.root / 'publications'
        publication.mkdir()
        for name in ['env.sh', 'manifest.json', 'spark.conf', 'hms.conf']:
            (publication / name).write_text('local verifier input only\n')
        self.context = {'schema': 1, 'identity': {'run_id': 'local-verifier-original-run',
                        'scope_root': 's3://local-verifier/original'}, 'deadline_unix_ms': 123456789}
        self.storage = {'local_model_only': True, 'owner': 'original-verifier-test'}
        environment = {'NOVA_FIXTURE_RUN_CONTEXT': json.dumps(self.context),
            'NOVA_FIXTURE_RUN_STORAGE': json.dumps(self.storage),
            'NOVA_ENV_REST_ENV_FILE': str(publication / 'env.sh'),
            'NOVAROCKS_SPARK_DEFAULTS': str(publication / 'spark.conf'),
            'NOVAROCKS_SPARK_HMS_DEFAULTS': str(publication / 'hms.conf'),
            'NOVAROCKS_WORKSPACE_ROOT': str(self.root)}
        self.environment = patch.dict(os.environ, environment)
        self.environment.start()
        self.addCleanup(self.environment.stop)
        self.sources = ['tests/sql/fixtures/iceberg-hms/fixture.py',
                        'docker/iceberg-rest/fixture_run.py', 'case.sql', 'case.result']
        for name in self.sources:
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('not executed; exact local source identity: ' + name)
        self.intent = {'schema': 'hms-case-input-intent-v1', 'namespace': self.namespace,
            'new_namespace': self.sibling, 'context': self.context,
            'storage_sha256': self.fixture.canonical_sha(self.storage),
            'publications': self.fixture.publications(),
            'sources': {name: self.sha(self.root / name) for name in self.sources}}
        self.write('input.intent.json', self.intent)
        self.remote = {'schema': 'fixture-compose-settlement-v1', 'local_model_only': True,
            'context': self.context, 'child_waited': True, 'returncode': 0,
            'cli_returncode': 0, 'cleanup_failed': False,
            'temporary_owner_removed': True, 'never_started': False}
        self.write('prepare.remote.json', self.remote)
        self.ready = {'schema': 'hms-case-input-ready-v1', 'local_model_only': True,
            'intent_sha256': self.sha(self.evidence / 'input.intent.json'),
            'remote_sha256': self.sha(self.evidence / 'prepare.remote.json'),
            'publications': self.intent['publications'],
            'facts': {'first_append': {'snapshot_id': 2**63 - 1}}}
        self.write('input.ready.json', self.ready)

    @staticmethod
    def sha(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def write(self, name, value):
        (self.evidence / name).write_text(json.dumps(value))

    def verify(self, *, namespace=None, sibling=None, receipt=None):
        return self.fixture.verify_prepared(self.root, self.evidence,
            self.namespace if namespace is None else namespace,
            self.sibling if sibling is None else sibling,
            self.evidence / 'input.ready.json' if receipt is None else receipt)

    def test_fixed_verify_prepared_cli_uses_exact_owned_receipt_without_execution(self):
        before = {path: self.sha(path) for path in self.evidence.iterdir()}
        output = io.StringIO()
        argv = ['fixture.py', 'verify-prepared', self.namespace,
                '--new-namespace', self.sibling, '--receipt', str(self.evidence / 'input.ready.json')]
        with patch.object(sys, 'argv', argv), contextlib.redirect_stdout(output), \
             patch.object(self.fixture, 'run_spark', side_effect=AssertionError('no Spark')) as spark, \
             patch.object(self.fixture, 'inventory', side_effect=AssertionError('no S3')) as inventory, \
             patch.object(subprocess, 'Popen', side_effect=AssertionError('no process')) as process:
            self.fixture.main()
        self.assertEqual(output.getvalue(), 'HMS_PREPARE_OK\n')
        self.assertEqual(self.verify()['facts']['first_append']['snapshot_id'], 2**63 - 1)
        self.assertEqual({path: self.sha(path) for path in self.evidence.iterdir()}, before)
        spark.assert_not_called()
        inventory.assert_not_called()
        process.assert_not_called()

    def test_foreign_receipt_path_is_rejected_before_loading_any_owner_fact(self):
        foreign = self.root / 'foreign-ready.json'
        foreign.write_bytes((self.evidence / 'input.ready.json').read_bytes())
        argv = ['fixture.py', 'verify-prepared', self.namespace,
                '--new-namespace', self.sibling, '--receipt', str(foreign)]
        with patch.object(sys, 'argv', argv), \
             patch.object(self.fixture, 'hms_intent', side_effect=AssertionError('foreign authority must not be loaded')) as load:
            with self.assertRaisesRegex(RuntimeError, 'receipt path differs'):
                self.fixture.main()
        load.assert_not_called()
        self.assertFalse((self.evidence / 'input.cleanup-attempt.json').exists())
        self.assertFalse((self.evidence / 'input.cleanup.json').exists())

    def test_fixed_verifier_rejects_foreign_namespace_sibling_context_and_storage(self):
        for keyword in ['namespace', 'sibling']:
            with self.subTest(keyword=keyword), self.assertRaisesRegex(RuntimeError, 'differs from its original owner'):
                self.verify(**{keyword: 'iru7_foreign'})
        for key in ['NOVA_FIXTURE_RUN_CONTEXT', 'NOVA_FIXTURE_RUN_STORAGE']:
            with self.subTest(key=key), patch.dict(os.environ, {key: '{}'}):
                with self.assertRaisesRegex(RuntimeError, 'differs from its original owner'):
                    self.verify()

    def test_fixed_verifier_rejects_changed_sources_without_cleanup_authority(self):
        for name in self.sources:
            path = self.root / name
            original = path.read_bytes()
            path.write_bytes(original + b'\nchanged')
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, 'input source changed'):
                self.verify()
            path.write_bytes(original)
        self.assertFalse((self.evidence / 'input.cleanup-attempt.json').exists())

    def test_fixed_verifier_rejects_stale_intent_remote_and_publication_facts(self):
        for field, value, expected in [
            ('intent_sha256', 'foreign-intent', 'intent hash changed'),
            ('remote_sha256', 'foreign-remote', 'remote exit receipt changed'),
            ('publications', {}, 'input publication changed'),
        ]:
            ready = dict(self.ready, **{field: value})
            self.write('input.ready.json', ready)
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, expected):
                self.verify()
        self.assertEqual(self.sha(self.evidence / 'input.intent.json'), self.ready['intent_sha256'])
        self.assertEqual(self.sha(self.evidence / 'prepare.remote.json'), self.ready['remote_sha256'])

    def test_fixed_verifier_rejects_incomplete_or_foreign_remote_settlement(self):
        for field, value in [
            ('schema', 'unknown'), ('context', {}), ('child_waited', False),
            ('returncode', 9), ('returncode', False), ('cli_returncode', 9),
            ('cli_returncode', False), ('cleanup_failed', True),
            ('temporary_owner_removed', False), ('never_started', True),
        ]:
            self.write('prepare.remote.json', dict(self.remote, **{field: value}))
            with self.subTest(field=field, value=value), self.assertRaisesRegex(RuntimeError, 'no successful original remote exit'):
                self.verify()
        self.assertFalse((self.evidence / 'input.cleanup.json').exists())

    def test_fixed_verifier_requires_exact_positive_int64_including_beyond_f64(self):
        for snapshot in [2**53 + 1, 2**63 - 1]:
            ready = dict(self.ready, facts={'first_append': {'snapshot_id': snapshot}})
            self.write('input.ready.json', ready)
            self.assertEqual(self.verify()['facts']['first_append']['snapshot_id'], snapshot)
        for snapshot in [None, False, True, 0, -1, '731', 731.5, 2**63]:
            ready = dict(self.ready, facts={'first_append': {'snapshot_id': snapshot}})
            self.write('input.ready.json', ready)
            with self.subTest(snapshot=snapshot), self.assertRaisesRegex(RuntimeError, 'exact positive Int64'):
                self.verify()


if __name__ == "__main__":
    unittest.main()
