# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""Hermetic binding checks; actual Trino execution belongs to P08."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_run
import fixture_runtime
import trino_run

class TrinoTests(unittest.TestCase):
    def setUp(self):
        sweep = patch.object(fixture_run, "startup_sweep", return_value={"complete": True})
        self.sweep = sweep.start()
        self.addCleanup(sweep.stop)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.env = self.root / "env.sh"
        self.env.write_text("export NOVA_ENV_ID=worktree\nexport NOVA_ENV_REST_WAREHOUSE_URI=s3://novarocks/worktree/rest\nexport AWS_S3_ACCESS_KEY_ID=private-key\nexport AWS_S3_SECRET_ACCESS_KEY=private-secret\n")
        self.manifest = {"ready":True, "shared_docker":True,"runtime":{"machine":{"id":"exact-machine","control_root":"/owned/machine","daemon_id":"exact-daemon","version":"exact-version","generation":"exact-generation", "config":{"minio_volume":"exact-volume"},"server_warehouses":{"rest":"s3://warehouse/machine/rest","rest-mv":"s3://warehouse/machine/rest-mv"},
            "network":"captured-machine-network", "policy":{"max_run_seconds":86400,
            "cleanup_grace_seconds":300,"scratch_ttl_seconds":604800}}}}
        (self.root / "manifest.json").write_text(json.dumps(self.manifest))

    def test_start_binds_exact_network_image_argv_and_original_deadline(self):
        calls=[]
        image={"Id":"sha256:captured", "Config":{"Entrypoint":["/bin/original", "two words"], "Cmd":["$(literal)"]}}
        class Backend:
            timeout=90
            def inspect(inner,kind,identity,**kwargs):
                return image if kind=="image" else {"Id":"c"*64,"Image":image["Id"]}
            def command(inner,args):
                self.sweep.assert_called_once()
                calls.append(args)
                envfile=Path(args[args.index("--env-file")+1])
                self.assertEqual(envfile.stat().st_mode & 0o777,0o600)
                self.assertIn("private-secret",envfile.read_text())
                return "c"*64
        with patch.dict(os.environ,{},clear=True):
            context=fixture_run.admit(self.env)[2].as_dict()
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"]=json.dumps(context)
            result=trino_run.start(self.env,"local-pin","unique-run",self.root,Backend())
        args=calls[0]
        self.assertEqual(result,"c"*64)
        self.assertEqual(args[args.index("--network")+1],"captured-machine-network")
        self.assertEqual(args[args.index("--pull")+1],"never")
        self.assertEqual(args[-3:],["/bin/original","two words","$(literal)"])
        self.assertIn("nr_end=" + str(context["deadline_unix_ms"]//1000), "\n".join(args))
        self.assertNotIn("private-secret",json.dumps(args))
        self.assertFalse(envfile_exists(args))
        self.sweep.assert_called_once()
        self.assertEqual(self.sweep.call_args.kwargs["budget"].as_dict(), context)

    def test_malformed_image_command_is_rejected(self):
        for command in ({"Entrypoint":"/bin/guess"},{"Entrypoint":[]},{"Cmd":[None]}):
            with self.subTest(command=command), self.assertRaises(fixture_runtime.RuntimeFailure):
                trino_run.image_command({"Config":command})

    def test_exec_rejects_replaced_identity_before_command(self):
        class Backend:
            def __init__(inner,**kwargs): pass
            def inspect(inner,*args,**kwargs): return {"Id":"replacement"}
        with patch.dict(os.environ,{},clear=True), patch.object(trino_run.runtime,"Docker",Backend), patch.object(fixture_run,"communicate") as run:
            with self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"exact container"):
                trino_run.main(["exec","--env-file",str(self.env),"--container","c"*64,"--","true"])
            run.assert_not_called()
        self.sweep.assert_not_called()

    def test_stop_preserves_native_cleanup_failure(self):
        class Backend:
            def __init__(inner,**kwargs): pass
            def inspect(inner,*args,**kwargs): return {"Id":"c"*64}
            def command(inner,args):
                self.assertEqual(args,["container","rm","--force","c"*64])
                raise fixture_runtime.RuntimeFailure("DockerOperationFailed","native cleanup exit 13")
        with patch.dict(os.environ,{},clear=True):
            context=fixture_run.admit(self.env)[2].as_dict()
            os.environ["NOVA_FIXTURE_RUN_CONTEXT"]=json.dumps(context)
            with patch.object(trino_run.runtime,"Docker",Backend):
                with self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"native cleanup exit 13"):
                    trino_run.main(["stop","--env-file",str(self.env),"--container","c"*64])
        self.sweep.assert_not_called()

    def test_failed_start_settles_only_returned_full_id(self):
        calls=[]
        class Backend:
            timeout=90
            count=0
            def inspect(inner,kind,identity,**kwargs):
                if kind=="image": return {"Id":"sha256:captured","Config":{"Cmd":["/bin/entry"]}}
                inner.count+=1
                return {"Id":identity,"Image":"wrong"} if inner.count==1 else None
            def command(inner,args):
                calls.append(args)
                return "c"*64
        with patch.dict(os.environ,{},clear=True),self.assertRaisesRegex(fixture_runtime.RuntimeFailure,"captured image"):
            trino_run.start(self.env,"pin","unique",self.root,Backend())
        self.assertEqual(calls[-1],["container","rm","--force","c"*64])

def envfile_exists(args):
    return Path(args[args.index("--env-file")+1]).exists()

if __name__=="__main__": unittest.main()
