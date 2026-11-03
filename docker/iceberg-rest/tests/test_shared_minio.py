# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Private catalogs retain their lifecycle while sharing only exact machine S3."""
import copy
import json
import os
from pathlib import Path
from unittest import mock

from test_runtime_entry import RendererCase, entry, machine, runtime, bom
from scratch import ScratchIdentity, RunBudget


class SharedMinioTests(RendererCase):
    def private_config(self):
        config = copy.deepcopy(self.config)
        config.update(shared_docker=False, shared_minio=self.snapshot)
        config["credentials"] = self.snapshot["credentials"]
        identity = ScratchIdentity.create("s3://novarocks/test-worktree")
        config["warehouses"] = {k: identity.scope_root + "/" + k for k in config["warehouses"]}
        config["run_context"] = RunBudget.admit(self.snapshot, identity).as_dict()
        config["writer_commands"] = {"rest": ["java", "-jar", "/rest.jar"], "rest-mv": ["java", "-jar", "/mv.jar"],
            "spark": ["/bin/bash", "-lc", "trap 'exit 0' TERM INT; sleep infinity & wait"]}
        config["writer_images"] = {"rest": "sha256:iceberg-rest", "rest-mv": "sha256:rest-mv", "spark": "sha256:iceberg-spark", "mc": "sha256:minio-mc"}
        return config

    def test_model_has_private_catalog_network_and_no_private_minio(self):
        config = self.private_config()
        ports = {**self.snapshot["ports"], "rest": 31002, "rest_mv": 31003, "spark": 31004}
        result = entry.render_isolated_stack(bom(), "nr-isolated-rest-test", ports, self.entry, config)
        record = result["record"]
        text = Path(record["compose_file"]).read_text()
        self.assertNotIn("  minio:\n", text)
        self.assertNotIn("  mc-init:\n", text)
        self.assertNotIn("minio-data", text)
        self.assertIn("  mc:\n", text)
        self.assertIn("name: ${NOVA_FIXTURE_PROJECT}_iceberg_net", text)
        self.assertNotEqual(record["network"], self.snapshot["network"])
        self.assertEqual(set(record["images"]), {"rest", "rest-mv", "spark", "mc"})
        self.assertEqual(record["volumes"], ["nr-isolated-rest-test_rest-catalog", "nr-isolated-rest-test_rest-mv-catalog"])
        entry.render_entry(result["context"], self.entry)
        manifest = json.loads((self.entry / "manifest.json").read_text())
        self.assertEqual(manifest["runtime"]["machine"], self.snapshot)
        self.assertEqual(manifest["runtime"]["catalog"]["project"], record["project"])
        self.assertEqual(manifest["minio"]["volume"], self.snapshot["config"]["minio_volume"])
        environment = self.environment(self.entry / "env.sh")
        self.assertEqual(environment["NOVA_ENV_OBJECT_STORE_CONTAINER"], self.snapshot["containers"]["minio"])
        self.assertEqual(environment["NOVA_FIXTURE_MACHINE_GENERATION"], self.snapshot["generation"])
        self.assertEqual(environment["NOVA_ENV_SHARED_DOCKER"], "false")

    def test_private_shared_s3_requires_original_context_before_any_creation(self):
        with mock.patch.dict(os.environ, {"NOVA_ENV_PRIVATE_SHARED_MINIO": "true"}, clear=True), \
                mock.patch.object(machine, "MachineOwner", return_value=self.owner), \
                mock.patch.object(entry, "start_private_stack") as start:
            with self.assertRaisesRegex(runtime.RuntimeFailure, "RunBudgetRejected"):
                entry.isolated_start(self.config, "nr-isolated-rest-test", self.entry, "stock", None, bom())
            start.assert_not_called()

    def test_private_shared_s3_inherits_deadline_and_server_warehouse(self):
        identity = ScratchIdentity.create("s3://novarocks/test-worktree")
        context = RunBudget.admit(self.snapshot, identity).as_dict()
        with mock.patch.dict(os.environ, {"NOVA_ENV_PRIVATE_SHARED_MINIO": "true", "NOVA_FIXTURE_RUN_CONTEXT": json.dumps(context),
                "NOVA_FIXTURE_MACHINE_GENERATION": self.snapshot["generation"], "NOVA_FIXTURE_MACHINE_ROOT": self.snapshot["control_root"],
                "NOVA_FIXTURE_MACHINE_DAEMON": self.snapshot["daemon_id"]}, clear=True), \
                mock.patch.object(machine, "MachineOwner", return_value=self.owner), \
                mock.patch.object(entry, "start_private_stack", return_value={}) as start:
            entry.isolated_start(self.config, "nr-isolated-rest-test", self.entry, "stock", None, bom())
        config = start.call_args.args[0]
        self.assertEqual(config["warehouses"]["rest_client"], identity.scope_root + "/rest")
        self.assertEqual(config["credentials"], self.snapshot["credentials"])
        self.assertEqual(start.call_args.kwargs["budget"].as_dict(), context)

    def test_expired_original_context_refuses_before_image_or_health_probes(self):
        identity = ScratchIdentity.create("s3://novarocks/test-worktree")
        context = RunBudget.admit(self.snapshot, identity).as_dict()
        context["started_unix_ms"] -= 3600000
        context["deadline_unix_ms"] -= 3600000
        with mock.patch.dict(os.environ, {"NOVA_ENV_PRIVATE_SHARED_MINIO": "true", "NOVA_FIXTURE_RUN_CONTEXT": json.dumps(context),
                "NOVA_FIXTURE_MACHINE_ROOT": self.snapshot["control_root"], "NOVA_FIXTURE_MACHINE_DAEMON": self.snapshot["daemon_id"],
                "NOVA_FIXTURE_MACHINE_GENERATION": self.snapshot["generation"]}, clear=True), \
                mock.patch.object(machine, "MachineOwner", return_value=self.owner), \
                mock.patch.object(self.owner, "required_definition") as definition, \
                mock.patch.object(self.owner, "operation_snapshot") as ready, \
                mock.patch.object(entry, "start_private_stack") as start, \
                mock.patch.object(entry, "build_inputs") as build:
            with self.assertRaisesRegex(runtime.RuntimeFailure, "RunBudgetExpired"):
                entry.isolated_start(self.config, "nr-isolated-rest-test", self.entry, "stock", None)
            build.assert_not_called()
            definition.assert_not_called()
            ready.assert_not_called()
            start.assert_not_called()

    def test_teardown_detaches_exact_shared_minio_and_removes_only_private_project(self):
        project = "nr-isolated-rest-test"
        config = self.private_config()
        result = entry.render_isolated_stack(bom(), project, self.snapshot["ports"], self.entry, config)
        record = result["record"]
        record["object_store_container"] = self.snapshot["containers"]["minio"]
        entry.render_entry(result["context"], self.entry)
        commands = []
        snapshot = self.snapshot
        class Docker:
            def __init__(self, **kwargs): pass
            def validate_resources(self, value):
                commands.append(("validate", value["project"]))
            def inspect(self, kind, identity, **kwargs):
                self_identity = snapshot["containers"]["minio"]
                if kind == "container" and identity == self_identity:
                    return {"Id": identity, "Image": snapshot["images"]["minio"]["image_id"],
                        "Config": {"Labels": machine.MachineDocker.labels(snapshot)}}
                if kind in ("network", "volume"):
                    return None
                raise AssertionError((kind, identity))
            def own_network(self, value):
                return {"Containers": {snapshot["containers"]["minio"]: {}}}
            def command(self, args):
                commands.append(tuple(args))
                return ""
            def compose(self, value, args): commands.append((value["project"], *args))
        with mock.patch.object(runtime, "Docker", Docker), mock.patch.dict(os.environ, {
                "NOVA_ENV_ALLOW_VOLUME_DELETE": "true", "NOVA_ENV_EXPECTED_COMPOSE_PROJECT": project,
                "NOVA_ENV_EXPECTED_MINIO_VOLUME": project + "_minio-data"}):
            entry.isolated_down(self.entry, project, volumes=True, purge=False)
        self.assertEqual(commands, [("validate", project), ("network", "disconnect", record["network"], snapshot["containers"]["minio"]),
            (project, "down", "-v"), ("ps", "-a", "--filter", "label=com.docker.compose.project=" + project, "-q")])
