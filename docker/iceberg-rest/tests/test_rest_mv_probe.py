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

"""REST contract probes against preserving and dropping HTTP fixtures."""

import copy
from contextlib import contextmanager
import gzip
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest import mock
import urllib.parse
import uuid


PATH = Path(__file__).resolve().parents[1] / "rest-mv/probe.py"
SPEC = importlib.util.spec_from_file_location("rest_mv_probe", PATH)
probe = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(probe)


class RestFixture:
    def __init__(self, *, preserve=False, prefix="", collapse_pointer=False, retain_null=False, ordinary_drift=False,
                 location_override=None, namespace_override=None):
        self.preserve = preserve
        self.prefix = prefix
        self.collapse_pointer = collapse_pointer
        self.retain_null = retain_null
        self.ordinary_drift = ordinary_drift
        self.location_override, self.namespace_override = location_override, namespace_override
        self.namespaces = {}
        self.calls = []
        self.config_statuses = []
        self.namespace_properties = {}
        self.warehouse = None
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def process(self):
                content = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                payload = json.loads(content) if content else None
                code, response = fixture.request(self.command, self.path, payload)
                encoded = json.dumps(response).encode() if response is not None else b""
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            do_GET = process
            do_POST = process
            do_DELETE = process

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=0.02), daemon=True)
        self.thread.start()
        self.uri = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def adjusted_version(self, item):
        item = copy.deepcopy(item)
        if not self.preserve or (item.get("storage-table") is None and not self.retain_null):
            item.pop("storage-table", None)
        return item

    def request(self, method, url, payload):
        self.calls.append((method, url, copy.deepcopy(payload)))
        parsed = urllib.parse.urlparse(url)
        if parsed.path == "/v1/config":
            if self.config_statuses:
                return self.config_statuses.pop(0), {}
            if "warehouse" not in urllib.parse.parse_qs(parsed.query):
                return 400, {}
            self.warehouse = urllib.parse.parse_qs(parsed.query)["warehouse"][0]
            return 200, {"defaults": {"prefix": self.prefix}, "overrides": {}}
        base = "/v1/" + (self.prefix + "/" if self.prefix else "") + "namespaces"
        if parsed.path == "/v1/namespaces" and method == "GET":
            return 200, {"namespaces":[[name] for name in self.namespaces]}
        if parsed.path == base and method == "POST":
            namespace = payload["namespace"][0]
            self.namespaces[namespace] = {"views": {}, "tables": {}}
            self.namespace_properties[namespace] = copy.deepcopy(payload.get("properties", {}))
            if self.namespace_override is not None:
                self.namespace_properties[namespace]["location"] = self.namespace_override
            return 200, {"namespace": [namespace], "properties": self.namespace_properties[namespace]}
        if not parsed.path.startswith(base + "/"):
            return 404, {}
        parts = parsed.path[len(base) + 1:].split("/")
        namespace = self.namespaces.get(parts[0])
        if namespace is None:
            return 404, {}
        if len(parts) == 1 and method == "GET":
            return 200, {"namespace":[parts[0]], "properties":self.namespace_properties[parts[0]]}
        if len(parts) == 1 and method == "DELETE":
            if any(namespace.values()):
                return 409, {}
            del self.namespaces[parts[0]]
            return 204, None
        resources = namespace[parts[1]]
        if len(parts) == 2 and method == "POST":
            name = payload["name"]
            if parts[1] == "views":
                pointer = payload["view-version"].get("storage-table")
                if self.preserve and pointer is not None and (not isinstance(pointer, dict)
                        or not isinstance(pointer.get("name"), str) or not isinstance(pointer.get("namespace"), list)):
                    return 400, {}
                item = self.adjusted_version(payload["view-version"])
                if self.ordinary_drift and name == "ordinary_view":
                    item["representations"][0]["sql"] = "SELECT 999 AS id"
                metadata = {"view-uuid": str(uuid.uuid4()), "format-version": 1,
                            "location": payload.get("location", self.warehouse+"/"+parts[0]+"/"+name), "current-version-id": 1,
                            "versions": [item], "schemas": [payload["schema"]],
                            "version-log": [{"version-id": 1, "timestamp-ms": item["timestamp-ms"]}]}
            else:
                metadata = {"table-uuid": str(uuid.uuid4()), "format-version": 2,
                            "location": payload.get("location", self.warehouse+"/"+parts[0]+"/"+name), "current-schema-id": 0,
                            "schemas": [payload["schema"]], "properties": {}}
            if self.location_override is not None:
                metadata["location"] = self.location_override(metadata["location"])
            resources[name] = {"metadata": metadata, "metadata-location": metadata["location"] + "/metadata/1.metadata.json"}
            return 200, resources[name]
        if len(parts) != 3 or parts[2] not in resources:
            return 404, {}
        result = resources[parts[2]]
        if method == "DELETE":
            del resources[parts[2]]
            return 204, None
        if method == "POST":
            metadata = result["metadata"]
            for update in payload["updates"]:
                action = update["action"]
                if action == "add-view-version":
                    item = self.adjusted_version(update["view-version"])
                    old = probe.current_version(metadata)
                    if not (self.collapse_pointer and item["representations"] == old["representations"]):
                        metadata["versions"].append(item)
                elif action == "set-current-view-version":
                    metadata["current-version-id"] = metadata["versions"][-1]["version-id"]
                    metadata["version-log"].append({"version-id": metadata["current-version-id"], "timestamp-ms": 42})
                elif action == "add-schema":
                    metadata["schemas"].append(update["schema"])
                elif action == "set-current-schema":
                    metadata["current-schema-id"] = metadata["schemas"][-1]["schema-id"]
                elif action == "set-properties":
                    metadata["properties"].update(update["updates"])
            result["metadata-location"] = metadata["location"] + "/metadata/2.metadata.json"
        return 200, result


class RestMVProbeTests(unittest.TestCase):
    def setUp(self):
        sweep = mock.patch.object(probe.fixture_run, "startup_sweep", return_value={"complete": True})
        self.startup_sweep = sweep.start()
        self.addCleanup(sweep.stop)

    def fixtures(self, *, stock=None, mv=None):
        stock = RestFixture(**(stock or {}))
        mv = RestFixture(**({"preserve": True} if mv is None else mv))
        self.addCleanup(stock.close)
        self.addCleanup(mv.close)
        return stock, mv, {"ready": True, "shared_docker": False,
                           "iceberg_rest": {"uri": stock.uri, "warehouse": "s3://warehouse/test/rest"},
                           "iceberg_rest_mv": {"uri": mv.uri, "warehouse": "s3://warehouse/test/rest-mv"}}

    def publication(self, manifest, *, grace=300, maximum=86400):
        directory=tempfile.TemporaryDirectory(); self.addCleanup(directory.cleanup)
        root=Path(directory.name).resolve()
        manifest.update(shared_docker=True,runtime={"publication_dir":str(root),"machine":{
            "ports":{"rest":urllib.parse.urlparse(manifest["iceberg_rest"]["uri"]).port,"rest_mv":urllib.parse.urlparse(manifest["iceberg_rest_mv"]["uri"]).port}, "id":"probe-machine", "config":{"minio_volume":"exact-volume"}, "server_warehouses":{"rest":"s3://warehouse/test/rest","rest-mv":"s3://warehouse/test/rest-mv"}, "control_root":"/owned/machine", "daemon_id":"captured-daemon", "version":"captured-version",
            "generation":"captured-generation", "policy":{"max_run_seconds":maximum,
            "cleanup_grace_seconds":grace,"scratch_ttl_seconds":2*(maximum+grace)+1000}}})
        (root/"manifest.json").write_text(json.dumps(manifest))
        env=root/"env.sh"; env.write_text("export NOVA_ENV_ID='probe-worktree'\n")
        return env

    def machine(self, manifest, *, on_mc=None, epoch=None):
        events=[]; held=[0]; outer=self
        snapshot=manifest["runtime"]["machine"]
        class Owner:
            def __init__(inner,root,*,daemon,timeout):
                outer.assertEqual((str(root),daemon),(snapshot["control_root"],snapshot["daemon_id"]))
                outer.assertGreater(timeout,0)
                inner.backend=inner
            @contextmanager
            def operation_snapshot(inner,*,deadline,expected_generation):
                outer.assertEqual(expected_generation,snapshot["generation"])
                if epoch is not None and epoch[0] != expected_generation:
                    raise probe.fixture_runtime.RuntimeFailure("MachineUnavailable","fixture activation changed")
                events.append(("ready",deadline,expected_generation))
                held[0]+=1
                try: yield snapshot
                finally: held[0]-=1
            def run_mc(inner,record,script,*,deadline,binary=False):
                outer.assertEqual(record,snapshot)
                outer.assertGreater(held[0],0)
                events.append(("mc",deadline,script))
                result = on_mc(script) if on_mc else b""
                return result if binary else (result.decode() + "NOVA_SCRATCH_MC_EXIT=0\n")
        return Owner,events,held

    def test_shared_probe_admits_full_run_locations_and_cleans_metadata_before_exact_s3(self):
        stock,mv,manifest=self.fixtures(stock={"prefix":"tenant/stock"}, mv={"preserve":True,"prefix":"tenant/mv"})
        env=self.publication(manifest)
        def sweep(env_file, *, budget):
            self.assertEqual(env_file, env)
            self.assertFalse(stock.calls or mv.calls, "startup sweep must precede Catalog config and DDL")
            self.assertEqual(budget.identity.scope_root.rsplit("/", 1)[-1], budget.identity.run_id)
        self.startup_sweep.side_effect = sweep
        def mc(script):
            owner_fixture=mv if "/rest-mv/" in script else stock
            self.assertEqual(owner_fixture.namespaces,{})
            self.assertIn('${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@minio:9000',script)
            self.assertTrue(all(not item.namespaces for item in (stock,mv)), "both catalogs must be empty before any storage operation")
            self.assertNotIn("shared/benchmarks",script)
            return b""
        owner,events,held=self.machine(manifest,on_mc=mc)
        for fixture in (stock,mv):
            original=fixture.request
            def checked(method,path,payload,original=original):
                self.assertGreater(held[0],0,"every shared HTTP operation must hold the captured epoch gate")
                return original(method,path,payload)
            fixture.request=checked
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner), \
                mock.patch.object(probe,"docker") as docker:
            probe.run_probe(manifest,"contract",env_file=env)
        docker.assert_not_called()
        self.startup_sweep.assert_called_once()
        names=[]
        for fixture,label in ((stock,"rest"),(mv,"rest-mv")):
            namespace_calls=[body for method,path,body in fixture.calls if method=="POST" and path.endswith("/namespaces")]
            namespace=namespace_calls[0]["namespace"][0]; names.append(namespace)
            run=probe.fixture_run.ScratchIdentity.from_dict({"schema":1,"run_id":namespace[:-6],
                "created_unix_ms":int(namespace.split('_')[2]),
                "scope_root":"s3://novarocks/probe-worktree/scratch/"+namespace[:-6]})
            self.assertEqual(namespace,run.resource_name("probe"))
            self.assertEqual(namespace_calls[0]["properties"],{})
            self.assertTrue(all("location" not in body for method,path,body in fixture.calls
                if method=="POST" and path.endswith(("/views","/tables"))))
        self.assertEqual(names[0],names[1],"parity retains the exact default-namespace semantics")
        self.assertEqual(sum(event[0]=="mc" for event in events),3)
        self.assertEqual(sum(event[0]=="ready" for event in events),len(stock.calls)+len(mv.calls)-4+1)
        self.assertEqual(held[0],0)

    def test_nested_probe_leaves_files_to_original_run_owner(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest)
        owner,events,_=self.machine(manifest)
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner):
            original=probe.fixture_run.admit(env)[2].as_dict()
            storage=os.environ["NOVA_FIXTURE_RUN_STORAGE"]
            probe.run_probe(manifest,"contract",env_file=env)
            self.assertEqual(json.loads(os.environ["NOVA_FIXTURE_RUN_CONTEXT"]),original)
            self.assertEqual(os.environ["NOVA_FIXTURE_RUN_STORAGE"],storage)
        self.assertFalse(stock.namespaces or mv.namespaces)
        self.assertFalse(any(event[0]=="mc" for event in events), "nested probe cannot reclaim a live outer run")

    def test_shared_startup_sweep_failure_blocks_config_and_ddl(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest)
        self.startup_sweep.side_effect = probe.fixture_runtime.RuntimeFailure("ScratchCleanupFailed", "startup sweep failed")
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"Catalog") as catalog:
            with self.assertRaisesRegex(probe.fixture_runtime.RuntimeFailure,"startup sweep failed"):
                probe.run_probe(manifest,"contract",env_file=env)
        self.startup_sweep.assert_called_once()
        catalog.assert_not_called()
        self.assertFalse(stock.calls or mv.calls)

    def test_shared_admission_rejects_expired_or_unbound_publication_before_config_or_ddl(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest)
        with mock.patch.dict(os.environ,{},clear=True):
            context=probe.fixture_run.admit(env)[2].as_dict()
        context["started_unix_ms"]-=10000000; context["deadline_unix_ms"]-=10000000
        with mock.patch.dict(os.environ,{"NOVA_FIXTURE_RUN_CONTEXT":json.dumps(context)},clear=True), \
                mock.patch.object(probe,"Catalog") as catalog,mock.patch.object(probe,"MachineOwner") as owner:
            with self.assertRaises(probe.fixture_runtime.RuntimeFailure): probe.run_probe(manifest,"contract",env_file=env)
        catalog.assert_not_called(); owner.assert_not_called()
        self.assertFalse(stock.calls or mv.calls)
        unbound=copy.deepcopy(manifest); unbound["runtime"].pop("machine")
        with self.assertRaisesRegex(probe.EnvironmentFailure,"no machine binding"):
            probe.run_probe(unbound,"contract",env_file=env)

    def test_shared_epoch_change_refuses_same_port_before_ddl_and_cleanup_is_unconfirmed(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest); epoch=[manifest["runtime"]["machine"]["generation"]]
        original=stock.request
        def switch(method,path,payload):
            response=original(method,path,payload)
            epoch[0]="replacement-generation"
            return response
        stock.request=switch
        owner,events,_=self.machine(manifest,epoch=epoch)
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner):
            with self.assertRaisesRegex(probe.EnvironmentFailure,"cleanup failed.*activation changed"):
                probe.run_probe(manifest,"contract",env_file=env)
        self.assertEqual([method for method,_,_ in stock.calls],["GET"])
        self.assertFalse(mv.calls)
        self.assertFalse(any(event[0]=="mc" for event in events))

    def test_shared_foreign_noncanonical_or_sibling_location_is_preserved(self):
        for alter in (lambda value:"s3://warehouse/foreign/table",lambda value:value+"/../escaped",
                      lambda value:value+"/%2e%2e/escaped",lambda value:value.rsplit('/',1)[0]+"-sibling/table"):
            with self.subTest(alter=alter):
                stock,mv,manifest=self.fixtures(stock={"location_override":alter})
                env=self.publication(manifest); owner,events,_=self.machine(manifest)
                with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner):
                    with self.assertRaisesRegex(probe.EnvironmentFailure,"cleanup failed.*unknown actual scope"):
                        probe.run_probe(manifest,"contract",env_file=env)
                self.assertFalse(any(method=="DELETE" for method,_,_ in stock.calls))
                self.assertFalse(any(event[0]=="mc" for event in events))
                self.assertTrue(stock.namespaces)

    def test_shared_namespace_property_does_not_control_jdbc_table_location(self):
        stock,mv,manifest=self.fixtures(stock={"namespace_override":"s3://warehouse/display-only/namespace"})
        env=self.publication(manifest); owner,events,_=self.machine(manifest)
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner):
            probe.run_probe(manifest,"contract",env_file=env)
        self.assertFalse(stock.namespaces or mv.namespaces)


    def test_shared_unknown_http_write_outcome_never_claims_cleanup_from_a_later_absent_get(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest); owner,events,_=self.machine(manifest)
        original=probe._DeadlineConnection
        class UnknownWrite:
            def __init__(inner,*args): inner.connection=original(*args)
            def request(inner,method,*args,**kwargs):
                if method=="POST": raise TimeoutError("transport outcome unknown")
                return inner.connection.request(method,*args,**kwargs)
            def getresponse(inner): return inner.connection.getresponse()
            def close(inner): inner.connection.close()
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner), \
                mock.patch.object(probe,"_DeadlineConnection",UnknownWrite):
            with self.assertRaisesRegex(probe.EnvironmentFailure,"cleanup failed.*unknown actual scope"):
                probe.run_probe(manifest,"contract",env_file=env)
        self.assertFalse(any(method=="DELETE" for method,_,_ in stock.calls))
        self.assertFalse(any(event[0]=="mc" for event in events))

    def test_shared_metadata_cleanup_failure_preserves_objects_and_still_cleans_peer(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest); original=mv.request
        def fail_delete(method,path,payload):
            if method=="DELETE":
                mv.calls.append((method,path,payload))
                return 500,{}
            return original(method,path,payload)
        mv.request=fail_delete
        owner,events,_=self.machine(manifest)
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner):
            with self.assertRaisesRegex(probe.EnvironmentFailure,"cleanup failed.*HTTP 500"):
                probe.run_probe(manifest,"contract",env_file=env)
        scripts=[event[2] for event in events if event[0]=="mc"]
        self.assertEqual(scripts,[], "one catalog cleanup failure blocks every file deletion for this run")
        self.assertTrue(mv.namespaces); self.assertFalse(stock.namespaces)

    def test_shared_native_mc_failure_is_explicit_and_keeps_peer_cleanup(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest)
        def native_failure(script):
            if "/rest-mv/" in script:
                raise probe.fixture_runtime.RuntimeFailure("MachineOperationFailed","native mc exit 7")
            return b""
        owner,events,_=self.machine(manifest,on_mc=native_failure)
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner):
            with self.assertRaisesRegex(probe.EnvironmentFailure,"cleanup failed.*MachineOperationFailed"):
                probe.run_probe(manifest,"contract",env_file=env)
        self.assertEqual(sum(event[0]=="mc" for event in events),2)

    def test_shared_cli_keeps_primary_contract_and_cleanup_failure_visible(self):
        stock,mv,manifest=self.fixtures(mv={"preserve":False})
        env=self.publication(manifest)
        def native_failure(script):
            if "/rest-mv/" in script:
                raise probe.fixture_runtime.RuntimeFailure("MachineOperationFailed","native mc exit 7")
            return b""
        owner,_,_=self.machine(manifest,on_mc=native_failure)
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner), \
                mock.patch("sys.stderr",new_callable=io.StringIO) as error,mock.patch("sys.stdout",new_callable=io.StringIO) as output:
            self.assertEqual(probe.main(["contract","--manifest",str(env.with_name("manifest.json"))]),2)
        self.assertIn("REST MV PRIMARY FAILED:",error.getvalue())
        self.assertIn("storage-table was lost",error.getvalue())
        self.assertIn("cleanup failed",error.getvalue())
        self.assertIn("MachineOperationFailed",error.getvalue())
        self.assertNotIn("PASSED",output.getvalue())

    def test_shared_cleanup_consumes_only_original_reserve_without_readmission(self):
        stock,mv,manifest=self.fixtures()
        env=self.publication(manifest,grace=2,maximum=60)
        owner,events,_=self.machine(manifest)
        original_cleanup=probe.Catalog.cleanup; delayed=[False]; budgets=[]
        original_admit=probe.fixture_run.admit
        def admit(*args,**kwargs):
            result=original_admit(*args,**kwargs); budgets.append(result[2]); return result
        def cleanup(client):
            if not delayed[0]:
                delayed[0]=True
                time.sleep(client.budget.remaining_seconds(work=True)+0.03)
                self.assertEqual(client.budget.remaining_seconds(work=True),0)
            return original_cleanup(client)
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(probe,"MachineOwner",owner), \
                mock.patch.object(probe.fixture_run,"admit",side_effect=admit),mock.patch.object(probe.Catalog,"cleanup",cleanup):
            probe.run_probe(manifest,"contract",env_file=env,budget_seconds=4)
        self.assertEqual(len(budgets),1)
        self.assertTrue(all(event[1]==budgets[0].deadline for event in events if event[0]=="mc"))
        self.assertFalse(stock.namespaces or mv.namespaces)

    def test_shared_slow_headers_cannot_reset_original_work_deadline(self):
        stock,mv,manifest=self.fixtures()
        class SlowHeaders(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_GET(self):
                try:
                    self.wfile.write(b"HTTP/1.1 200 OK\r\nX-Slow: "); self.wfile.flush()
                    for _ in range(100):
                        self.wfile.write(b"a"); self.wfile.flush(); time.sleep(0.01)
                    self.wfile.write(b"\r\nContent-Length: 2\r\n\r\n{}")
                except OSError: pass
        server=ThreadingHTTPServer(("127.0.0.1",0),SlowHeaders)
        thread=threading.Thread(target=lambda:server.serve_forever(poll_interval=0.02),daemon=True); thread.start()
        self.addCleanup(thread.join); self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        manifest["iceberg_rest"]["uri"]="http://127.0.0.1:"+str(server.server_port)
        env=self.publication(manifest,grace=2,maximum=60)
        run=probe.fixture_run.ScratchIdentity.create("s3://novarocks/probe-worktree",now_ms=int(time.time()*1000)-1800)
        start=run.as_dict()["created_unix_ms"]
        context={"schema":1,"identity":run.as_dict(),"started_unix_ms":start,"deadline_unix_ms":start+4000,"cleanup_grace_seconds":2}
        owner,events,_=self.machine(manifest)
        began=time.monotonic()
        with mock.patch.dict(os.environ,{"NOVA_FIXTURE_RUN_CONTEXT":json.dumps(context)},clear=True),mock.patch.object(probe,"MachineOwner",owner):
            with self.assertRaisesRegex(probe.EnvironmentFailure,"original deadline"):
                probe.run_probe(manifest,"contract",env_file=env)
        self.assertLess(time.monotonic()-began,0.8)
        self.assertFalse(any(event[0]=="mc" for event in events))

    def test_contract_accepts_preserving_mv_and_dropping_stock_with_config_prefix(self):
        stock, mv, manifest = self.fixtures(stock={"prefix": "tenant/stock"}, mv={"preserve": True, "prefix": "tenant/mv"})
        probe.run_probe(manifest, "contract")
        self.startup_sweep.assert_not_called()
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})
        self.assertTrue(any(path.startswith("/v1/tenant/mv/namespaces/") for _, path, _ in mv.calls))
        self.assertTrue(any("warehouse=" in path for _, path, _ in mv.calls))

    def test_dropping_mv_is_rejected_and_both_catalogs_are_cleaned(self):
        stock, mv, manifest = self.fixtures(mv={"preserve": False})
        with self.assertRaisesRegex(probe.ContractFailure, "storage-table was lost"):
            probe.run_probe(manifest, "contract")
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})

    def test_pointer_only_version_collapse_and_null_retention_are_rejected(self):
        for defect, message in (("collapse_pointer", "did not advance"), ("retain_null", "null storage-table")):
            with self.subTest(defect=defect):
                stock, mv, manifest = self.fixtures(mv={"preserve": True, defect: True})
                with self.assertRaisesRegex(probe.ContractFailure, message):
                    probe.run_probe(manifest, "contract")
                self.assertEqual(stock.namespaces, {})
                self.assertEqual(mv.namespaces, {})

    def test_reference_parity_requires_both_catalogs_to_drop_storage_table(self):
        stock, reference, manifest = self.fixtures(mv={"preserve": False})
        probe.run_probe(manifest, "parity", expect_dropped=True)
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(reference.namespaces, {})

    def test_ordinary_view_drift_is_not_normalized_away(self):
        stock, mv, manifest = self.fixtures(mv={"preserve": True, "ordinary_drift": True})
        with self.assertRaisesRegex(probe.ContractFailure, "ordinary view or table behavior differs"):
            probe.run_probe(manifest, "contract")
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})

    def test_lifecycle_refuses_shared_or_foreign_projects_before_any_requests(self):
        for shared, project in ((True, "nr-isolated-rest-test"), (False, "nr-fx-shared"), ("false", "nr-isolated-rest-test")):
            with self.subTest(shared=shared, project=project):
                manifest = {"ready": True, "shared_docker": shared, "compose_project": project}
                with mock.patch.object(probe, "Catalog") as catalog, mock.patch.object(probe, "docker") as docker:
                    with self.assertRaisesRegex(probe.EnvironmentFailure, "requires an isolated"):
                        probe.run_probe(manifest, "lifecycle")
                catalog.assert_not_called()
                docker.assert_not_called()

    def test_lifecycle_reads_metadata_and_recreates_only_mv_from_saved_compose(self):
        stock, mv, manifest = self.fixtures()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "compose.yml").write_text("saved fixture\n")
            (root / "compose.env").write_text("saved environment\n")
            manifest.update(shared_docker=False, compose_project="nr-isolated-rest-probe-test",
                            compose_file=str(root / "compose.yml"), compose_env=str(root / "compose.env"))
            commands = []
            def docker(command, *, binary=False):
                commands.append(command)
                if "run" in command:
                    self.assertTrue(binary)
                    namespace = next(iter(mv.namespaces.values()))
                    return json.dumps(namespace["views"]["storage_view"]["metadata"]).encode()
                mv.config_statuses.append(503)
                return ""
            with mock.patch.object(probe, "docker", side_effect=docker):
                probe.run_probe(manifest, "lifecycle")
            self.assertEqual(len(commands), 2)
            self.assertIn(str(root / "compose.yml"), commands[0])
            self.assertIn(str(root / "compose.env"), commands[0])
            self.assertIn("mc cat", commands[0][-1])
            self.assertEqual(commands[1][-5:], ["up", "-d", "--no-deps", "--force-recreate", "rest-mv"])
        invalid = [body for method, _, body in mv.calls if method == "POST" and body and body.get("name", "").startswith("invalid_view_")]
        self.assertEqual(len(invalid), 2)
        self.assertEqual(stock.namespaces, {})
        self.assertEqual(mv.namespaces, {})

    def test_lifecycle_readiness_does_not_hide_non_transient_http_contract_errors(self):
        client = mock.Mock()
        client.warehouse = "s3://warehouse/test/rest-mv"
        client.request.side_effect = probe.HTTPStatusFailure(404, "missing config endpoint")
        response = {"metadata": {"view-uuid": "test"}, "metadata-location": "s3://warehouse/test/v1.metadata.json"}
        with mock.patch.object(probe, "isolated_compose", return_value=["docker", "compose"]), mock.patch.object(
            probe, "docker", side_effect=[json.dumps(response["metadata"]).encode(), ""]
        ), mock.patch.object(probe.time, "sleep") as sleep:
            with self.assertRaisesRegex(probe.HTTPStatusFailure, "missing config endpoint"):
                probe.lifecycle({}, client, response)
        sleep.assert_not_called()

    def test_metadata_reads_plain_and_both_iceberg_gzip_filename_forms(self):
        metadata = {"view-uuid": "test", "versions": [{"storage-table": {"name": "storage", "namespace": ["ns"]}}]}
        plain = json.dumps(metadata).encode()
        for filename, content in (("v1.metadata.json", plain),
                                  ("v1.gz.metadata.json", gzip.compress(plain)),
                                  ("v1.metadata.json.gz", gzip.compress(plain))):
            with self.subTest(filename=filename):
                self.assertEqual(probe.metadata_json(content, "s3://warehouse/test/" + filename), metadata)

    def test_metadata_rejects_corrupt_gzip_and_codec_mismatch_without_fallback(self):
        compressed = gzip.compress(b'{"view-uuid":"test"}')
        for filename, content in (("v1.gz.metadata.json", compressed[:-4]),
                                  ("v1.gz.metadata.json", b'{}'),
                                  ("v1.metadata.json", compressed)):
            with self.subTest(filename=filename, content=content):
                with self.assertRaisesRegex(probe.ContractFailure, "persisted metadata is not valid"):
                    probe.metadata_json(content, "s3://warehouse/test/" + filename)

    def test_docker_captures_binary_metadata_without_text_decoding(self):
        compressed = gzip.compress(b'{"view-uuid":"test"}')
        result = mock.Mock(returncode=0, stdout=compressed)
        with mock.patch.object(probe.subprocess, "run", return_value=result) as run:
            self.assertEqual(probe.docker(["docker", "compose", "run", "mc"], binary=True), compressed)
        self.assertFalse(run.call_args.kwargs["text"])

    def test_main_exit_codes_distinguish_contract_and_environment_errors(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "manifest.json"
            path.write_text("{}")
            for failure, code in ((None, 0), (probe.ContractFailure("bad contract"), 1), (probe.EnvironmentFailure("bad fixture"), 2),
                                  (probe.fixture_runtime.RuntimeFailure("RunBudgetRejected", "invalid complete budget"),75)):
                with self.subTest(code=code), mock.patch.object(probe, "run_probe", side_effect=failure), mock.patch("sys.stdout", new_callable=io.StringIO), mock.patch("sys.stderr", new_callable=io.StringIO):
                    self.assertEqual(probe.main(["contract", "--manifest", str(path)]), code)


if __name__ == "__main__":
    unittest.main()
