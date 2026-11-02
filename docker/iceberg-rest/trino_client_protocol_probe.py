#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information. Licensed under the Apache License, Version 2.0.
"""Explicit TRC-1 released-client oracle; never pulls images or changes assertions.

The native System scenario owns registration and the original deadline. This
helper retains raw pages, exact subprocess exits and catalog-first cleanup.
G1 compares same-signature values; a declared future owner never permits a
same-signature value mismatch. Numeric JSON tokens retain their lexical form.
"""
import argparse
from decimal import Decimal
import http.client
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.parse

import fixture_run
import fixture_runtime as runtime
import trino_run

TRINO_RELEASE = "483"
TRINO_MANIFEST = "sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534"
PIN = "trinodb/trino@" + TRINO_MANIFEST
# Only temporal logical-type differences are accepted by the approved G1b route.
DIFFERENCE_OWNERS = {
    "ts_ntz": "TRC-6c", "ts_zoned": "TRC-6c", "time_value": "TRC-6c",
    "time_array": "TRC-6c", "time_map": "TRC-6c",
    "ts_ntz_array": "TRC-6c", "ts_zoned_array": "TRC-6c",
    "nano_ntz": "TRC-6c", "nano_zoned": "TRC-6c",
}
BASE_COLUMNS = ["bool_value", "int_value", "long_value", "real_value", "double_value",
                "decimal_value", "text_value", "binary_value", "date_value", "ts_ntz",
                "ts_zoned", "array_value", "map_value", "row_value", "binary_map",
                "ts_ntz_array", "ts_zoned_array"]
TIME_COLUMNS = ["time_value", "time_array", "time_map"]
VARIANT_COLUMNS = ["variant_value", "variant_array", "variant_map", "variant_row"]


class ProbeBlocked(Exception):
    pass


class ProbeFailure(Exception):
    pass


class JsonNumber(Decimal):
    def __new__(cls, text):
        value = super().__new__(cls, text)
        value.lexical = text
        return value


def parse_page(raw):
    return json.loads(raw, parse_int=JsonNumber, parse_float=JsonNumber,
                      parse_constant=lambda value: (_ for _ in ()).throw(ProbeFailure("non-JSON numeric token: " + value)))


def compare_value(left, right, signature, path="value"):
    """Independent released-engine value comparison with recursive float checks."""
    if left is None or right is None:
        if left is not right:
            raise ProbeFailure(path + ": NULL differs")
        return
    raw_type = signature["rawType"]
    if raw_type in ("real", "double"):
        for value in (left, right):
            if isinstance(value, JsonNumber):
                if not any(c in value.lexical for c in ".eE"):
                    raise ProbeFailure(path + ": floating JSON value used an integer token")
            elif value not in ("NaN", "Infinity", "-Infinity"):
                raise ProbeFailure(path + ": invalid floating protocol value")
    if raw_type == "array":
        if not isinstance(left, list) or not isinstance(right, list) or len(left) != len(right):
            raise ProbeFailure(path + ": array shape differs")
        child = signature["arguments"][0]["value"]
        for index, (a, b) in enumerate(zip(left, right)):
            compare_value(a, b, child, path + "[" + str(index) + "]")
        return
    if raw_type == "row":
        arguments = signature["arguments"]
        if not isinstance(left, list) or not isinstance(right, list) or len(left) != len(right) or len(left) != len(arguments):
            raise ProbeFailure(path + ": row shape differs")
        for index, (a, b, argument) in enumerate(zip(left, right, arguments)):
            compare_value(a, b, argument["value"]["typeSignature"], path + "." + str(index))
        return
    if raw_type == "map":
        if not isinstance(left, dict) or not isinstance(right, dict) or left.keys() != right.keys():
            raise ProbeFailure(path + ": map keys differ")
        child = signature["arguments"][1]["value"]
        for key in left:
            compare_value(left[key], right[key], child, path + "[" + repr(key) + "]")
        return
    if isinstance(left, JsonNumber) or isinstance(right, JsonNumber):
        if not isinstance(left, JsonNumber) or not isinstance(right, JsonNumber) or left != right:
            raise ProbeFailure(path + ": numeric value differs")
    elif isinstance(left, list) or isinstance(right, list):
        if not isinstance(left, list) or not isinstance(right, list) or len(left) != len(right):
            raise ProbeFailure(path + ": generic array differs")
        for index, (a, b) in enumerate(zip(left, right)):
            compare_value(a, b, {"rawType": "variant"}, path + "/" + str(index))
    elif isinstance(left, dict) or isinstance(right, dict):
        if not isinstance(left, dict) or not isinstance(right, dict) or left.keys() != right.keys():
            raise ProbeFailure(path + ": generic object differs")
        for key in left:
            compare_value(left[key], right[key], {"rawType": "variant"}, path + "/" + key)
    elif type(left) is not type(right) or left != right:
        raise ProbeFailure(path + ": value differs")


def compare_column(name, reference, candidate):
    ref_error, candidate_error = reference.get("error"), candidate.get("error")
    if ref_error:
        raise ProbeFailure("released Trino oracle rejected " + name + ": " + ref_error["errorName"])
    ref_sig = reference["columns"][0]["typeSignature"]
    actual_sig = candidate.get("columns", [{}])[0].get("typeSignature")
    if candidate_error or actual_sig != ref_sig:
        owner = DIFFERENCE_OWNERS.get(name)
        if owner is None:
            raise ProbeFailure("unregistered G1b difference for " + name)
        return {"column": name, "owner": owner, "trino_signature": ref_sig,
                "novarocks_signature": actual_sig, "novarocks_error": candidate_error}
    if len(reference["data"]) != len(candidate["data"]):
        raise ProbeFailure(name + ": row count differs")
    for index, (a, b) in enumerate(zip(reference["data"], candidate["data"])):
        if len(a) != 1 or len(b) != 1:
            raise ProbeFailure(name + ": independent column probe returned wrong width")
        compare_value(a[0], b[0], ref_sig, name + "/" + str(index))
    return None


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    runtime.atomic_bytes(path, (json.dumps(value, indent=2, sort_keys=True, default=lambda value: int(value) if isinstance(value, Decimal) and value == value.to_integral_value() else str(value)) + "\n").encode())


class Probe:
    def __init__(self, args):
        self.args = args
        self.root = Path(args.output).resolve()
        self.root.mkdir(parents=True, exist_ok=False)
        self.repository = Path(__file__).resolve().parents[2]
        self.env_file = Path(args.env_file).resolve(strict=True)
        self.values, self.manifest, self.budget = fixture_run.admit(self.env_file)
        if self.budget is None:
            raise ProbeBlocked("a shared fixture with an original RunBudget is required")
        self.purpose = {"encoding-conformance": "trc1_g1", "official-cli": "trc1_cli", "jdbc-probe": "trc1_jdbc"}[args.mode]
        self.namespace = self.budget.identity.resource_name(self.purpose)
        self.scope = self.budget.identity.scope_root + "/rest/" + self.namespace
        self.containers = []
        self.namespace_intent = False
        self.catalog_owned = False
        self.image = None
        self.oracle = None
        self.sequence = 0
        self.ledger()

    def ledger(self):
        write_json(self.root / "resource-intents.json", {
            "schema": 1, "namespace": self.namespace, "purpose": self.purpose, "scope": self.scope,
            "namespace_intent": self.namespace_intent, "catalog_owned": self.catalog_owned,
            "containers": self.containers, "image_id": self.image, "run": self.budget.as_dict(),
        })

    def timeout(self, cleanup=False):
        return max(0.001, self.budget.remaining_seconds(work=not cleanup))

    def command(self, argv, label, *, data=None, check=True, cleanup=False):
        self.sequence += 1
        directory = self.root / (f"command-{self.sequence:04d}-" + label)
        directory.mkdir()
        write_json(directory / "command.json", {"argv": [str(v) for v in argv]})
        environment = runtime.controlled_environment()
        environment.update({"NOVA_ENV_REST_ENV_FILE": str(self.env_file), "NOVAROCKS_WORKSPACE_ROOT": str(self.repository),
                            "NOVA_FIXTURE_RUN_CONTEXT": json.dumps(self.budget.as_dict()),
                            "NOVA_FIXTURE_RUN_STORAGE": os.environ["NOVA_FIXTURE_RUN_STORAGE"]})
        if cleanup:
            environment["NOVA_FIXTURE_CLEANUP_MODE"] = "true"
        result = fixture_run.communicate([str(v) for v in argv], timeout=self.timeout(cleanup),
                                         data=data, capture=True, environment=environment)
        runtime.atomic_bytes(directory / "stdout", result.stdout or b"")
        runtime.atomic_bytes(directory / "stderr", result.stderr or b"")
        write_json(directory / "exit.json", {"returncode": result.returncode, "child_waited": True})
        if check and result.returncode:
            raise ProbeFailure(f"{label} exited {result.returncode}; see {directory}")
        return result

    def preflight(self):
        if shutil.which("docker") is None:
            raise ProbeBlocked("Docker client is unavailable; no image pull attempted")
        if platform.system() != "Linux":
            raise ProbeBlocked("official client host-network execution requires a Linux Docker host")
        candidates = [os.environ.get("NOVA_TRINO_IMAGE"), PIN, TRINO_MANIFEST]
        for candidate in filter(None, candidates):
            result = self.command(["docker", "image", "inspect", candidate], "image-inspect", check=False)
            if result.returncode:
                continue
            info = json.loads(result.stdout)[0]
            if info.get("Id") != TRINO_MANIFEST and not any(value.endswith("@" + TRINO_MANIFEST) for value in (info.get("RepoDigests") or [])):
                raise ProbeFailure("present Trino candidate does not expose the frozen manifest digest")
            self.image = info["Id"]
            break
        if self.image is None:
            raise ProbeBlocked("released Trino483 frozen image is absent; no pull attempted")
        # A host-network child must actually see a loopback socket, not merely accept the flag.
        result = self.run_image(["/bin/sh", "-c", "command -v curl >/dev/null && test -x /usr/bin/trino && curl --silent --show-error --fail --max-time 5 -H 'Host: localhost' \"$1/v1/info\"",
            "probe", self.args.nova], "host-network-prerequisite", check=False)
        if result.returncode:
            raise ProbeBlocked("pinned image lacks curl/CLI or host network cannot reach the original FE loopback listener")
        self.ledger()
        write_json(self.root / "preflight.json", {"status": "PASS", "release": TRINO_RELEASE, "manifest": TRINO_MANIFEST,
                    "image_id": self.image, "host_network_verified_against": self.args.nova})

    def run_image(self, command, label, check=True):
        from private_deadline import command_guard
        directory = self.root / (label + "-container")
        directory.mkdir()
        cidfile = directory / "container-id"
        record = {"name": label, "id": None, "cidfile": str(cidfile), "state": "intent", "role": label}
        self.containers.append(record)
        self.ledger()
        guarded = command_guard(command, self.budget.as_dict())
        try:
            return self.command(["docker", "run", "--rm", "--pull", "never", "--network", "host", "--cidfile", cidfile,
                "--entrypoint", guarded[0], self.image, *guarded[1:]], label, check=check)
        finally:
            if cidfile.is_file():
                identity = cidfile.read_text().strip()
                if not re.fullmatch("[0-9a-f]{64}", identity):
                    raise ProbeFailure("child wrote an invalid exact Docker container identity")
                record.update(id=identity, state="created")
                self.ledger()

    def spark(self, sql, label, cleanup=False):
        path = self.root / (label + ".sql")
        runtime.atomic_bytes(path, sql.encode())
        return self.command([self.repository / "docker/iceberg-rest/spark-sql.sh", path], label, cleanup=cleanup)

    def shell(self, script, label, cleanup=False):
        path = self.root / (label + ".scala")
        runtime.atomic_bytes(path, script.encode())
        return self.command([self.repository / "docker/iceberg-rest/spark-shell.sh", path], label, cleanup=cleanup)

    def start_oracle(self):
        name = "nr-trc1-oracle-" + self.namespace
        self.containers.append({"name": name, "id": None, "state": "intent", "role": "oracle"})
        self.ledger()
        identity = trino_run.start(self.env_file, self.image, name,
                                  self.repository / "tests/datasketches-tck/interop/trino/rest-catalog")
        self.containers[-1].update(id=identity, state="created")
        self.ledger()
        self.oracle = identity
        while True:
            self.budget.check()
            result = self.command(["docker", "exec", identity, "curl", "--silent", "--show-error", "--fail", "http://localhost:8080/v1/info"], "oracle-ready", check=False)
            if result.returncode == 0 and not json.loads(result.stdout).get("starting", True):
                info = json.loads(result.stdout)
                if info.get("nodeVersion", {}).get("version") != TRINO_RELEASE:
                    raise ProbeFailure("frozen image did not self-report released Trino483")
                write_json(self.root / "oracle-info.json", info)
                break
            time.sleep(min(0.5, self.timeout()))

    def request(self, endpoint, method, uri, headers, body=None):
        if endpoint == "trino":
            argv = [sys.executable, Path(__file__).with_name("trino_run.py"), "protocol", "--env-file", self.env_file,
                    "--container", self.oracle, "--", "--request", method]
            for name, value in headers.items():
                argv += ["--header", name + ": " + value]
            if body is not None:
                argv += ["--data-binary", body]
            result = self.command(argv + [uri], "oracle-http")
            head, data = result.stdout.split(b"\r\n\r\n", 1)
            status = int(head.split(b" ", 2)[1])
            return status, head + b"\r\n\r\n", data
        parsed = urllib.parse.urlsplit(uri)
        connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=min(35, self.timeout()))
        try:
            connection.request(method, urllib.parse.urlunsplit(("", "", parsed.path, parsed.query, "")), body=body, headers=headers)
            response = connection.getresponse()
            head = (f"HTTP/{response.version // 10}.{response.version % 10} {response.status} {response.reason}\r\n" + "".join(f"{k}: {v}\r\n" for k, v in response.getheaders()) + "\r\n").encode()
            return response.status, head, response.read()
        finally:
            connection.close()

    def query(self, endpoint, sql, label, capabilities="PARAMETRIC_DATETIME", extra_headers=None):
        directory = self.root / "pages" / label / endpoint
        directory.mkdir(parents=True, exist_ok=False)
        authority = "localhost:8080" if endpoint == "trino" else urllib.parse.urlsplit(self.args.nova).netloc
        headers = {"X-Trino-User": self.args.user, "X-Trino-Client-Capabilities": capabilities, "Host": authority}
        headers.update(extra_headers or {})
        uri = ("http://localhost:8080" if endpoint == "trino" else self.args.nova) + "/v1/statement"
        method, body, ordinal = "POST", sql, 0
        result = {"data": [], "pages": 0}
        write_json(directory / "request.json", {"sql": sql, "capabilities": capabilities, "headers": headers})
        while True:
            status, head, raw = self.request(endpoint, method, uri, headers, body)
            runtime.atomic_bytes(directory / (f"{ordinal:04d}.headers"), head)
            runtime.atomic_bytes(directory / (f"{ordinal:04d}.json"), raw)
            if status != 200:
                raise ProbeFailure(f"{endpoint} HTTP status {status}; see {directory}")
            page = parse_page(raw)
            result["pages"] += 1
            if "columns" in page:
                if "columns" in result and result["columns"] != page["columns"]:
                    raise ProbeFailure("frozen column metadata changed between pages")
                result["columns"] = page["columns"]
            result["data"].extend(page.get("data", []))
            if "error" in page:
                result["error"] = page["error"]
            if "nextUri" not in page:
                if endpoint == "nova" and "data" in page:
                    raise ProbeFailure("NovaRocks terminal page unexpectedly contains data")
                break
            uri, method, body = page["nextUri"], "GET", None
            ordinal += 1
            self.budget.check()
        return result

    def setup_catalog(self):
        source = Path(self.values["NOVAROCKS_ICE_REST_CATALOG_SQL"])
        result = self.query("nova", source.read_text(), "create-nova-catalog")
        if "error" in result:
            raise ProbeFailure("fresh NovaRocks cluster could not create the fixture catalog")
        self.catalog_owned = True
        self.ledger()

    def prepare_tables(self):
        self.namespace_intent = True
        self.ledger()
        ns, scope = self.namespace, self.scope.replace("'", "''")
        sql = f"""SET spark.sql.session.timeZone=UTC;
CREATE NAMESPACE ice_rest.{ns};
CREATE TABLE ice_rest.{ns}.encoding_values (
id INT, bool_value BOOLEAN, int_value INT, long_value BIGINT, real_value FLOAT, double_value DOUBLE,
decimal_value DECIMAL(38,18), text_value STRING, binary_value BINARY, date_value DATE,
ts_ntz TIMESTAMP_NTZ, ts_zoned TIMESTAMP,
array_value ARRAY<DECIMAL(10,4)>, map_value MAP<INT, STRING>,
row_value STRUCT<negative:INT,quoted:STRING,items:ARRAY<DOUBLE>>,
binary_map MAP<BINARY,STRING>, ts_ntz_array ARRAY<TIMESTAMP_NTZ>, ts_zoned_array ARRAY<TIMESTAMP>)
USING iceberg LOCATION '{scope}/encoding_values' TBLPROPERTIES ('format-version'='2');
INSERT INTO ice_rest.{ns}.encoding_values VALUES
(1,true,-7,-9007199254740991,CAST('NaN' AS FLOAT),CAST('Infinity' AS DOUBLE),CAST('-0.125' AS DECIMAL(38,18)),
 'unicode-雪-\\n-quote-"',unhex('0001ff'),DATE '1969-12-31',TIMESTAMP_NTZ '1969-12-31 23:59:59.123456',TIMESTAMP '1969-12-31 23:59:59.123456',
 array(CAST('-0.125' AS DECIMAL(10,4)),CAST(NULL AS DECIMAL(10,4))),map(-3,'negative',2,'two'),
 named_struct('negative',-4,'quoted','value-雪','items',array(CAST('Infinity' AS DOUBLE),CAST('-Infinity' AS DOUBLE))),
 map(unhex('0001ff'),'bytes'),array(TIMESTAMP_NTZ '2024-01-02 03:04:05.000500'),array(TIMESTAMP '2024-01-02 03:04:05.000500')),
(2,false,0,0,CAST('-Infinity' AS FLOAT),CAST('-Infinity' AS DOUBLE),CAST('0.000000000000000001' AS DECIMAL(38,18)),
 '',unhex(''),DATE '2000-02-29',TIMESTAMP_NTZ '2024-01-02 03:04:05',TIMESTAMP '2024-01-02 03:04:05.999999',
 array(),map(),named_struct('negative',0,'quoted','','items',array()),map(),array(),array()),
(3,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL),
(4,true,1,1,CAST('1.25' AS FLOAT),CAST('-0.0' AS DOUBLE),CAST('1.0' AS DECIMAL(38,18)),
 'finite',unhex('7f80'),DATE '2024-01-01',TIMESTAMP_NTZ '2024-01-02 03:04:05.123000',TIMESTAMP '2024-01-02 03:04:05.123000',
 array(CAST('0.5' AS DECIMAL(10,4))),map(1,'one'),named_struct('negative',1,'quoted','finite','items',array(CAST('NaN' AS DOUBLE))),
 map(unhex('7f80'),'finite'),array(TIMESTAMP_NTZ '2024-01-02 03:04:05.123000'),array(TIMESTAMP '2024-01-02 03:04:05.123000'));
CREATE TABLE ice_rest.{ns}.cli_pages USING iceberg LOCATION '{scope}/cli_pages' TBLPROPERTIES ('format-version'='2') AS
SELECT CAST(id AS INT) id, repeat('x', 32768) payload FROM range(96);
"""
        self.spark(sql, "spark-independent-fixture")
        template = self.repository / "tests/system-test-runner/src/scenarios/trino_interop/supplemental.scala"
        self.shell(template.read_text().replace("__NAMESPACE__", ns).replace("__SCOPE__", scope), "spark-independent-time-variant")
        write_json(self.root / "fixture-coverage.json", {"producer": "Spark3.5 SQL plus Iceberg1.11 GenericRecord API within original Spark JVM",
            "base_columns": BASE_COLUMNS, "time_columns": TIME_COLUMNS, "variant_columns": VARIANT_COLUMNS,
            "timestamp_precision_values": [0, 3, 6], "variant": "conditional on both endpoint reads"})

    def encoding(self):
        self.prepare_tables()
        self.start_oracle()
        self.setup_catalog()
        differences, compared = [], []
        for capability in ("", "PARAMETRIC_DATETIME"):
            mode = "legacy-time" if not capability else "parametric-time"
            for table, names in (("encoding_values", BASE_COLUMNS), ("time_values", TIME_COLUMNS)):
                for name in names:
                    sql = f"SELECT {name} FROM ice_rest.{self.namespace}.{table} ORDER BY id"
                    label = mode + "-" + name
                    reference = self.query("trino", sql, label, capability)
                    candidate = self.query("nova", sql, label, capability)
                    difference = compare_column(name, reference, candidate)
                    if difference:
                        differences.append(dict(mode=mode, **difference))
                    else:
                        compared.append({"mode": mode, "column": name, "rows": len(reference["data"])})
                    write_json(self.root / "g1a-comparison.json", {"status": "RUNNING", "compared": compared})
                    write_json(self.root / "g1b-differences.json", {"status": "RUNNING", "registered_owners": DIFFERENCE_OWNERS, "differences": differences})
        # Conditional Variant coverage is discovered by real reads, not by image tag assumptions.
        probes = {endpoint: self.query(endpoint, f"SELECT variant_value FROM ice_rest.{self.namespace}.variant_values ORDER BY id", "variant-support")
                  for endpoint in ("trino", "nova")}
        if any("error" in value for value in probes.values()):
            if any(value.get("error", {}).get("errorName") not in (None, "NOT_SUPPORTED", "UNSUPPORTED_TYPE") for value in probes.values()):
                raise ProbeFailure("Variant support probe failed outside an explicit unsupported capability")
            variant = {"status": "NOT_RUN", "reason": "at least one endpoint rejects the independent Variant table",
                       "endpoint_errors": {name: value.get("error") for name, value in probes.items()}}
        else:
            for capability in ("PARAMETRIC_DATETIME", "PARAMETRIC_DATETIME,VARIANT", "PARAMETRIC_DATETIME,VARIANT,VARIANT_BINARY"):
                for name in VARIANT_COLUMNS:
                    sql = f"SELECT {name} FROM ice_rest.{self.namespace}.variant_values ORDER BY id"
                    label = capability.replace(",", "-") + "-" + name
                    reference = self.query("trino", sql, label, capability)
                    candidate = self.query("nova", sql, label, capability)
                    difference = compare_column(name, reference, candidate)
                    if difference:
                        differences.append(dict(mode=capability, **difference))
                    else:
                        compared.append({"mode": capability, "column": name, "rows": len(reference["data"])})
            variant = {"status": "PASS"}
        write_json(self.root / "g1a-comparison.json", {"status": "PASS", "compared": compared, "variant": variant})
        write_json(self.root / "g1b-differences.json", {"status": "PASS", "registered_owners": DIFFERENCE_OWNERS, "differences": differences})

    def cleanup(self):
        errors = []
        if self.namespace_intent:
            try:
                self.spark("\n".join(f"DROP TABLE IF EXISTS ice_rest.{self.namespace}.{table} PURGE;" for table in
                    ("encoding_values", "time_values", "variant_values", "cli_pages", "cli_ddl")) +
                    f"\nDROP NAMESPACE IF EXISTS ice_rest.{self.namespace};\n", "cleanup-independent-catalog", cleanup=True)
                # Only after catalog removal: reclaim this exact original namespace's
                # uncommitted objects as well as its formerly reachable files.
                scope = self.scope.replace('"', '\\"') + "/"
                self.shell('''try {
  import org.apache.iceberg.aws.s3.S3FileIO
  import scala.collection.JavaConverters._
  val io = new S3FileIO()
  val prefix = "spark.sql.catalog.ice_rest."
  val properties = spark.conf.getAll.filter { case (key, _) => key.startsWith(prefix) }.map { case (key, value) => (key.substring(prefix.length), value) }.asJava
  io.initialize(properties)
  val owned = "''' + scope + '''"
  try { io.deletePrefix(owned); require(!io.listPrefix(owned).iterator().hasNext(), "owned independent producer objects remain") } finally { io.close() }
  println("TRC1_OWNED_PREFIX_CLEANUP_OK")
  System.exit(0)
} catch { case error: Throwable => error.printStackTrace(); System.exit(1) }
''', "cleanup-owned-prefix", cleanup=True)
            except BaseException as error:
                errors.append(str(error))
        if self.catalog_owned:
            try:
                result = self.query("nova", "DROP CATALOG ice_rest", "cleanup-nova-catalog")
                if "error" in result:
                    errors.append("NovaRocks catalog cleanup returned " + result["error"]["errorName"])
            except BaseException as error:
                errors.append(str(error))
        backend = runtime.Docker(timeout=min(90, self.timeout(cleanup=True)))
        backend.operation_deadline = self.budget.deadline
        for record in reversed(self.containers):
            identity = record.get("id")
            if identity is None:
                # Unknown start outcome cannot justify deleting a mutable name.
                errors.append("container creation outcome is unknown: " + record["name"])
                continue
            try:
                info = backend.inspect("container", identity, absent_ok=True)
                if info:
                    if info.get("Id") != identity:
                        raise ProbeFailure("container cleanup lost its exact full ID")
                    backend.command(["container", "rm", "--force", identity])
                    if backend.inspect("container", identity, absent_ok=True):
                        raise ProbeFailure("owned container remains after cleanup")
                record["state"] = "released"
            except BaseException as error:
                record["state"] = "cleanup_failed"
                errors.append(str(error))
        for owner_path in self.root.glob("*-container/owner.json"):
            owner = json.loads(owner_path.read_text())
            if owner.get("state") == "released" and owner.get("child_waited"):
                continue
            if owner.get("state") == "released_after_recovery" and owner.get("external_container_exit_observed"):
                continue
            try:
                if owner.get("run_context") != self.budget.as_dict() or owner.get("image_id") != self.image:
                    raise ProbeFailure("client cleanup lost the original run/image identity")
                identity = owner.get("container_id")
                if identity is None:
                    cidfile = Path(owner["cidfile"])
                    if cidfile.parent != owner_path.parent:
                        raise ProbeFailure("client CID file escaped its original owner directory")
                    identity = cidfile.read_text().strip()
                if not re.fullmatch("[0-9a-f]{64}", identity):
                    raise ProbeFailure("client cleanup has no exact full container ID")
                info = backend.inspect("container", identity, absent_ok=True)
                if info:
                    if info.get("Id") != identity or info.get("Image") != owner["image_id"]:
                        raise ProbeFailure("client cleanup exact container changed")
                    backend.command(["container", "rm", "--force", identity])
                    if backend.inspect("container", identity, absent_ok=True):
                        raise ProbeFailure("client container remains after recovery cleanup")
                prior = owner_path.with_name("prior-owner-" + str(self.sequence) + ".json")
                owner_path.rename(prior)
                owner.update(state="released_after_recovery", external_container_exit_observed=True,
                             prior_owner=str(prior), container_id=identity)
                write_json(owner_path, owner)
            except BaseException as error:
                errors.append("client owner cleanup failed: " + str(error))
        self.ledger()
        write_json(self.root / "cleanup.json", {"complete": not errors, "errors": errors, "containers": self.containers})
        if errors:
            raise ProbeFailure("external cleanup failed: " + "; ".join(errors))

    def cli(self):
        self.prepare_tables()
        self.setup_catalog()
        catalog = "ice_rest"
        sql = f"SELECT id, payload FROM cli_pages ORDER BY id;"
        result = self.invoke_cli(["--catalog", catalog, "--schema", self.namespace, "--output-format", "CSV", "--execute", sql], "cli-multipage")
        if len(result.stdout) <= 1024 * 1024 or result.stdout.count(b"\n") != 96:
            raise ProbeFailure("official CLI did not print the complete multipage fixture")
        # sql_mode is observable through the existing MySQL dialect's || semantics.
        session_sql = f"USE ice_rest.{self.namespace};\nSET sql_mode='PIPES_AS_CONCAT';\nSELECT 'a' || 'b';\nSELECT id FROM cli_pages ORDER BY id LIMIT 1;\n"
        path = self.root / "cli-session.sql"
        runtime.atomic_bytes(path, session_sql.encode())
        result = self.invoke_cli(["--catalog", catalog, "--schema", "default", "--output-format", "CSV", "--file", "/probe/session.sql"],
                                 "cli-session", mount=path)
        if b'"ab"' not in result.stdout or b'"0"' not in result.stdout:
            raise ProbeFailure("official CLI failed to replay USE/Set-Session effects into its next statement")
        ddl = self.invoke_cli(["--catalog", catalog, "--schema", self.namespace, "--execute",
            "CREATE TABLE cli_ddl (id INT)"], "cli-ddl")
        if b"CREATE TABLE" not in ddl.stdout + ddl.stderr:
            raise ProbeFailure("official CLI did not print DDL updateType")
        for name, sql in (("largeint", "SELECT CAST(1 AS LARGEINT)"), ("wide-decimal", "SELECT CAST(1 AS DECIMAL256(50,0))")):
            result = self.invoke_cli(["--execute", sql], "cli-d13-" + name, check=False)
            if result.returncode == 0 or b"sql.admit.client_type_unsupported" not in result.stdout + result.stderr:
                raise ProbeFailure("official CLI did not expose the stable D13 unsupported-type diagnostic")
        self.interrupt_cli()
        write_json(self.root / "official-cli.json", {"status": "PASS", "cases": ["multipage-catalog-schema", "use-and-session-replay", "ddl-updateType", "d13-errors", "sigint-cancellation"]})

    def invoke_cli(self, arguments, label, check=True, mount=None):
        argv = [sys.executable, Path(__file__).with_name("trino_run.py"), "cli", "--env-file", self.env_file,
                "--image", self.image, "--evidence-dir", self.root / (label + "-container"), "--metrics", self.args.metrics, "--"]
        if mount:
            argv += ["--probe-sql-file", str(mount)]
        argv += ["--server", self.args.nova, "--user", self.args.user, *arguments]
        return self.command(argv, label, check=check)

    def interrupt_cli(self):
        # The helper below records the actual full container identity and delivered SIGINT.
        # Native scenario additionally observes the original cluster's resource convergence.
        result = self.invoke_cli(["--probe-interrupt", "--execute",
            f"SELECT COUNT(*) FROM ice_rest.{self.namespace}.cli_pages a CROSS JOIN ice_rest.{self.namespace}.cli_pages b CROSS JOIN ice_rest.{self.namespace}.cli_pages c CROSS JOIN ice_rest.{self.namespace}.cli_pages d CROSS JOIN ice_rest.{self.namespace}.cli_pages e CROSS JOIN ice_rest.{self.namespace}.cli_pages f"],
            "cli-sigint", check=False)
        if result.returncode == 0 or not any(word in (result.stdout + result.stderr).lower() for word in (b"cancel", b"interrupt", b"abort")):
            raise ProbeFailure("official CLI did not report interruption/cancellation")

    def jdbc(self):
        result = self.run_image(["/bin/sh", "-c", "command -v java >/dev/null && find /usr/lib/trino -type f -name 'trino-jdbc-*.jar' -print"], "jdbc-prerequisite", check=False)
        jars = result.stdout.decode().splitlines() if result.returncode == 0 else []
        if not jars:
            write_json(self.root / "jdbc-probe.json", {"status": "NOT_RUN", "reason": "pinned image has no Java runtime and JDBC driver jar pair; no download attempted"})
            return
        source = self.repository / "tests/system-test-runner/src/scenarios/trino_interop/JdbcProbe.java"
        result = self.command([sys.executable, Path(__file__).with_name("trino_run.py"), "jdbc", "--env-file", self.env_file,
            "--image", self.image, "--evidence-dir", self.root / "jdbc-container", "--source", source, "--jdbc-jar", jars[0],
            "--", self.args.nova, self.args.user], "jdbc-probe")
        if b"JDBC_PROBE_OK" not in result.stdout:
            raise ProbeFailure("JDBC probe did not confirm isValid/version")
        write_json(self.root / "jdbc-probe.json", {"status": "PASS", "jar": jars[0]})


def resume_cleanup(args):
    """Reopen only the original recorded resource identities and deadline."""
    probe = Probe.__new__(Probe)
    probe.args = args
    probe.root = Path(args.output).resolve(strict=True)
    probe.repository = Path(__file__).resolve().parents[2]
    probe.env_file = Path(args.env_file).resolve(strict=True)
    ledger = json.loads((probe.root / "resource-intents.json").read_text())
    os.environ["NOVA_FIXTURE_CLEANUP_MODE"] = "true"
    probe.values, probe.manifest, probe.budget = fixture_run.admit(probe.env_file, inherited=ledger["run"])
    if probe.budget.as_dict() != ledger["run"]:
        raise ProbeFailure("cleanup original deadline or identity changed")
    probe.namespace, probe.scope, probe.purpose = ledger["namespace"], ledger["scope"], ledger["purpose"]
    if probe.purpose not in ("trc1_g1", "trc1_cli", "trc1_jdbc") or probe.namespace != probe.budget.identity.resource_name(probe.purpose) or probe.scope != probe.budget.identity.scope_root + "/rest/" + probe.namespace:
        raise ProbeFailure("cleanup resource location differs from the original run")
    probe.containers = ledger["containers"]
    for record in probe.containers:
        if not record.get("id") and record.get("cidfile"):
            candidate = Path(record["cidfile"])
            if candidate.parent.parent != probe.root:
                raise ProbeFailure("cleanup CID file escaped its original retained root")
            if candidate.is_file():
                identity = candidate.read_text().strip()
                if not re.fullmatch("[0-9a-f]{64}", identity):
                    raise ProbeFailure("cleanup CID file does not contain an exact full ID")
                record["id"] = identity
    probe.namespace_intent = ledger["namespace_intent"]
    # The native runner has already shut down the FE. Its invocation-owned local
    # projection does not own external tables or data; Spark still removes those.
    probe.catalog_owned = False
    probe.image, probe.oracle = ledger.get("image_id"), None
    probe.sequence = max([int(path.name.split("-")[1]) for path in probe.root.glob("command-*-*")] or [0])
    for name in ("cleanup.json", "result.json"):
        path = probe.root / name
        if path.exists():
            retained = probe.root / ("prior-" + str(probe.sequence) + "-" + name)
            path.rename(retained)
    return probe


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("encoding-conformance", "official-cli", "jdbc-probe", "cleanup"))
    parser.add_argument("--env-file", required=True)
    parser.add_argument("--nova", required=True)
    parser.add_argument("--user", default="root")
    parser.add_argument("--output", required=True)
    parser.add_argument("--metrics", required=True)
    args = parser.parse_args(argv)
    runtime.install_signal_handlers()
    probe = None
    primary = None
    code = 0
    try:
        if args.mode == "cleanup":
            probe = resume_cleanup(args)
        else:
            probe = Probe(args)
            probe.preflight()
            {"encoding-conformance": probe.encoding, "official-cli": probe.cli, "jdbc-probe": probe.jdbc}[args.mode]()
    except ProbeBlocked as error:
        primary, code = str(error), 75
    except BaseException as error:
        primary, code = str(error), 1
    finally:
        if probe:
            try:
                probe.cleanup()
            except BaseException as error:
                write_json(probe.root / "cleanup-failure.json", {"error": str(error)})
                primary = ((primary + "; ") if primary else "") + str(error)
                code = 1
            write_json(probe.root / "result.json", {"status": "BLOCKED" if code == 75 else ("PASS" if code == 0 else "FAIL"), "exit": code, "error": primary})
    if primary:
        print(primary, file=sys.stderr)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
