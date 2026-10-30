#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Real stock Iceberg REST metadata producer, Python stdlib only.

No service lifecycle, retries, credentials, native SQL, or synthetic REST facts.
CLI: --freeze BOUND.json --phase preflight|bulk|verify-after-drop --output NEW_DIRECTORY
Bulk additionally requires --preflight-receipt PREFLIGHT_PASS.json.
Read-only verify-after-drop requires --ready-receipt READY.json. It verifies
namespace-list absence and independently reloads every surviving object; it does
not infer an HTTP error type or establish that native SQL executed the DROP.
--self-test exercises local parsers only, without HTTP/provider acceptance.

Wire source: apache/iceberg ccb8bc435062171e64bc8b7e5f56e6aed9c5b934.
RESTCatalogServlet returns 200 (including empty DELETE responses). The canonical
TableMetadataParser permits absent snapshots as empty, but list collections are
mandatory. Pagination accepts omitted/null continuation as terminal; every
nonterminal token must be a new nonempty string. No guessed continuation token.
"""

import argparse
import hashlib
import http.client
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import stat
import sys
import tempfile
import threading
import time
import unittest
from urllib.parse import quote, urlencode, urlsplit
import uuid


class Refusal(Exception):
    """Messages contain fixed reasons only, never remote bodies or credentials."""


NORMAL = {
    "namespaces": 32, "tables_per_namespace": 512, "views_per_namespace": 512,
    "page_size": 256, "namespace_pattern": "cl_ns_%04d",
    "table_pattern": "cl_table_%06d", "view_pattern": "cl_view_%06d",
    "concurrency": [1, 8, 16],
}
ORIGINAL_SHA256 = "e279724dc4ab2ce34dfdef5f3a939ad3f0f05ed076c36c5c60a7b7d7c6c1a3d3"
BOUNDS = {
    "request_timeout_seconds": 30, "preflight_deadline_seconds": 300,
    "bulk_deadline_seconds": 7200, "max_response_bytes": 16777216,
    "max_request_bytes": 65536, "page_size": 256,
}
SOURCE = {
    "canonical_lock_sha256": "60ffd560721ee8d65ba55da5b7dde44e4e3a4992fa9d77443eea75c27cc37d44",
    "image_source": "apache/iceberg-rest-fixture", "platform": "linux/arm64",
    "image_digest": "sha256:f7d679d30ac9c640bdeb2c015dff533cd3c8f1c7d491ebcb5d436f9a42db1d6f",
    "image_alias": "novarocks/fixture-iceberg-rest:f7d679d30ac9",
    "jar_sha256": "49d01ec8e0995001a92b4c880adfe184d602541ba39d135f105511101da4db7d",
    "jar_bytes": 211889894, "iceberg_commit": "ccb8bc435062171e64bc8b7e5f56e6aed9c5b934",
    "iceberg_tag": "apache-iceberg-1.10.1", "iceberg_build_version": "1.11.0-SNAPSHOT",
}
SCHEMA = {"type": "struct", "schema-id": 0, "fields": [
    {"id": 1, "name": "v", "required": False, "type": "long"}]}
BINDING_KEYS = {
    "rest_uri", "warehouse", "server_default_warehouse", "fixture_id", "compose_project", "owner_mode",
    "owner_manifest_path", "owner_manifest_sha256", "rest_image_id",
}
FREEZE_KEYS = {
    "schema_version", "task", "frozen_before_execution", "original_input",
    "normal", "bounds", "source", "producer_sha256", "binding",
    "view_timestamp_ms", "scope",
}
REQUIRED_ENDPOINTS = {
    "GET /v1/{prefix}/namespaces", "POST /v1/{prefix}/namespaces",
    "DELETE /v1/{prefix}/namespaces/{namespace}",
    *{f"{method} /v1/{{prefix}}/namespaces/{{namespace}}/{kind}{suffix}"
      for kind in ("tables", "views")
      for method, suffix in (("GET", ""), ("POST", ""),
                             ("GET", "/{table}" if kind == "tables" else "/{view}"),
                             ("DELETE", "/{table}" if kind == "tables" else "/{view}"))},
}


def need(condition, reason):
    if not condition:
        raise Refusal(reason)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("utf-8")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def strict_json(data):
    def pairs(values):
        result = {}
        for key, value in values:
            need(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise Refusal("nonfinite JSON number")

    try:
        return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError):
        raise Refusal("invalid JSON response") from None


def bounded_file(path, maximum):
    with Path(path).open("rb") as handle:
        data = handle.read(maximum + 1)
    need(len(data) <= maximum, "local manifest exceeds bound")
    return data


def digest_string(value):
    return isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def safe_location(value, warehouse):
    need(isinstance(value, str) and len(value) <= 4096, "invalid metadata location")
    parsed = urlsplit(value)
    need(parsed.scheme == "s3" and parsed.username is None and parsed.password is None
         and not parsed.query and not parsed.fragment, "unsafe metadata location")
    need(value.startswith(warehouse.rstrip("/") + "/"), "metadata outside private warehouse")
    need(not any(part in (".", "..") for part in parsed.path.split("/")),
         "noncanonical metadata location")
    return value


def validate_freeze(path):
    raw = bounded_file(path, 65536)
    freeze = strict_json(raw)
    need(type(freeze) is dict and set(freeze) == FREEZE_KEYS, "invalid freeze fields")
    need(type(freeze["schema_version"]) is int and freeze["schema_version"] == 1,
         "unsupported freeze schema")
    need(freeze["task"] == "MEM-1-M07" and freeze["frozen_before_execution"] is True,
         "producer input is not frozen")
    need(encoded(freeze["normal"]) == encoded(NORMAL),
         "original normal input changed")
    need(freeze["bounds"] == BOUNDS and all(type(x) is int for x in freeze["bounds"].values()),
         "producer bounds changed")
    need(freeze["source"] == SOURCE, "source identity differs from reviewed jar")
    need(type(freeze["view_timestamp_ms"]) is int and freeze["view_timestamp_ms"] > 0,
         "invalid frozen view timestamp")
    need(freeze["scope"] == "external-producer-only; native/SQL deadlines remain independent",
         "invalid producer scope")
    need(freeze["producer_sha256"] == sha(Path(__file__).read_bytes()),
         "producer source differs from freeze")
    original = freeze["original_input"]
    need(type(original) is dict and set(original) == {"path", "sha256"}
         and original["sha256"] == ORIGINAL_SHA256, "invalid original input binding")
    original_bytes = bounded_file(original["path"], 65536)
    need(sha(original_bytes) == original["sha256"], "original input digest mismatch")
    original_json = strict_json(original_bytes)
    need(original_json["schema_version"] == 3 and original_json["spec_revision"] == 7
         and original_json["frozen_before_execution"] is True
         and encoded(original_json["normal"]) == encoded(NORMAL), "original v3 normal input mismatch")
    binding = freeze["binding"]
    need(type(binding) is dict and set(binding) == BINDING_KEYS, "invalid private binding fields")
    need(all(isinstance(x, str) and x and len(x) <= 4096 for x in binding.values()),
         "missing actual private binding")
    # Runtime env_id is a stable hash of the actual workspace; explicit Compose
    # project is independent. Neither may be synthesized from the other.
    need(binding["owner_mode"] == "task-private"
         and re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,95}", binding["compose_project"]),
         "invalid task-private owner or Compose project")
    uri = urlsplit(binding["rest_uri"])
    need(uri.scheme == "http" and uri.hostname and uri.port and uri.path in ("", "/")
         and uri.username is None and uri.password is None and not uri.query and not uri.fragment,
         "unsupported stock REST URI")
    try:
        host = ipaddress.ip_address(uri.hostname)
    except ValueError:
        raise Refusal("numeric private REST address required; no unbounded DNS") from None
    need(host.is_loopback, "stock private REST endpoint is not loopback")
    for field in ("warehouse", "server_default_warehouse"):
        wh = urlsplit(binding[field])
        need(wh.scheme == "s3" and wh.netloc and wh.path and wh.path != "/"
             and wh.username is None and wh.password is None and not wh.query and not wh.fragment
             and not any(part in (".", "..") for part in wh.path.split("/")),
             "invalid private warehouse")
    need(binding["rest_image_id"] == SOURCE["image_digest"], "private image differs from source")
    need(digest_string(binding["owner_manifest_sha256"]), "invalid owner manifest digest")
    manifest_path = Path(binding["owner_manifest_path"])
    need(manifest_path.is_absolute(), "actual owner manifest path must be absolute")
    owner_bytes = bounded_file(manifest_path, 1048576)
    need(sha(owner_bytes) == binding["owner_manifest_sha256"], "owner manifest digest mismatch")
    owner = strict_json(owner_bytes)
    need(type(owner) is dict and owner.get("ready") is True and owner.get("shared_docker") is False
         and owner.get("compose_project") == binding["compose_project"]
         and owner.get("env_id") == binding["fixture_id"],
         "owner is not ready and private")
    need(type(owner.get("iceberg_rest")) is dict
         and owner["iceberg_rest"].get("uri") == binding["rest_uri"]
         and owner["iceberg_rest"].get("warehouse") == binding["warehouse"]
         and owner["iceberg_rest"].get("server_default_warehouse") == binding["server_default_warehouse"],
         "actual owner endpoint or warehouse mismatch")
    try:
        image = owner["runtime"]["catalog"]["images"]["rest"]
        # The stock nonshared runtime entry resolves image_id through Docker
        # at isolated_start while retaining the immutable BOM tag;
        # this helper consumes that explicit binding, without invoking Docker.
        need(image["image_id"] == SOURCE["image_digest"]
             and image["tag"] == SOURCE["image_alias"], "owner REST image digest or alias mismatch")
        need(owner["runtime"]["profile"] == "stock"
             and owner["runtime"]["producer_receipt"]["lock_sha256"] == SOURCE["canonical_lock_sha256"]
             and owner["runtime"]["catalog"]["project"] == binding["compose_project"],
             "owner stock profile or BOM mismatch")
        need(Path(owner["runtime"]["publication_dir"]).resolve(strict=True) == manifest_path.parent.resolve(strict=True)
             and Path(owner["runtime"]["entry_root"]).resolve(strict=True)
             == Path(owner["current_dir"]).resolve(strict=True)
             and Path(owner["runtime"]["entry_root"]).name == binding["fixture_id"],
             "owner publication or entry identity mismatch")
    except (KeyError, TypeError):
        raise Refusal("actual owner image facts missing") from None
    return freeze, sha(raw)


def parse_config(value, warehouse):
    need(type(value) is dict, "config response must be object")
    for field in ("defaults", "overrides"):
        need(type(value.get(field)) is dict and all(isinstance(k, str) and isinstance(v, str)
             for k, v in value[field].items()), "config properties missing or malformed")
        need(value[field].get("prefix", "") == "", "nonempty REST prefix unsupported")
        if "warehouse" in value[field]:
            need(value[field]["warehouse"] == warehouse, "config warehouse differs from owner")
    if "endpoints" in value:
        endpoints = value["endpoints"]
        need(type(endpoints) is list and all(isinstance(e, str) for e in endpoints)
             and len(set(endpoints)) == len(endpoints), "invalid config capability declaration")
        need(REQUIRED_ENDPOINTS.issubset(set(endpoints)), "declared capabilities omit required endpoint")
    return {"endpoints_present": "endpoints" in value,
            "endpoints_sha256": sha(encoded(value.get("endpoints"))),
            "capability_proof": "actual preflight create/load/page/delete required"}


class PageCollector:
    """Independent exact set oracle; never substitutes missing fields with []."""

    def __init__(self, expected, page_size, namespace=None):
        self.expected = set(expected)
        self.size = page_size
        self.namespace = namespace
        self.seen = set()
        self.tokens = set()
        self.pages = 0
        self.finished = False

    def add(self, value):
        need(not self.finished and type(value) is dict, "page after terminal or invalid page")
        key = "namespaces" if self.namespace is None else "identifiers"
        need(key in value and type(value[key]) is list, "listing collection missing or malformed")
        items = value[key]
        need(len(items) <= self.size, "page exceeds requested size")
        self.pages += 1
        need(self.pages <= max(1, len(self.expected) + 1), "excessive pagination")
        for item in items:
            if self.namespace is None:
                need(type(item) is list and len(item) == 1 and isinstance(item[0], str),
                     "unexpected namespace shape")
                name = item[0]
            else:
                need(type(item) is dict and set(item) == {"namespace", "name"}
                     and item["namespace"] == [self.namespace] and isinstance(item["name"], str),
                     "identifier namespace or shape mismatch")
                name = item["name"]
            need(name in self.expected, "extra listing name")
            need(name not in self.seen, "duplicate listing name")
            self.seen.add(name)
        token = value.get("next-page-token")
        if token is None:
            need(self.seen == self.expected, "listing omitted expected names")
            self.finished = True
        else:
            need(isinstance(token, str) and 0 < len(token.encode("utf-8")) <= 4096,
                 "invalid continuation token")
            need(token not in self.tokens, "pagination token cycle")
            need(items, "nonterminal page made no progress")
            self.tokens.add(token)
        return token


def metadata_fact(response, kind, namespace, name, table, warehouse):
    need(type(response) is dict and type(response.get("metadata")) is dict,
         "load response missing actual metadata")
    metadata = response["metadata"]
    metadata_location = safe_location(response.get("metadata-location"), warehouse)
    location = safe_location(metadata.get("location"), warehouse)
    uuid_field = "table-uuid" if kind == "tables" else "view-uuid"
    identifier = metadata.get(uuid_field)
    try:
        need(isinstance(identifier, str) and str(uuid.UUID(identifier)) == identifier,
             "invalid metadata UUID")
    except ValueError:
        raise Refusal("invalid metadata UUID") from None
    expected_format = 2 if kind == "tables" else 1
    need(type(metadata.get("format-version")) is int
         and metadata["format-version"] == expected_format, "unexpected metadata format version")
    schemas = metadata.get("schemas")
    need(type(schemas) is list and len(schemas) == 1 and schemas[0] == SCHEMA
         and type(schemas[0].get("schema-id")) is int
         and type(schemas[0]["fields"][0].get("id")) is int
         and type(schemas[0]["fields"][0].get("required")) is bool,
         "actual metadata schema differs from frozen schema")
    if kind == "tables":
        need(type(metadata.get("current-schema-id")) is int and metadata["current-schema-id"] == 0,
             "table current schema mismatch")
        # Exact source parser lines 512-525 permits absent snapshots as empty.
        need("snapshots" not in metadata or metadata["snapshots"] == [], "table is not empty")
        need("current-snapshot-id" not in metadata or metadata["current-snapshot-id"] in (None, -1),
             "table has current snapshot")
    else:
        current = metadata.get("current-version-id")
        versions = metadata.get("versions")
        need(type(current) is int and current > 0 and type(versions) is list and len(versions) == 1,
             "invalid current view version")
        version = versions[0]
        need(type(version) is dict and type(version.get("version-id")) is int
             and version["version-id"] == current
             and type(version.get("schema-id")) is int and version["schema-id"] == 0
             and type(version.get("timestamp-ms")) is int and version["timestamp-ms"] > 0
             and type(version.get("summary")) is dict
             and version.get("default-namespace") == [namespace]
             and version.get("representations") == [
                 {"type": "sql", "sql": "SELECT v FROM " + table, "dialect": "spark"}],
             "view schema, namespace or SQL differs from frozen definition")
        need(version.get("default-catalog") in (None, ""), "unexpected default catalog")
        history = metadata.get("version-log")
        need(type(history) is list and len(history) == 1 and type(history[0]) is dict
             and type(history[0].get("version-id")) is int and history[0]["version-id"] == current
             and type(history[0].get("timestamp-ms")) is int and history[0]["timestamp-ms"] > 0,
             "invalid view history")
    return {"namespace": namespace, "kind": kind, "name": name, "uuid": identifier,
            "metadata_location": metadata_location, "location": location,
            "schema_sha256": sha(encoded(schemas)), "metadata_sha256": sha(encoded(metadata))}


def atomic_json(path, value, deadline=None):
    path = Path(path)
    need(not path.exists(), "receipt already exists")
    descriptor, temporary = tempfile.mkstemp(prefix=".receipt-", dir=path.parent)
    published = False
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded(value) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        need(deadline is None or time.monotonic() < deadline, "preparation deadline expired before publication")
        os.replace(temporary, path)
        published = True
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    except BaseException:
        # A failed durability step cannot leave a success receipt behind.
        if published:
            path.unlink()
        raise
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


LEDGER_FILE_BYTES = 64 * 1024 * 1024
LEDGER_LINE_BYTES = 64 * 1024
FACT_KEYS = {"namespace", "kind", "name", "uuid", "metadata_location", "location",
             "schema_sha256", "metadata_sha256"}


def prepared_identifiers():
    return {(NORMAL["namespace_pattern"] % namespace, kind, pattern % index)
            for namespace in range(NORMAL["namespaces"])
            for kind, pattern, count in (
                ("tables", NORMAL["table_pattern"], NORMAL["tables_per_namespace"]),
                ("views", NORMAL["view_pattern"], NORMAL["views_per_namespace"]))
            for index in range(count)}


def verification_input(path, maximum):
    # A FIFO/device cannot turn a bounded local ledger read into an unbounded wait.
    handle = os.fdopen(os.open(path, os.O_RDONLY | os.O_NONBLOCK), "rb")
    try:
        facts = os.fstat(handle.fileno())
        need(stat.S_ISREG(facts.st_mode) and facts.st_size <= maximum,
             "verification input is not a bounded regular file")
    except BaseException:
        handle.close()
        raise
    return handle


def read_ledger(path, expected_sha, consume, check_deadline):
    """Bound both physical input and each JSON line, including its terminator."""
    need(digest_string(expected_sha), "ledger digest is not canonical")
    digest, total, lines = hashlib.sha256(), 0, 0
    with verification_input(path, LEDGER_FILE_BYTES) as handle:
        while True:
            check_deadline()
            raw = handle.readline(LEDGER_LINE_BYTES + 1)
            if not raw:
                break
            total += len(raw)
            need(total <= LEDGER_FILE_BYTES and len(raw) <= LEDGER_LINE_BYTES,
                 "ledger exceeds file or line cap")
            need(raw.endswith(b"\n") and raw != b"\n", "ledger line is incomplete or empty")
            digest.update(raw)
            lines += 1
            consume(strict_json(raw), lines)
    need(digest.hexdigest() == expected_sha, "ledger digest mismatch")
    return lines


def load_ready_oracle(path, expected_sha, expected, warehouse, check_deadline):
    facts, sources, uuids = {}, {}, set()

    def consume(value, _line):
        need(type(value) is dict and set(value) == FACT_KEYS | {"source"},
             "oracle fields differ from prepared fact contract")
        source = value["source"]
        need(source in ("actual-POST", "independent-GET"), "oracle source is not a real create/load fact")
        need(all(type(value[key]) is str for key in FACT_KEYS), "oracle fact field is not a string")
        identity = (value["namespace"], value["kind"], value["name"])
        need(identity in expected, "oracle contains a foreign prepared identifier")
        seen = sources.setdefault(identity, set())
        need(source not in seen, "oracle contains a duplicate source fact")
        seen.add(source)
        fact = {key: value[key] for key in FACT_KEYS}
        try:
            need(str(uuid.UUID(fact["uuid"])) == fact["uuid"], "oracle UUID is not canonical")
        except ValueError:
            raise Refusal("oracle UUID is not canonical") from None
        safe_location(fact["location"], warehouse)
        safe_location(fact["metadata_location"], warehouse)
        need(fact["schema_sha256"] == sha(encoded([SCHEMA]))
             and digest_string(fact["metadata_sha256"]), "oracle schema or metadata digest is invalid")
        if identity in facts:
            need(facts[identity] == fact, "oracle POST and independent GET facts differ")
        else:
            need(fact["uuid"] not in uuids, "oracle contains a duplicate object UUID")
            uuids.add(fact["uuid"])
            facts[identity] = fact

    lines = read_ledger(path, expected_sha, consume, check_deadline)
    need(set(facts) == expected and lines == 2 * len(expected)
         and all(value == {"actual-POST", "independent-GET"} for value in sources.values()),
         "oracle lacks complete paired create and independent load facts")
    return facts


def validate_ready_audit(path, expected_sha, expected, previous, check_deadline):
    namespaces = {identity[0] for identity in expected}
    create, load, namespace_creates, listings = set(), set(), set(), {}
    group_sizes = {}
    for identity in expected:
        group_sizes[identity[:2]] = group_sizes.get(identity[:2], 0) + 1
    response_bytes = mutations = pages = 0
    config_seen = False
    keys = {"sequence", "method", "identity", "request_body_bytes", "request_body_sha256",
            "mutation", "status", "response_bytes", "response_sha256", "outcome", "elapsed_ms"}

    def consume(value, line):
        nonlocal response_bytes, mutations, pages, config_seen
        need(type(value) is dict and set(value) == keys, "READY audit fields differ from success contract")
        need(type(value["sequence"]) is int and value["sequence"] == line
             and type(value["status"]) is int and value["status"] == 200
             and value["outcome"] == "http-success", "READY audit is not contiguous successful HTTP")
        for key, maximum in (("request_body_bytes", BOUNDS["max_request_bytes"]),
                             ("response_bytes", BOUNDS["max_response_bytes"])):
            need(type(value[key]) is int and 0 <= value[key] <= maximum,
                 "READY audit byte count is invalid")
        need(digest_string(value["request_body_sha256"]) and digest_string(value["response_sha256"])
             and type(value["elapsed_ms"]) in (int, float)
             and 0 <= value["elapsed_ms"] <= BOUNDS["bulk_deadline_seconds"] * 1000,
             "READY audit digest or elapsed time is invalid")
        method, identity = value["method"], value["identity"]
        need(method in ("GET", "POST") and type(value["mutation"]) is bool
             and value["mutation"] == (method == "POST") and type(identity) is dict,
             "READY audit contains unexpected operation or mutation")
        operation = identity.get("operation")
        if operation in ("create", "load") and set(identity) == {"namespace", "kind", "name", "operation"}:
            need(all(type(identity[key]) is str for key in ("namespace", "kind", "name")),
                 "READY audit identity is invalid")
            key = (identity["namespace"], identity["kind"], identity["name"])
            seen = create if operation == "create" else load
            need(key in expected and key not in seen and method == ("POST" if operation == "create" else "GET"),
                 "READY audit contains duplicate or foreign object operation")
            need(key[0] in namespace_creates and (operation != "load" or len(create) == len(expected)),
                 "READY audit lacks create-before-independent-load ordering")
            seen.add(key)
        elif operation == "create" and set(identity) == {"namespace", "operation"}:
            namespace = identity["namespace"]
            need(type(namespace) is str and namespace in namespaces and namespace not in namespace_creates
                 and method == "POST", "READY audit namespace creation differs")
            namespace_creates.add(namespace)
        elif operation == "list" and set(identity) == {"namespace", "kind", "operation"}:
            namespace, kind = identity["namespace"], identity["kind"]
            need(method == "GET" and ((namespace is None and kind is None)
                 or (type(namespace) is str and namespace in namespaces and kind in ("tables", "views"))),
                 "READY audit listing is outside prepared scope")
            key = (namespace, kind)
            listings[key] = listings.get(key, 0) + 1
            maximum = 1 + 2 * (len(namespaces) + 1) if namespace is None else 1 + group_sizes[key]
            need(listings[key] <= maximum, "READY audit listing exceeds finite page bound")
            pages += 1
        else:
            need(identity == {"operation": "config"} and method == "GET" and line == 1,
                 "READY audit contains unexpected request")
            config_seen = True
        if method == "GET":
            need(value["request_body_bytes"] == 0 and value["request_body_sha256"] == sha(b""),
                 "READY audit GET unexpectedly carries a body")
        response_bytes += value["response_bytes"]
        mutations += int(value["mutation"])

    lines = read_ledger(path, expected_sha, consume, check_deadline)
    need(config_seen and create == expected and load == expected and namespace_creates == namespaces
         and set(listings) == {(None, None)} | {item[:2] for item in expected}
         and listings[(None, None)] >= 3, "READY audit lacks complete actual operations")
    for key, actual in (("http_requests", lines), ("mutations_attempted", mutations),
                        ("http_response_bytes", response_bytes), ("listing_pages", pages)):
        need(type(previous.get(key)) is int and previous[key] == actual, "READY audit receipt counter mismatch")


def load_ready(path, freeze, freeze_sha, check_deadline):
    with verification_input(path, 65536) as handle:
        raw = handle.read(65537)
    need(len(raw) <= 65536, "READY receipt exceeds local input cap")
    previous = strict_json(raw)
    receipt_keys = {"schema_version", "task", "state", "phase", "freeze_sha256", "producer_sha256",
                    "original_input", "source", "binding", "bounds", "details", "http_requests",
                    "mutations_attempted", "http_response_bytes", "listing_pages", "elapsed_seconds",
                    "http_audit_sha256", "metadata_oracle_sha256", "acceptance_scope"}
    need(type(previous) is dict and set(previous) == receipt_keys and previous.get("schema_version") == 1
         and type(previous["schema_version"]) is int and previous.get("task") == "MEM-1-M07"
         and previous.get("state") == "READY" and previous.get("phase") == "bulk",
         "verification requires a real bulk READY receipt")
    need(previous["acceptance_scope"] == "external preparation only; no native/provider CL acceptance"
         and type(previous["elapsed_seconds"]) in (int, float)
         and 0 <= previous["elapsed_seconds"] <= BOUNDS["bulk_deadline_seconds"],
         "READY preparation scope or elapsed time differs")
    for key, expected in (("freeze_sha256", freeze_sha), ("binding", freeze["binding"]),
                          ("producer_sha256", freeze["producer_sha256"]), ("source", SOURCE),
                          ("original_input", freeze["original_input"]), ("bounds", BOUNDS)):
        need(previous.get(key) == expected, "READY differs from exact bound freeze, source or owner")
    details = previous.get("details")
    detail_keys = {"capabilities", "normal", "actual_namespaces", "actual_tables_loaded", "actual_views_loaded",
                   "all_metadata_independently_loaded", "identifiers_sha256", "actual_metadata_facts_sha256",
                   "preflight_receipt_sha256", "mutations_stopped_before_independent_verification"}
    need(type(details) is dict and set(details) == detail_keys and details.get("normal") == NORMAL
         and details.get("all_metadata_independently_loaded") is True
         and details.get("mutations_stopped_before_independent_verification") is True
         and type(details.get("capabilities")) is dict
         and digest_string(details.get("preflight_receipt_sha256")),
         "READY lacks original complete independent preparation")
    for key, count in (("actual_namespaces", 32), ("actual_tables_loaded", 16384),
                       ("actual_views_loaded", 16384)):
        need(type(details.get(key)) is int and details[key] == count, "READY original population differs")
    expected = prepared_identifiers()
    parent = Path(path).parent
    warehouse = freeze["binding"]["server_default_warehouse"].rstrip("/")
    facts = load_ready_oracle(parent / "metadata-oracle.jsonl", previous.get("metadata_oracle_sha256"),
                              expected, warehouse, check_deadline)
    need(details.get("identifiers_sha256") == sha(encoded(sorted(facts)))
         and details.get("actual_metadata_facts_sha256") == sha(encoded([facts[key] for key in sorted(facts)])),
         "READY independent fact digest mismatch")
    validate_ready_audit(parent / "http-audit.jsonl", previous.get("http_audit_sha256"),
                         expected, previous, check_deadline)
    return previous, facts, sha(raw)


class Producer:
    def __init__(self, freeze, freeze_sha, phase, output):
        self.freeze = freeze
        self.freeze_sha = freeze_sha
        self.phase = phase
        self.binding = freeze["binding"]
        self.warehouse = self.binding["server_default_warehouse"].rstrip("/")
        self.output = Path(output)
        # Exclusive new directory: no accidental reuse, truncated audit or old READY.
        self.output.mkdir(mode=0o700, parents=False, exist_ok=False)
        self.started = time.monotonic()
        budget_phase = "bulk" if phase == "verify-after-drop" else phase
        self.deadline = self.started + BOUNDS[budget_phase + "_deadline_seconds"]
        self.count = 0
        self.bytes = 0
        self.page_count = 0
        self.mutations = 0
        self.audit = (self.output / "http-audit.jsonl").open("xb")
        self.oracle = (self.output / "metadata-oracle.jsonl").open("xb")

    def check_deadline(self):
        need(time.monotonic() < self.deadline, "preparation absolute deadline expired")

    def record(self, handle, value):
        handle.write(encoded(value) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())

    def request(self, method, path, identity, payload=None, query=None, empty=False):
        self.check_deadline()
        need(self.phase != "verify-after-drop" or (method == "GET" and payload is None and not empty),
             "verification permits only read-only GET requests")
        data = b"" if payload is None else encoded(payload)
        need(len(data) <= BOUNDS["max_request_bytes"], "request body exceeds frozen cap")
        uri = urlsplit(self.binding["rest_uri"])
        target = path + ("?" + urlencode(query) if query else "")
        need(len(target.encode()) + len(data) <= BOUNDS["max_request_bytes"],
             "request target and body exceed frozen cap")
        self.count += 1
        mutation = method in ("POST", "DELETE")
        self.mutations += int(mutation)
        started = time.monotonic()
        deadline = min(self.deadline, started + BOUNDS["request_timeout_seconds"])
        observation = {"sequence": self.count, "method": method, "identity": identity,
                       "request_body_bytes": len(data), "request_body_sha256": sha(data),
                       "mutation": mutation, "status": None, "response_bytes": 0,
                       "response_sha256": None, "outcome": "not-sent"}
        connection = http.client.HTTPConnection(uri.hostname, uri.port, timeout=deadline - started)
        timer = None
        response = None
        expired = threading.Event()
        raw = b""
        try:
            # http.client connects directly and never reads proxy/no_proxy variables.
            # Numeric loopback prevents DNS from escaping this absolute budget.
            connection.connect()
            need(time.monotonic() < deadline, "request absolute deadline expired during connect")
            sock = connection.sock

            def expire():
                expired.set()
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                sock.close()

            # Captures the actual socket even if getresponse detaches Connection: close.
            # This watchdog interrupts header/read trickles; socket timeout alone resets
            # on each read and would not establish an absolute request deadline.
            timer = threading.Timer(max(0, deadline - time.monotonic()), expire)
            timer.daemon = True
            timer.start()
            observation["outcome"] = "unknown-write" if mutation else "transport-failure"
            connection.request(method, target, body=data if payload is not None else None,
                               headers={"Accept": "application/json", "Content-Type": "application/json",
                                        "Connection": "close", "Accept-Encoding": "identity"})
            response = connection.getresponse()
            observation["status"] = response.status
            lengths = response.headers.get_all("Content-Length", [])
            encodings = response.headers.get_all("Content-Encoding", [])
            need(not encodings or encodings == ["identity"], "compressed response unsupported")
            need(len(lengths) <= 1, "duplicate Content-Length")
            if lengths:
                need(re.fullmatch(r"[0-9]+", lengths[0]) is not None
                     and int(lengths[0]) <= BOUNDS["max_response_bytes"],
                     "declared response exceeds frozen cap")
            raw = response.read(BOUNDS["max_response_bytes"] + 1)
            need(not expired.is_set() and time.monotonic() < deadline,
                 "request absolute deadline expired")
            observation["response_bytes"] = len(raw)
            observation["response_sha256"] = sha(raw)
            self.bytes += len(raw)
            need(len(raw) <= BOUNDS["max_response_bytes"], "response body exceeds frozen cap")
            if lengths:
                need(len(raw) == int(lengths[0]), "truncated response body")
            if response.status != 200:
                observation["outcome"] = "http-refusal"
                if raw:
                    try:
                        error = strict_json(raw)
                        error_type = error.get("error", {}).get("type")
                        if isinstance(error_type, str) and re.fullmatch(r"[A-Za-z0-9_.]{1,128}", error_type):
                            observation["iceberg_error_type"] = error_type
                    except (Refusal, AttributeError):
                        pass
                raise Refusal("actual REST HTTP refusal; inspect safe audit status/type")
            if empty:
                need(raw == b"", "canonical DELETE response must be 200 with empty body")
                value = None
            else:
                content_types = response.headers.get_all("Content-Type", [])
                need(len(content_types) == 1 and content_types[0].split(";", 1)[0].strip() == "application/json",
                     "response content type is not unique JSON")
                value = strict_json(raw)
            self.check_deadline()
            observation["outcome"] = "http-success"
            return value
        except (OSError, http.client.HTTPException, TimeoutError):
            raise Refusal("unknown mutation outcome or read transport failure; no automatic retry") from None
        finally:
            if timer is not None:
                timer.cancel()
                timer.join()
            if response is not None:
                response.close()
            connection.close()
            observation["elapsed_ms"] = round((time.monotonic() - started) * 1000, 3)
            self.record(self.audit, observation)

    def list_exact(self, expected, namespace=None, kind=None, page_size=256):
        collector = PageCollector(expected, page_size, namespace)
        path = "/v1/namespaces" if namespace is None else self.object_path(namespace, kind)
        token = None
        while not collector.finished:
            query = {"pageSize": page_size}
            if token is not None:
                query["pageToken"] = token
            value = self.request("GET", path, {"namespace": namespace, "kind": kind,
                                 "operation": "list"}, query=query)
            token = collector.add(value)
            self.page_count += 1
            self.check_deadline()
        return collector.pages

    @staticmethod
    def object_path(namespace, kind=None, name=None):
        path = "/v1/namespaces/" + quote(namespace, safe="")
        if kind is not None:
            path += "/" + kind
        if name is not None:
            path += "/" + quote(name, safe="")
        return path

    def namespace(self, name):
        value = self.request("POST", "/v1/namespaces", {"namespace": name, "operation": "create"},
                             {"namespace": [name], "properties": {}})
        need(type(value) is dict and value.get("namespace") == [name]
             and type(value.get("properties")) is dict, "created namespace response mismatch")

    def create(self, namespace, kind, name, table):
        body = {"name": name, "schema": SCHEMA}
        if kind == "tables":
            body["properties"] = {"format-version": "2"}
        else:
            body["view-version"] = {
                "version-id": 1, "timestamp-ms": self.freeze["view_timestamp_ms"],
                "schema-id": 0, "summary": {}, "default-namespace": [namespace],
                "representations": [{"type": "sql", "sql": "SELECT v FROM " + table, "dialect": "spark"}],
            }
            body["properties"] = {}
        value = self.request("POST", self.object_path(namespace, kind),
                             {"namespace": namespace, "kind": kind, "name": name, "operation": "create"}, body)
        return metadata_fact(value, kind, namespace, name, table, self.warehouse)

    def load(self, namespace, kind, name, table, expected):
        value = self.request("GET", self.object_path(namespace, kind, name),
                             {"namespace": namespace, "kind": kind, "name": name, "operation": "load"})
        fact = metadata_fact(value, kind, namespace, name, table, self.warehouse)
        need(fact == expected, "independently loaded metadata differs from creation")
        self.record(self.oracle, {"source": "independent-GET", **fact})
        return fact

    def initial(self, page_size):
        value = self.request("GET", "/v1/config", {"operation": "config"},
                             query={"warehouse": self.binding["warehouse"]})
        capabilities = parse_config(value, self.warehouse)
        self.list_exact([], page_size=page_size)
        return capabilities

    def preflight(self):
        capabilities = self.initial(1)
        namespace, table, view = "cl_preflight", "probe_table", "probe_view"
        self.namespace(namespace)
        created_table = self.create(namespace, "tables", table, table)
        created_view = self.create(namespace, "views", view, table)
        need(created_table["uuid"] != created_view["uuid"], "duplicate table/view UUID")
        self.list_exact([namespace], page_size=1)
        self.list_exact([table], namespace, "tables", 1)
        self.list_exact([view], namespace, "views", 1)
        self.load(namespace, "tables", table, table, created_table)
        self.load(namespace, "views", view, table, created_view)
        for kind, name in (("views", view), ("tables", table), (None, None)):
            self.request("DELETE", self.object_path(namespace, kind, name),
                         {"namespace": namespace, "kind": kind, "name": name, "operation": "delete"},
                         query={"purgeRequested": "true"} if kind == "tables" else None, empty=True)
        self.list_exact([], page_size=1)
        return {"capabilities": capabilities, "fresh_namespaces_before_and_after": True,
                "actual_tables_loaded": 1, "actual_views_loaded": 1,
                "deleted_namespace": namespace, "page_size": 1}

    def bulk(self, preflight_path):
        previous = strict_json(bounded_file(preflight_path, 65536))
        need(type(previous) is dict and previous.get("state") == "PREFLIGHT_PASS"
             and previous.get("freeze_sha256") == self.freeze_sha
             and previous.get("binding") == self.binding
             and previous.get("producer_sha256") == self.freeze["producer_sha256"],
             "bulk requires exact same bound preflight receipt")
        preflight_parent = Path(preflight_path).parent
        for filename, key in (("http-audit.jsonl", "http_audit_sha256"),
                              ("metadata-oracle.jsonl", "metadata_oracle_sha256")):
            need(sha(bounded_file(preflight_parent / filename, 16777216)) == previous.get(key),
                 "preflight ledger digest mismatch")
        capabilities = self.initial(256)
        facts = {}
        uuids = set()
        namespaces = [NORMAL["namespace_pattern"] % i for i in range(32)]
        tables = [NORMAL["table_pattern"] % i for i in range(512)]
        views = [NORMAL["view_pattern"] % i for i in range(512)]
        for namespace in namespaces:
            self.namespace(namespace)
            for kind, names in (("tables", tables), ("views", views)):
                for index, name in enumerate(names):
                    self.check_deadline()
                    fact = self.create(namespace, kind, name, tables[index])
                    need(fact["uuid"] not in uuids, "duplicate actual metadata UUID")
                    uuids.add(fact["uuid"])
                    facts[(namespace, kind, name)] = fact
                    self.record(self.oracle, {"source": "actual-POST", **fact})
        # The verification pass issues fresh GETs and consumes every returned token.
        # Producer counters alone never establish independent catalog completeness.
        self.list_exact(namespaces)
        for namespace in namespaces:
            for kind, names in (("tables", tables), ("views", views)):
                self.list_exact(names, namespace, kind)
                for index, name in enumerate(names):
                    self.load(namespace, kind, name, tables[index], facts[(namespace, kind, name)])
        self.list_exact(namespaces)
        return {"capabilities": capabilities, "normal": NORMAL,
                "actual_namespaces": len(namespaces), "actual_tables_loaded": 16384,
                "actual_views_loaded": 16384, "all_metadata_independently_loaded": True,
                "identifiers_sha256": sha(encoded(sorted(facts))),
                "actual_metadata_facts_sha256": sha(encoded([facts[key] for key in sorted(facts)])),
                "preflight_receipt_sha256": sha(Path(preflight_path).read_bytes()),
                "mutations_stopped_before_independent_verification": True}

    def verify_after_drop(self, ready_path):
        previous, facts, ready_sha = load_ready(ready_path, self.freeze, self.freeze_sha,
                                               self.check_deadline)
        target = NORMAL["namespace_pattern"] % 0
        namespaces = [NORMAL["namespace_pattern"] % index for index in range(1, NORMAL["namespaces"])]
        tables = [NORMAL["table_pattern"] % index for index in range(NORMAL["tables_per_namespace"])]
        views = [NORMAL["view_pattern"] % index for index in range(NORMAL["views_per_namespace"])]
        value = self.request("GET", "/v1/config", {"operation": "config"},
                             query={"warehouse": self.binding["warehouse"]})
        capabilities = parse_config(value, self.warehouse)
        need(capabilities == previous["details"].get("capabilities"),
             "REST configuration differs from prepared READY")
        # The normal catalog's namespace authority is its exact namespace listing.
        # No nonexistent-object GET or guessed 404 error type is accepted as proof.
        # The separate native receipt must establish which SQL caused this absence.
        self.list_exact(namespaces)
        retained = {}
        for namespace in namespaces:
            for kind, names in (("tables", tables), ("views", views)):
                self.list_exact(names, namespace, kind)
                for index, name in enumerate(names):
                    self.check_deadline()
                    identity = (namespace, kind, name)
                    retained[identity] = self.load(namespace, kind, name, tables[index], facts[identity])
        self.list_exact(namespaces)
        need(self.mutations == 0, "verification attempted a mutation")
        return {"capabilities": capabilities, "normal": NORMAL,
                "verification_deadline_seconds": BOUNDS["bulk_deadline_seconds"],
                "ready_receipt_sha256": ready_sha,
                "prepared_http_audit_sha256": previous["http_audit_sha256"],
                "prepared_metadata_oracle_sha256": previous["metadata_oracle_sha256"],
                "target_namespace": target, "target_namespace_list_absent": True,
                "target_identifier_absence_basis": "normal catalog namespace authority; exact listing absence",
                "target_prepared_identifiers_sha256": sha(encoded(sorted(key for key in facts if key[0] == target))),
                "native_drop_execution_proved": False, "object_http_refusal_types_proved": False,
                "actual_namespaces": len(namespaces),
                "actual_tables_loaded": len(namespaces) * len(tables),
                "actual_views_loaded": len(namespaces) * len(views),
                "all_surviving_metadata_independently_loaded_and_unchanged": True,
                "identifiers_sha256": sha(encoded(sorted(retained))),
                "actual_metadata_facts_sha256": sha(encoded([retained[key] for key in sorted(retained)]))}

    def finish(self, details):
        self.check_deadline()
        self.audit.close()
        self.oracle.close()
        receipt = {"schema_version": 1, "task": "MEM-1-M07",
                   "state": {"preflight": "PREFLIGHT_PASS", "bulk": "READY",
                             "verify-after-drop": "VERIFICATION_PASS"}[self.phase],
                   "phase": self.phase, "freeze_sha256": self.freeze_sha,
                   "producer_sha256": self.freeze["producer_sha256"],
                   "original_input": self.freeze["original_input"], "source": SOURCE,
                   "binding": self.binding, "bounds": BOUNDS, "details": details,
                   "http_requests": self.count, "mutations_attempted": self.mutations,
                   "http_response_bytes": self.bytes, "listing_pages": self.page_count,
                   "elapsed_seconds": round(time.monotonic() - self.started, 6),
                   "http_audit_sha256": sha((self.output / "http-audit.jsonl").read_bytes()),
                   "metadata_oracle_sha256": sha((self.output / "metadata-oracle.jsonl").read_bytes()),
                   "acceptance_scope": "external preparation only; no native/provider CL acceptance"}
        if self.phase == "verify-after-drop":
            receipt["acceptance_scope"] = (
                "external REST verification only; namespace-list absence and unchanged surviving metadata; "
                "no native DROP execution, object HTTP refusal type or provider CL acceptance")
        self.check_deadline()
        atomic_json(self.output / (receipt["state"] + ".json"), receipt, self.deadline)
        return receipt

    def fail(self, reason):
        for handle in (self.audit, self.oracle):
            if not handle.closed:
                handle.close()
        atomic_json(self.output / "FAILED.json", {
            "schema_version": 1, "state": "FAILED", "phase": self.phase,
            "freeze_sha256": self.freeze_sha, "reason": reason,
            "http_requests": self.count, "mutations_attempted": self.mutations,
            "recovery": "no automatic retry; inspect exact safe mutation audit and actual owner"})


class ParserTests(unittest.TestCase):
    def rejected(self, callback):
        with self.assertRaises(Refusal):
            callback()

    def test_source_matches_current_stock_fixture_input_owner(self):
        # Resolve the current source lock through its original schema/platform
        # author. This is local identity validation, not Docker or HTTP proof.
        import importlib.util

        repository = Path(__file__).resolve().parents[3]
        module_path = repository / "docker/fixture-inputs/fixture_inputs.py"
        spec = importlib.util.spec_from_file_location("real_rest_fixture_inputs", module_path)
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        inputs = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(inputs)
        lock, identity = inputs.load_lock(
            repository / "docker/fixture-inputs/lock.json",
            platform=SOURCE["platform"], consumer="iceberg-rest")
        self.assertEqual(identity, SOURCE["canonical_lock_sha256"])
        rest = lock["images"]["iceberg-rest"]
        self.assertEqual(rest["source"], SOURCE["image_source"])
        self.assertEqual(rest["platform"], SOURCE["platform"])
        self.assertEqual(rest["manifest_digest"], SOURCE["image_digest"])
        self.assertEqual(rest["alias"], SOURCE["image_alias"])

    def test_private_owner_guard_keeps_stock_bom_and_image_exact(self):
        repository = Path(__file__).resolve().parents[3]
        with tempfile.TemporaryDirectory() as directory:
            entry = Path(directory) / "private-fixture"
            entry.mkdir()
            manifest_path = entry / "manifest.json"
            freeze_path = entry / "freeze.json"
            binding = {"owner_mode": "task-private", "fixture_id": entry.name,
                       "compose_project": "nr-isolated-rest-test",
                       "owner_manifest_path": str(manifest_path),
                       "owner_manifest_sha256": "0" * 64,
                       "rest_uri": "http://127.0.0.1:21000",
                       "warehouse": "s3://warehouse/private-fixture/rest",
                       "server_default_warehouse": "s3://warehouse/private-fixture/rest/",
                       "rest_image_id": SOURCE["image_digest"]}
            owner = {"ready": True, "shared_docker": False,
                     "compose_project": binding["compose_project"], "env_id": entry.name,
                     "current_dir": str(entry),
                     "iceberg_rest": {key: binding[key] for key in
                                      ("rest_uri", "warehouse", "server_default_warehouse")},
                     "runtime": {"profile": "stock", "publication_dir": str(entry),
                                 "entry_root": str(entry),
                                 "producer_receipt": {"lock_sha256": SOURCE["canonical_lock_sha256"]},
                                 "catalog": {"project": binding["compose_project"],
                                             "images": {"rest": {"image_id": SOURCE["image_digest"],
                                                                 "tag": SOURCE["image_alias"]}}}}}
            owner["iceberg_rest"]["uri"] = owner["iceberg_rest"].pop("rest_uri")
            freeze = {"schema_version": 1, "task": "MEM-1-M07", "frozen_before_execution": True,
                      "original_input": {"path": str(repository / "docs/testing/mem-1-m07/inputs/cl-listing-freeze-v3.json"),
                                         "sha256": ORIGINAL_SHA256},
                      "normal": NORMAL, "bounds": BOUNDS, "source": SOURCE,
                      "producer_sha256": sha(Path(__file__).read_bytes()), "binding": binding,
                      "view_timestamp_ms": 1791504000000,
                      "scope": "external-producer-only; native/SQL deadlines remain independent"}

            def write_owner(value):
                raw = encoded(value)
                manifest_path.write_bytes(raw)
                binding["owner_manifest_sha256"] = sha(raw)
                freeze_path.write_bytes(encoded(freeze))

            write_owner(owner)
            self.assertEqual(validate_freeze(freeze_path)[0]["source"], SOURCE)
            for field, value, reason in (
                ("profile", "publication-hook", "owner stock profile or BOM mismatch"),
                ("lock", "76512c5eb0aaef5b918c0fb551e32e1d9b3d51cccbf74b9e8ad8364cabc7c25b",
                 "owner stock profile or BOM mismatch"),
                ("image", "sha256:" + "0" * 64, "owner REST image digest or alias mismatch"),
            ):
                with self.subTest(field=field):
                    changed = strict_json(encoded(owner))
                    if field == "profile":
                        changed["runtime"]["profile"] = value
                    elif field == "lock":
                        changed["runtime"]["producer_receipt"]["lock_sha256"] = value
                    else:
                        changed["runtime"]["catalog"]["images"]["rest"]["image_id"] = value
                    write_owner(changed)
                    with self.assertRaisesRegex(Refusal, reason):
                        validate_freeze(freeze_path)

    def test_json_duplicate_nonfinite(self):
        for value in (b'{"namespaces":[],"namespaces":[["extra"]]}', b'{"x":NaN}', b'{'):
            self.rejected(lambda v=value: strict_json(v))

    def test_namespace_terminal_fields(self):
        for value in ({"namespaces": []}, {"namespaces": [], "next-page-token": None}):
            collector = PageCollector([], 1)
            self.assertIsNone(collector.add(value))
            self.assertTrue(collector.finished)
        for value in ({}, {"namespaces": None}, {"namespaces": [], "next-page-token": ""}):
            self.rejected(lambda v=value: PageCollector([], 1).add(v))

    def test_token_cycle_and_no_progress(self):
        collector = PageCollector(["a", "b", "c"], 1)
        collector.add({"namespaces": [["a"]], "next-page-token": "opaque-a"})
        collector.add({"namespaces": [["b"]], "next-page-token": "opaque-b"})
        self.rejected(lambda: collector.add({"namespaces": [["c"]], "next-page-token": "opaque-a"}))
        self.rejected(lambda: PageCollector(["a"], 1).add({"namespaces": [], "next-page-token": "x"}))

    def test_duplicate_extra_missing_and_cross_scope(self):
        collector = PageCollector(["a", "b"], 1)
        collector.add({"namespaces": [["a"]], "next-page-token": "x"})
        self.rejected(lambda: collector.add({"namespaces": [["a"]]}))
        self.rejected(lambda: PageCollector(["a"], 1).add({"namespaces": [["extra"]]}))
        self.rejected(lambda: PageCollector(["a"], 1).add({"namespaces": []}))
        self.rejected(lambda: PageCollector(["a"], 1, "ns").add(
            {"identifiers": [{"namespace": ["foreign"], "name": "a"}]}))
        self.rejected(lambda: PageCollector(["a"], 1, "ns").add({"namespaces": [["a"]]}))

    def test_page_and_token_caps(self):
        self.rejected(lambda: PageCollector(["a", "b"], 1).add({"namespaces": [["a"], ["b"]]}))
        self.rejected(lambda: PageCollector(["a", "b"], 1).add(
            {"namespaces": [["a"]], "next-page-token": "x" * 4097}))

    def test_valid_independent_identifier_pages(self):
        collector = PageCollector(["a", "b"], 1, "ns")
        self.assertEqual(collector.add({"identifiers": [{"namespace": ["ns"], "name": "a"}],
                                       "next-page-token": "server-opaque"}), "server-opaque")
        collector.add({"identifiers": [{"namespace": ["ns"], "name": "b"}]})
        self.assertEqual(collector.seen, {"a", "b"})
        self.rejected(lambda: collector.add({"identifiers": []}))

    def test_config_capability_errors(self):
        self.assertFalse(parse_config({"defaults": {}, "overrides": {}}, "s3://b/private")["endpoints_present"])
        for value in ({"defaults": {}}, {"defaults": {}, "overrides": {"prefix": "hidden"}},
                      {"defaults": {}, "overrides": {}, "endpoints": []},
                      {"defaults": {}, "overrides": {}, "endpoints": None}):
            self.rejected(lambda v=value: parse_config(v, "s3://b/private"))

    def test_table_metadata_empty_snapshots_and_refusals(self):
        metadata = {"table-uuid": "019fa000-0000-7000-8000-000000000001", "format-version": 2,
                    "location": "s3://b/private/ns/t", "schemas": [SCHEMA], "current-schema-id": 0}
        response = {"metadata": metadata, "metadata-location": "s3://b/private/ns/t/metadata/00000.json"}
        fact = metadata_fact(response, "tables", "ns", "t", "t", "s3://b/private")
        self.assertEqual(fact["uuid"], metadata["table-uuid"])
        for bad in ({**metadata, "snapshots": None}, {**metadata, "snapshots": [{"snapshot-id": 1}]},
                    {**metadata, "current-schema-id": True}, {**metadata, "table-uuid": "bad"},
                    {**metadata, "location": "s3://b/shared/t"}):
            self.rejected(lambda m=bad: metadata_fact({**response, "metadata": m},
                                                     "tables", "ns", "t", "t", "s3://b/private"))

    def test_view_metadata_real_definition(self):
        version = {"version-id": 1, "schema-id": 0, "timestamp-ms": 1, "summary": {},
                   "default-namespace": ["ns"], "representations": [
                       {"type": "sql", "sql": "SELECT v FROM t", "dialect": "spark"}]}
        metadata = {"view-uuid": "019fa000-0000-7000-8000-000000000002", "format-version": 1,
                    "location": "s3://b/private/ns/v", "schemas": [SCHEMA], "current-version-id": 1,
                    "versions": [version], "version-log": [{"version-id": 1, "timestamp-ms": 1}]}
        response = {"metadata": metadata, "metadata-location": "s3://b/private/ns/v/metadata/00000.json"}
        metadata_fact(response, "views", "ns", "v", "t", "s3://b/private")
        for bad in ({**version, "schema-id": 2}, {**version, "default-namespace": ["other"]},
                    {**version, "representations": []}):
            self.rejected(lambda v=bad: metadata_fact({**response, "metadata": {**metadata, "versions": [v]}},
                                                     "views", "ns", "v", "t", "s3://b/private"))

    def test_locations_do_not_admit_client_or_foreign_authority(self):
        self.assertEqual(safe_location("s3://warehouse/private/ns/t", "s3://warehouse/private"),
                         "s3://warehouse/private/ns/t")
        for location in ("s3://warehouse/client/ns/t", "s3://warehouse/private-extra/t",
                         "s3://warehouse/private/../shared/t", "s3://warehouse/private/t?secret=x",
                         "s3://user:secret@warehouse/private/t"):
            self.rejected(lambda value=location: safe_location(value, "s3://warehouse/private"))

    @staticmethod
    def oracle_fixture():
        namespace, name = "cl_ns_0001", "cl_table_000000"
        metadata = {"table-uuid": "019fa000-0000-7000-8000-000000000001", "format-version": 2,
                    "location": "s3://b/private/ns/t", "schemas": [SCHEMA], "current-schema-id": 0}
        response = {"metadata": metadata, "metadata-location": "s3://b/private/ns/t/metadata/00000.json"}
        fact = metadata_fact(response, "tables", namespace, name, name, "s3://b/private")
        return (namespace, "tables", name), fact, response

    def test_ready_oracle_complete_pair_and_bad_digest(self):
        identity, fact, _ = self.oracle_fixture()
        raw = b"".join(encoded({"source": source, **fact}) + b"\n"
                       for source in ("actual-POST", "independent-GET"))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oracle.jsonl"
            path.write_bytes(raw)
            self.assertEqual(load_ready_oracle(path, sha(raw), {identity}, "s3://b/private", lambda: None),
                             {identity: fact})
            self.rejected(lambda: load_ready_oracle(path, "0" * 64, {identity}, "s3://b/private", lambda: None))

    def test_ready_oracle_duplicate_missing_unknown_source_and_fields(self):
        identity, fact, _ = self.oracle_fixture()
        post, get = ({"source": source, **fact} for source in ("actual-POST", "independent-GET"))
        cases = ([post, get, get], [post], [post, {**get, "source": "synthetic"}],
                 [post, {key: value for key, value in get.items() if key != "metadata_sha256"}],
                 [post, {**get, "uuid": "019fa000-0000-7000-8000-000000000002"}],
                 [post, {**get, "name": "foreign"}])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oracle.jsonl"
            for records in cases:
                raw = b"".join(encoded(value) + b"\n" for value in records)
                path.write_bytes(raw)
                self.rejected(lambda: load_ready_oracle(path, sha(raw), {identity}, "s3://b/private", lambda: None))

    def test_ready_ledger_line_bound_and_incomplete_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            for raw in (encoded({"value": "x" * LEDGER_LINE_BYTES}) + b"\n", b'{}'):
                path.write_bytes(raw)
                self.rejected(lambda: read_ledger(path, sha(raw), lambda _value, _line: None, lambda: None))

    def test_ready_ledger_file_cap_and_nonregular_input(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ledger.jsonl"
            with path.open("wb") as handle:
                handle.truncate(LEDGER_FILE_BYTES + 1)
            with self.assertRaisesRegex(Refusal, "bounded regular file"):
                read_ledger(path, "0" * 64, lambda _value, _line: None, lambda: None)
            path.unlink()
            os.mkfifo(path)
            with self.assertRaisesRegex(Refusal, "bounded regular file"):
                read_ledger(path, "0" * 64, lambda _value, _line: None, lambda: None)

    def test_ready_audit_requires_complete_real_create_load_and_counters(self):
        identity, _, _ = self.oracle_fixture()
        namespace, kind, name = identity
        operations = [("GET", {"operation": "config"}),
                      ("GET", {"namespace": None, "kind": None, "operation": "list"}),
                      ("POST", {"namespace": namespace, "operation": "create"}),
                      ("POST", {"namespace": namespace, "kind": kind, "name": name, "operation": "create"}),
                      ("GET", {"namespace": None, "kind": None, "operation": "list"}),
                      ("GET", {"namespace": namespace, "kind": kind, "operation": "list"}),
                      ("GET", {"namespace": namespace, "kind": kind, "name": name, "operation": "load"}),
                      ("GET", {"namespace": None, "kind": None, "operation": "list"})]
        records = [{"sequence": index, "method": method, "identity": item,
                    "request_body_bytes": 0, "request_body_sha256": sha(b""), "mutation": method == "POST",
                    "status": 200, "response_bytes": 2, "response_sha256": sha(b"{}"),
                    "outcome": "http-success", "elapsed_ms": 1}
                   for index, (method, item) in enumerate(operations, 1)]
        previous = {"http_requests": 8, "mutations_attempted": 2, "http_response_bytes": 16, "listing_pages": 4}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.jsonl"

            def validate(values, receipt=previous):
                raw = b"".join(encoded(value) + b"\n" for value in values)
                path.write_bytes(raw)
                validate_ready_audit(path, sha(raw), {identity}, receipt, lambda: None)

            validate(records)
            self.rejected(lambda: validate(records, {**previous, "http_requests": 7}))
            self.rejected(lambda: validate(records[:-2] + records[-1:]))
            self.rejected(lambda: validate([{**value, "status": 404} if value["sequence"] == 7 else value
                                           for value in records]))
            self.rejected(lambda: validate([{**value, "method": "DELETE", "mutation": True}
                                           if value["sequence"] == 7 else value for value in records]))

    def test_after_drop_rejects_surviving_metadata_difference(self):
        identity, fact, response = self.oracle_fixture()
        producer = object.__new__(Producer)
        producer.warehouse = "s3://b/private"
        with tempfile.TemporaryFile("w+b") as oracle:
            producer.oracle = oracle
            producer.request = lambda *_args, **_kwargs: response
            producer.load(*identity, identity[2], fact)
            for changed in ({**response, "metadata-location": "s3://b/private/ns/t/metadata/00001.json"},
                            {**response, "metadata": {**response["metadata"], "location": "s3://b/private/changed/t"}},
                            {**response, "metadata": {**response["metadata"], "table-uuid":
                                                      "019fa000-0000-7000-8000-000000000002"}},
                            {**response, "metadata": {**response["metadata"], "properties": {"changed": "true"}}}):
                producer.request = lambda *_args, result=changed, **_kwargs: result
                self.rejected(lambda: producer.load(*identity, identity[2], fact))

    def test_verification_rejects_mutation_before_network(self):
        producer = object.__new__(Producer)
        producer.phase = "verify-after-drop"
        producer.deadline = time.monotonic() + 1
        for method in ("POST", "DELETE", "PUT"):
            self.rejected(lambda m=method: producer.request(m, "/never-sent", {}))
        self.rejected(lambda: producer.request("GET", "/never-sent", {}, payload={}))
        self.rejected(lambda: producer.request("GET", "/never-sent", {}, empty=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--freeze", type=Path)
    parser.add_argument("--phase", choices=("preflight", "bulk", "verify-after-drop"))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--preflight-receipt", type=Path)
    parser.add_argument("--ready-receipt", type=Path)
    args = parser.parse_args()
    if args.self_test:
        need(not any((args.freeze, args.phase, args.output, args.preflight_receipt, args.ready_receipt)),
             "self-test cannot accept service arguments")
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ParserTests)
        passed = unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
        print("Pure-local parser tests only; no HTTP or provider acceptance.")
        return 0 if passed else 1
    need(args.freeze and args.phase and args.output, "freeze, phase and output are required")
    need((args.phase == "bulk") == bool(args.preflight_receipt),
         "only bulk requires preflight receipt")
    need((args.phase == "verify-after-drop") == bool(args.ready_receipt),
         "only verify-after-drop requires READY receipt")
    freeze, freeze_sha = validate_freeze(args.freeze)
    producer = Producer(freeze, freeze_sha, args.phase, args.output)
    try:
        if args.phase == "preflight":
            details = producer.preflight()
        elif args.phase == "bulk":
            details = producer.bulk(args.preflight_receipt)
        else:
            details = producer.verify_after_drop(args.ready_receipt)
        receipt = producer.finish(details)
        print(json.dumps({"state": receipt["state"], "phase": args.phase,
                          "http_requests": receipt["http_requests"], "freeze_sha256": freeze_sha}))
        return 0
    except Refusal as error:
        producer.fail(str(error))
        raise
    except (OSError, KeyError, TypeError, ValueError, KeyboardInterrupt):
        producer.fail("local error or interruption; no automatic retry")
        raise Refusal("local error or interruption; inspect FAILED and safe audit") from None


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Refusal as failure:
        print("REFUSED: " + str(failure), file=sys.stderr)
        sys.exit(1)
    except (OSError, KeyError, TypeError, ValueError):
        # Never let Python exception text expose owner manifest/config secrets.
        print("REFUSED: local binding or input error", file=sys.stderr)
        sys.exit(1)
