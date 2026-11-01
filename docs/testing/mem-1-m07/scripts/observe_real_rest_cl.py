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

"""Bounded stdlib observer for actual normal REST CL; never a provider or injector.

Read-only integration evidence:
* tests/cluster-harness/src/vended_rest_catalog.rs:1332 proxies a real catalog,
  but :1373 injects credentials and its audit lacks CL counters. Do not use its
  response rewriting for this observer.
* tests/system-test-runner/src/scenarios/mv_uea7.rs:49-115 binds catalog URI and
  FE/BE credentials to one IsolatedIcebergRestFixture. Replace only that catalog
  URI with this observer's actual bound URI, preserving the same owner/warehouse
  and static S3 facts. Keep both owners until native cluster shutdown.

CLI: python3 SCRIPT --freeze OBSERVER_BOUND.json --output NEW_DIRECTORY
Control is one JSON line per stdin command, one safe stdout JSON response:
  {"command":"snapshot"}
  {"command":"reset","phase":"catalog_admission"}
  {"command":"quiesce"}
  {"command":"stop"}
EOF stops the observer. The initial admission state is QUIESCENT. Reset opens
one phase only after quiesce actually closes/joins every connection pair and
all four original live counters are zero under the admission lock. Idle pairs
remain counted; persistent HTTP message completion is not physical exit.

Observer freeze schema (exact keys; main must freeze before actual execution):
  schema_version: 1, frozen_before_execution: true,
  observer_sha256: exact SCRIPT digest,
  producer_path: absolute reviewed producer script path,
  producer_sha256: exact producer script digest,
  producer_freeze_path: absolute bound producer JSON,
  producer_freeze_sha256: exact bound JSON digest,
  bounds: BOUNDS below.
The existing producer.validate_freeze verifies private owner/actual manifest/
source binding before any socket bind. No Docker or service lifecycle commands.
OBSERVER_BOUND.json is an endpoint/process receipt, never provider/native READY.

Counters reflect actual requests, complete downstream pages and body sends;
they do not infer FE PID, task identity, SDK positions, or query completion.
metadata_loads/successful_mutations are complete downstream HTTP200 responses,
not a substitute for native typed success. emitted_body_bytes counts bytes
accepted by the observer's client socket, not FE consumption or decoded memory.
Main must save actual FE/catalog config containing this exact observer URI,
native FE PID/start/build, private downstream image/manifest binding, phase
isolation, and observer-process exclusion from FE allocator samples. No producer
or independent verification client may use the observer during native phases.

Semantic headers/status/reason/payload bytes are preserved. HTTP hop framing
(Host, Connection, transfer chunk framing) is adapted for direct HTTP connections.
One bounded pair may reuse both hops within a phase; only close-framed replies
require transport EOF. Explicit quiesce closes and joins all pairs before reset.
No JSON, names, tokens, credentials, errors or status are rewritten.
Observer bound/transport failures close the socket and taint the phase; they
never generate a substitute HTTP error, retry, partial-success or fake catalog.
Compressed listing bodies are observed with bounded gzip decode, then the exact
original compressed bytes/header are forwarded. Unsupported encodings taint
the phase while the actual bounded response is still forwarded unchanged.

--self-test uses only pure parsers and synthetic loopback byte transparency
tests. It is explicitly NOT real provider, native CL or product acceptance.
"""

import argparse
import errno
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
import importlib.util
import io
import ipaddress
import json
import os
from pathlib import Path
import re
import select
import socket
from socketserver import TCPServer, ThreadingMixIn
import struct
import sys
import tempfile
import threading
import time
import unittest
from unittest import mock
from urllib.parse import unquote, urlsplit
import zlib


BOUNDS = {
    "max_observers": 256, "max_request_bytes": 65536,
    "max_response_bytes": 16777216, "max_decoded_json_bytes": 16777216,
    "max_header_bytes": 65536, "max_header_fields": 100,
    "request_absolute_deadline_seconds": 30,
}
FREEZE_KEYS = {"schema_version", "frozen_before_execution", "observer_sha256",
               "producer_path", "producer_sha256", "producer_freeze_path",
               "producer_freeze_sha256", "bounds"}
HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
       "te", "trailer", "transfer-encoding", "upgrade"}
FORWARD_STAGES = frozenset({
    "request-validate", "request-body", "upstream-open", "upstream-connect",
    "upstream-send", "upstream-response", "upstream-transport-eof", "client-send", "request-completion",
    "idle-client-read", "unknown-operation",
})


class IdleReset:
    """A typed reset observed by the original idle transport probe."""
    def __init__(self, side, probe, error):
        self.side, self.probe, self.error = side, probe, error


class Refusal(Exception):
    """Only static safe reason strings may be passed to this exception."""


def forwarding_failure_details(stage, error, sequence):
    """Closed diagnostic facts; exception names/messages/arguments never escape."""
    if isinstance(error, Refusal):
        category = "refusal"
    elif isinstance(error, OSError):
        category = "os-error"
    elif isinstance(error, http.client.HTTPException):
        category = "http-error"
    elif isinstance(error, ValueError):
        category = "value-error"
    else:
        category = "other-error"
    native_errno = error.errno if isinstance(error, OSError) else None
    return {"operation_stage": stage if stage in FORWARD_STAGES else "unknown-operation",
            "exception_category": category,
            "errno": native_errno if type(native_errno) is int else None,
            "request_sequence": sequence if type(sequence) is int and sequence > 0 else None}


def need(condition, reason):
    if not condition:
        raise Refusal(reason)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def parse_json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            need(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def constant(_):
        raise Refusal("nonfinite JSON value")

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=constant)
    except (ValueError, UnicodeError, RecursionError):
        raise Refusal("malformed JSON") from None


def read_file(path, cap):
    with Path(path).open("rb") as handle:
        data = handle.read(cap + 1)
    need(len(data) <= cap, "local input exceeds cap")
    return data


def downstream_address(uri):
    value = urlsplit(uri)
    need(value.scheme == "http" and value.hostname and value.port and value.path in ("", "/")
         and value.username is None and value.password is None and not value.query and not value.fragment,
         "invalid exact private downstream URI")
    try:
        address = ipaddress.ip_address(value.hostname)
    except ValueError:
        raise Refusal("numeric downstream address required") from None
    need(address.is_loopback, "private downstream must be loopback")
    return value


def validate_freeze(path):
    raw = read_file(path, 65536)
    freeze = parse_json(raw)
    need(type(freeze) is dict and set(freeze) == FREEZE_KEYS, "invalid observer freeze fields")
    need(type(freeze["schema_version"]) is int and freeze["schema_version"] == 1
         and freeze["frozen_before_execution"] is True, "observer is not frozen")
    need(encode(freeze["bounds"]) == encode(BOUNDS), "observer bounds differ from reviewed draft")
    need(freeze["observer_sha256"] == sha(Path(__file__).read_bytes()), "observer source mismatch")
    for key in ("producer_path", "producer_freeze_path"):
        need(isinstance(freeze[key], str) and Path(freeze[key]).is_absolute(), "explicit absolute producer input required")
    producer_bytes = read_file(freeze["producer_path"], 1048576)
    producer_freeze_bytes = read_file(freeze["producer_freeze_path"], 65536)
    need(sha(producer_bytes) == freeze["producer_sha256"], "producer source digest mismatch")
    need(sha(producer_freeze_bytes) == freeze["producer_freeze_sha256"], "producer freeze digest mismatch")
    spec = importlib.util.spec_from_file_location("reviewed_rest_producer", freeze["producer_path"])
    need(spec is not None and spec.loader is not None, "producer module unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    try:
        producer_freeze, producer_sha = module.validate_freeze(freeze["producer_freeze_path"])
    except module.Refusal:
        raise Refusal("actual producer/private-owner binding refused") from None
    need(producer_sha == freeze["producer_freeze_sha256"], "bound producer digest changed")
    binding = producer_freeze["binding"]
    downstream_address(binding["rest_uri"])
    return freeze, sha(raw), binding, producer_freeze["source"]


def header_facts(headers, cap=65536):
    need(len(headers) <= 100, "header field count exceeds cap")
    total = 2
    for name, value in headers:
        need(re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name) is not None
             and not any(ch in value for ch in ("\r", "\n", "\x00")), "unsafe header syntax")
        total += len((name + ": " + value + "\r\n").encode("latin-1"))
    need(total <= cap, "header bytes exceed cap")
    lengths = [value for name, value in headers if name.lower() == "content-length"]
    transfers = [value for name, value in headers if name.lower() == "transfer-encoding"]
    need(len(lengths) <= 1 and len(transfers) <= 1 and not (lengths and transfers),
         "ambiguous HTTP body framing")
    if lengths:
        need(re.fullmatch(r"[0-9]+", lengths[0]) is not None, "invalid Content-Length")
    if transfers:
        need(transfers == ["chunked"], "unsupported transfer framing")
    connections = [value for name, value in headers if name.lower() == "connection"]
    hop = set(HOP)
    for connection in connections:
        for token in connection.split(","):
            token = token.strip().lower()
            need(re.fullmatch(r"[!#$%&'*+.^_`|~0-9a-z-]+", token) is not None,
                 "invalid Connection header")
            hop.add(token)
    # Do not silently drop an authorization/entity header declared hop-by-hop.
    need(not ({"authorization", "content-type", "content-encoding", "content-length"} & (hop - HOP)),
         "semantic header declared hop-by-hop")
    return hop, int(lengths[0]) if lengths else None, bool(transfers)


def route(method, target):
    parsed = urlsplit(target)
    need(not parsed.scheme and not parsed.netloc and not parsed.fragment
         and parsed.path.startswith("/v1/"), "request outside actual REST normal scope")
    parts = parsed.path.split("/")
    if method == "GET" and parts == ["", "v1", "namespaces"]:
        return "list_namespaces"
    if len(parts) == 5 and parts[:3] == ["", "v1", "namespaces"] and parts[4] in ("tables", "views"):
        if method == "GET":
            return "list_" + parts[4]
    if len(parts) == 6 and parts[:3] == ["", "v1", "namespaces"] and parts[4] in ("tables", "views"):
        if method == "GET":
            return "load_" + parts[4]
    return "other"


def listing_facts(category, target, body, headers, status, cap=16777216):
    if not category.startswith("list_") or status != 200:
        return {"pages": 0, "names": 0, "name_bytes": 0}
    types = [v for k, v in headers if k.lower() == "content-type"]
    need(len(types) == 1 and types[0].split(";", 1)[0].strip() == "application/json",
         "listing Content-Type is not unique JSON")
    encodings = [v for k, v in headers if k.lower() == "content-encoding"]
    need(len(encodings) <= 1, "duplicate Content-Encoding")
    if encodings and encodings != ["identity"]:
        need(encodings == ["gzip"], "listing encoding cannot be observed")
        try:
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            decoded = decoder.decompress(body, cap + 1)
            need(len(decoded) <= cap and not decoder.unconsumed_tail and decoder.eof
                 and not decoder.unused_data, "gzip JSON exceeds cap or is incomplete")
            body = decoded
        except zlib.error:
            raise Refusal("invalid gzip JSON") from None
    need(len(body) <= cap, "decoded listing JSON exceeds cap")
    value = parse_json(body)
    need(type(value) is dict, "listing response is not object")
    token = value.get("next-page-token")
    need(token is None or (isinstance(token, str) and 0 < len(token.encode()) <= 4096),
         "invalid observed continuation")
    names = []
    if category == "list_namespaces":
        need("namespaces" in value and type(value["namespaces"]) is list, "missing namespaces collection")
        for namespace in value["namespaces"]:
            need(type(namespace) is list and len(namespace) == 1 and isinstance(namespace[0], str),
                 "namespace outside frozen normal shape")
            names.append(namespace[0])
    else:
        need("identifiers" in value and type(value["identifiers"]) is list, "missing identifiers collection")
        namespace = unquote(urlsplit(target).path.split("/")[3], errors="strict")
        for item in value["identifiers"]:
            need(type(item) is dict and set(item) == {"namespace", "name"}
                 and item["namespace"] == [namespace] and isinstance(item["name"], str),
                 "identifier namespace or shape mismatch")
            names.append(item["name"])
    need(len(names) == len(set(names)), "duplicate actual names within page")
    query = urlsplit(target).query
    # Count actual returned short names; namespace-level bytes are counted in
    # namespace-list responses, matching the product listing collector vocabulary.
    return {"pages": 1, "names": len(names), "name_bytes": sum(len(n.encode()) for n in names),
            "returned_names_sha256": sha(encode(names)), "query_sha256": sha(query.encode()),
            "next_token_present": token is not None,
            "next_token_sha256": sha(token.encode()) if token is not None else None}


class Audit:
    def __init__(self, ledger=None):
        self.lock = threading.RLock()
        self.ledger = ledger
        self.connections = 0
        self.active = 0
        self.upstream_active = 0
        self.upstream_connections = 0
        self.http_requests = 0
        self.list_active = 0
        self.sequence = 0
        self.phase = "unclassified"
        self.generation = 1
        self.total_failures = 0
        self.reset_counters()

    def reset_counters(self):
        self.counters = {key: 0 for key in (
            "requests", "upstream_requests_sent", "upstream_mutations_sent", "completed", "observer_connections_peak", "active_peak",
            "upstream_active_peak", "listing_active_peak", "failures", "admission_refusals",
            "pages", "names", "name_bytes", "upstream_body_bytes", "emitted_body_bytes",
            "metadata_loads", "mutations", "successful_mutations")}
        self.routes = {}
        self.statuses = {}

    def event(self, value):
        if self.ledger:
            self.ledger.write(encode(value) + b"\n")
            self.ledger.flush()

    def accept(self):
        with self.lock:
            self.connections += 1
            self.counters["observer_connections_peak"] = max(self.counters["observer_connections_peak"], self.connections)

    def release(self):
        with self.lock:
            self.connections -= 1
            need(self.connections >= 0, "connection ownership underflow")

    def request_enter(self):
        with self.lock:
            self.http_requests += 1

    def request_exit(self):
        with self.lock:
            self.http_requests -= 1
            need(self.http_requests >= 0, "HTTP request ownership underflow")

    def upstream_open(self):
        with self.lock:
            self.upstream_connections += 1

    def upstream_close(self):
        with self.lock:
            self.upstream_connections -= 1
            need(self.upstream_connections >= 0, "upstream socket ownership underflow")

    def failure(self, reason, admission=False, *, operation=None, error=None, diagnostic=None, request_held=None):
        with self.lock:
            self.counters["failures"] += 1
            self.total_failures += 1
            self.counters["admission_refusals"] += int(admission)
            event = {"event": "failure", "phase": self.phase, "generation": self.generation, "reason": reason}
            if error is not None:
                # Retain only structural exception facts, never message/body,
                # request headers, target or credential-bearing diagnostics.
                event["error"] = {"type": type(error).__name__,
                                  "errno": error.errno if isinstance(error, OSError) else None,
                                  "operation": operation}
            if request_held is not None:
                event["request_held"] = request_held
            if diagnostic is not None:
                event["diagnostic"] = diagnostic
            self.event(event)

    def begin(self, method, target, category, peer):
        with self.lock:
            self.sequence += 1
            self.active += 1
            self.counters["requests"] += 1
            self.counters["active_peak"] = max(self.counters["active_peak"], self.active)
            self.counters["mutations"] += int(method in ("POST", "PUT", "PATCH", "DELETE"))
            self.routes[category] = self.routes.get(category, 0) + 1
            return {"sequence": self.sequence, "phase": self.phase, "generation": self.generation,
                    "method": method, "target_sha256": sha(target.encode()), "route": category,
                    "peer_ip": peer[0], "peer_port": peer[1], "status": None,
                    "request_body_bytes": 0, "request_body_sha256": None,
                    "response_body_bytes": 0, "response_body_sha256": None,
                    "emitted_body_bytes": 0, "outcome": "incomplete"}

    def sent(self, method):
        with self.lock:
            self.counters["upstream_requests_sent"] += 1
            self.counters["upstream_mutations_sent"] += int(method in ("POST", "PUT", "PATCH", "DELETE"))

    def upstream_begin(self, category):
        with self.lock:
            self.upstream_active += 1
            self.list_active += int(category.startswith("list_"))
            self.counters["upstream_active_peak"] = max(self.counters["upstream_active_peak"], self.upstream_active)
            self.counters["listing_active_peak"] = max(self.counters["listing_active_peak"], self.list_active)

    def upstream_end(self, category):
        with self.lock:
            self.upstream_active -= 1
            self.list_active -= int(category.startswith("list_"))

    def emitted(self, count):
        with self.lock:
            self.counters["emitted_body_bytes"] += count

    def received(self, record, facts):
        with self.lock:
            self.counters["upstream_body_bytes"] += record["response_body_bytes"]
            self.statuses[str(record["status"])] = self.statuses.get(str(record["status"]), 0) + 1
            for key in ("pages", "names", "name_bytes"):
                self.counters[key] += facts[key]
            if record["status"] == 200:
                self.counters["metadata_loads"] += int(record["route"].startswith("load_"))
                self.counters["successful_mutations"] += int(record["method"] in ("POST", "PUT", "PATCH", "DELETE"))

    def complete(self):
        with self.lock:
            self.counters["completed"] += 1

    def end(self, record):
        with self.lock:
            self.active -= 1
            need(self.active >= 0, "request ownership underflow")
            self.event({"event": "request", **record})

    def snapshot(self):
        with self.lock:
            if self.ledger and not self.ledger.closed:
                self.ledger.flush()
                os.fsync(self.ledger.fileno())
            return {"phase": self.phase, "generation": self.generation,
                    "active_connections": self.connections, "active_requests": self.active,
                    "active_http_requests": self.http_requests,
                    "active_upstream_connections": self.upstream_connections,
                    "active_upstream_requests": self.upstream_active,
                    "active_upstream_listings": self.list_active,
                    "valid_observation": self.counters["failures"] == 0,
                    "lifetime_failures": self.total_failures,
                    "counters": dict(self.counters), "routes": dict(self.routes), "statuses": dict(self.statuses),
                    "scope": "actual observer traffic; FE PID and acceptance must be proven externally"}

    def reset(self, phase):
        need(isinstance(phase, str) and re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", phase), "invalid phase label")
        with self.lock:
            need(self.connections == self.http_requests == self.upstream_connections
                 == self.active == self.upstream_active == self.list_active == 0,
                 "phase reset requires actual owner idle")
            previous = self.snapshot()
            self.event({"event": "phase-finished", "snapshot": previous})
            self.phase = phase
            self.generation += 1
            self.reset_counters()
            self.event({"event": "phase-started", "phase": phase, "generation": self.generation})
            return {"previous": previous, "current": self.snapshot()}


class HeaderReader:
    def __init__(self, wrapped, cap):
        self.wrapped = wrapped
        self.remaining = cap
        self.header_mode = True

    def readline(self, limit=-1):
        if not self.header_mode:
            return self.wrapped.readline(limit)
        size = self.remaining + 1 if limit < 0 else min(limit, self.remaining + 1)
        raw = self.wrapped.readline(size)
        self.remaining -= len(raw)
        need(self.remaining >= 0, "request line/headers exceed cap")
        return raw

    def peek(self, count=0):
        return self.wrapped.peek(count)


    def read(self, count=-1):
        return self.wrapped.read(count)

    def close(self):
        self.wrapped.close()

    def flush(self):
        self.wrapped.flush()


class ReaderView:
    """A message borrows the pair's one real buffered reader, never closes it."""
    def __init__(self, reader, prefix=b""):
        self.reader, self.prefix = reader, prefix
        self.closed = False

    def read(self, count=-1):
        need(not self.closed, "message reader view is closed")
        if count == 0:
            return b""
        prefix, self.prefix = self.prefix, b""
        return prefix + self.reader.read(-1 if count < 0 else count - len(prefix))

    def readline(self, limit=-1):
        need(not self.closed, "message reader view is closed")
        prefix, self.prefix = self.prefix, b""
        if limit == len(prefix):
            return prefix
        return prefix + self.reader.readline(-1 if limit < 0 else limit - len(prefix))

    def close(self):
        self.closed = True

    def flush(self):
        pass


class ReaderSocketView:
    def __init__(self, reader):
        self.reader = reader

    def makefile(self, mode):
        need(mode == "rb", "unexpected message reader mode")
        return ReaderView(self.reader)


class BoundedResponse(http.client.HTTPResponse):
    def __init__(self, *args, header_cap, reader=None, **kwargs):
        self.transport_fp = None
        if reader is not None:
            args = (ReaderSocketView(reader), *args[1:])
        super().__init__(*args, **kwargs)
        self.fp = HeaderReader(self.fp, header_cap)

    def _close_conn(self):
        # HTTP framing completion is not transport EOF. Retain this response
        # view for an explicit close-framed EOF check. The pair owns the real
        # buffered reader and any prefetched bytes across persistent replies.
        if self.fp is not None:
            self.transport_fp = self.fp
            self.fp = None

    def require_transport_eof(self):
        # Close-delimited reads can reach EOF while HTTPResponse.fp remains
        # attached. Transfer that reader through the same ownership path.
        self._close_conn()
        need(self.transport_fp is not None, "upstream transport reader is missing")
        need(self.transport_fp.read(1) == b"", "bytes follow the complete upstream response")

    def close(self):
        try:
            super().close()
        finally:
            reader, self.transport_fp = self.transport_fp, None
            if reader is not None:
                reader.close()

    def begin(self):
        super().begin()
        if self.fp is not None:
            self.fp.header_mode = False

    def _read_and_discard_trailer(self):
        # http.client silently discards trailers. Stock Servlet writes none;
        # detecting an extension is safer than losing a semantic header while
        # claiming transparency. No response is substituted on this refusal.
        line = self.fp.readline(65537)
        need(len(line) <= 65536 and line in (b"\r\n", b"\n"),
             "downstream trailers outside frozen normal contract")


class ObserverServer(ThreadingMixIn, HTTPServer):
    daemon_threads = False
    block_on_close = True
    allow_reuse_address = False
    request_queue_size = 256

    def server_bind(self):
        # HTTPServer.server_bind uses socket.getfqdn even for numeric loopback;
        # no DNS operation is part of this private observer's startup contract.
        TCPServer.server_bind(self)
        self.server_name, self.server_port = self.server_address[:2]

    def __init__(self, address, downstream, audit, bounds=None):
        self.downstream = downstream_address(downstream)
        self.audit = audit
        self.bounds = BOUNDS if bounds is None else bounds
        self.positions = threading.BoundedSemaphore(self.bounds["max_observers"])
        self.admission_state = "QUIESCENT"
        self.pairs = {}
        self.quiesce_lock = threading.Lock()
        super().__init__(address, ObserverHandler)
        self.socket.setblocking(False)

    def _handle_request_noblock(self):
        # The listener is explicitly nonblocking. Keep accept, verification,
        # registration and thread start on one admission frontier; a socket
        # accepted in the old generation cannot appear after quiesce/reset.
        with self.audit.lock:
            super()._handle_request_noblock()

    def process_request(self, request, client_address):
        with self.audit.lock:
            # Register before setup/start so quiesce cannot miss an admitted
            # socket whose handler has not been scheduled yet.
            for key, pair in list(self.pairs.items()):
                if not pair.thread.is_alive():
                    pair.thread.join()
                    pair.release_position()
                    del self.pairs[key]
            if self.admission_state != "OPEN":
                self.audit.failure("connection arrived after phase admission closed", admission=True)
                self.shutdown_request(request)
                return
            if not self.positions.acquire(blocking=False):
                self.audit.failure("observer connection admission exhausted", admission=True)
                self.shutdown_request(request)
                return
            pair = ConnectionPair(self, request)
            self.pairs[id(request)] = pair
            self.audit.accept()
            pair.thread = threading.Thread(target=self.process_request_thread,
                                           args=(request, client_address), daemon=False)
            try:
                pair.thread.start()
            except BaseException:
                try:
                    pair.close()
                finally:
                    self.audit.release()
                    pair.release_position()
                    del self.pairs[id(request)]
                    self.shutdown_request(request)
                raise

    def process_request_thread(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except Exception:
            self.handle_error(request, client_address)
        finally:
            pair = self.pairs[id(request)]
            try:
                try:
                    pair.close()
                except Exception:
                    self.audit.failure("connection owner cleanup failed")
            finally:
                try:
                    self.shutdown_request(request)
                finally:
                    with self.audit.lock:
                        self.audit.release()

    def handle_error(self, request, client_address):
        # Never print exception bodies, Authorization, query text or payload.
        self.audit.failure("observer handler exception")

    def snapshot(self):
        with self.audit.lock:
            return {"command": "snapshot", "state": self.admission_state,
                    "audit": self.audit.snapshot()}

    def reset(self, phase):
        with self.audit.lock:
            need(self.admission_state == "QUIESCENT" and not self.pairs,
                 "phase reset requires joined connection owners")
            # One nonblocking closed-admission poll covers a queued socket
            # that the listener has not dispatched. Any one late acceptance
            # taints the old generation and prevents opening another phase.
            # With EAGAIN, this same-lock check/open is the admission cut,
            # not a claim about the wall-clock arrival of network packets.
            self._handle_request_noblock()
            need(self.audit.total_failures == 0 and self.audit.counters["failures"] == 0,
                 "phase reset cannot clear a failed observation")
            result = self.audit.reset(phase)
            self.admission_state = "OPEN"
            return {"command": "reset", "state": "OPEN", **result}

    def quiesce(self):
        with self.quiesce_lock:
            with self.audit.lock:
                self.admission_state = "QUIESCING"
                pairs = list(self.pairs.values())
                for pair in pairs:
                    pair.closing = True
                    if not pair.busy:
                        # Only wake the blocked idle client read here. Its
                        # owner checks buffered data and closes both readers.
                        pair.shutdown_client()
            for pair in pairs:
                pair.thread.join()
            with self.audit.lock:
                need(self.audit.connections == self.audit.http_requests == self.audit.upstream_connections
                     == self.audit.active == self.audit.upstream_active == self.audit.list_active == 0, "quiesce did not release actual owners")
                for pair in pairs:
                    pair.release_position()
                self.pairs.clear()
                self.admission_state = "QUIESCENT"
                return {"command": "quiesce", "state": "QUIESCENT", "audit": self.audit.snapshot()}

    def server_close(self):
        self.quiesce()
        HTTPServer.server_close(self)


class ConnectionPair:
    """One admitted client owner retains at most one affine upstream owner."""
    def __init__(self, server, client):
        self.server, self.client = server, client
        self.client_reader = self.upstream = self.upstream_reader = self.upstream_socket = None
        self.thread = None
        self.busy = self.closing = False
        self.idle_timed_out = False
        self.client_reset = False
        self.upstream_reset = False
        self.idle_exchange_complete = False
        self.position_held = True
        self.upstream_counted = False

    def release_position(self):
        # Called under the admission lock after actual thread join, or when
        # starting that thread failed. A returning thread grants no capacity.
        if self.position_held:
            self.position_held = False
            self.server.positions.release()

    @staticmethod
    def shutdown(sock):
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def shutdown_client(self):
        self.shutdown(self.client)

    def shutdown_all(self):
        self.shutdown_client()
        self.shutdown(self.upstream_socket)

    def pending(self, reader, sock):
        if reader is None or sock is None:
            return False
        previous = sock.gettimeout()
        try:
            # A positive timeout poisons SocketIO after expiry. Nonblocking
            # WouldBlock returns None instead; peek never discards bytes.
            sock.setblocking(False)
            return bool(reader.peek(1))
        finally:
            sock.settimeout(previous)

    def close_upstream(self):
        try:
            if self.upstream_reader is not None:
                self.upstream_reader.close()
        finally:
            self.upstream_reader = None
            try:
                if self.upstream is not None:
                    self.upstream.close()
            finally:
                self.upstream = None
                self.upstream_socket = None
                if self.upstream_counted:
                    self.server.audit.upstream_close()
                    self.upstream_counted = False

    def close(self):
        try:
            try:
                if (not self.idle_timed_out and not self.client_reset
                        and self.pending(self.client_reader, self.client)):
                    self.server.audit.failure("client bytes remain at connection retirement",
                                              admission=self.closing)
                if (not self.upstream_reset
                        and self.pending(self.upstream_reader, self.upstream_socket)):
                    self.server.audit.failure("upstream bytes remain at connection retirement")
            except (OSError, ValueError):
                self.server.audit.failure("connection retirement inspection failed")
        finally:
            try:
                self.close_upstream()
            finally:
                try:
                    if self.client_reader is not None:
                        self.client_reader.close()
                finally:
                    self.client_reader = None
                    self.client.close()


class ObserverHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    wbufsize = 0

    def setup(self):
        self.pair = self.server.pairs[id(self.request)]
        self.timer = None
        self.request_held = False
        self.connection = self.request
        self.connection.settimeout(self.server.bounds["request_absolute_deadline_seconds"])
        super().setup()
        self.pair.client_reader = self.rfile
        self.rfile = HeaderReader(ReaderView(self.pair.client_reader), self.server.bounds["max_header_bytes"])

    def finish(self):
        try:
            self.finish_timer()
        finally:
            if self.request_held:
                self.server.audit.request_exit()
                self.request_held = False
            super().finish()

    def finish_timer(self):
        if self.timer is not None:
            self.timer.cancel()
            self.timer.join()
            self.timer = None

    def log_message(self, format_string, *args):
        pass

    def send_error(self, *args, **kwargs):
        self.server.audit.failure("invalid HTTP request; no substitute response")
        self.close_connection = True

    def peek_idle_client(self):
        previous = self.connection.gettimeout()
        try:
            self.connection.setblocking(False)
            try:
                return self.pair.client_reader.peek(1)
            except BlockingIOError:
                return None
            except ConnectionResetError as error:
                if error.errno != errno.ECONNRESET:
                    raise
                return IdleReset("client", "buffered_client_peek", error)
        finally:
            self.connection.settimeout(previous)

    @staticmethod
    def peek_idle_socket(sock, side="client", probe="raw_client_peek"):
        previous = sock.gettimeout()
        try:
            sock.setblocking(False)
            try:
                return sock.recv(1, socket.MSG_PEEK)
            except BlockingIOError:
                return None
            except ConnectionResetError as error:
                if error.errno != errno.ECONNRESET:
                    raise
                return IdleReset(side, probe, error)
        finally:
            sock.settimeout(previous)


    def peek_idle_upstream(self):
        # The persistent pair's one reader may already own bytes beyond the
        # last response. A raw-fd-only peek must not hide those bytes.
        sock = self.pair.upstream_socket
        previous = sock.gettimeout()
        try:
            sock.setblocking(False)
            try:
                return self.pair.upstream_reader.peek(1)
            except BlockingIOError:
                return None
            except ConnectionResetError as error:
                if error.errno != errno.ECONNRESET:
                    raise
                return IdleReset("upstream", "buffered_upstream_peek", error)
        finally:
            sock.settimeout(previous)

    def retire_idle_reset(self, reset):
        # A reset is an idle transport conclusion only after the same pair's
        # previous exchange, request position and timer actually completed.
        if not self.pair.idle_exchange_complete or self.request_held or self.timer is not None:
            raise reset.error
        audit = self.server.audit
        with audit.lock:
            if reset.side == "client":
                self.pair.client_reset = True
                # Preserve the upstream pair owner's closed diagnostic event.
                audit.event({"event": "idle-peer-reset", "phase": audit.phase,
                             "generation": audit.generation,
                             "diagnostic": forwarding_failure_details("idle-client-read", reset.error, None)})
            else:
                self.pair.upstream_reset = True
            audit.event({"event": "idle-pair-reset", "phase": audit.phase,
                         "generation": audit.generation, "side": reset.side,
                         "probe": reset.probe, "errno": reset.error.errno,
                         "peer_ip": self.client_address[0], "peer_port": self.client_address[1],
                         "sequence": None, "previous_exchange_complete": True,
                         "first_byte_observed": False})
        self.close_connection = True
        return None

    def wait_client_first_byte(self):
        # Idle belongs to this original pair owner. Readiness/probes cannot
        # make a later send atomic with peer closure; any later failure still
        # takes forward's original no-replay path.
        while True:
            first_byte = None
            try:
                pending = self.peek_idle_client()
                if isinstance(pending, IdleReset):
                    return self.retire_idle_reset(pending)
                first_byte = time.monotonic() if pending else None
                upstream = self.pair.upstream_socket
                watched = [self.connection] + ([upstream] if upstream is not None else [])
                readable, _, _ = select.select(watched, [], [], 0 if pending else None)
                if not pending and self.connection in readable:
                    pending = self.peek_idle_client()
                    if isinstance(pending, IdleReset):
                        return self.retire_idle_reset(pending)
                    if pending:
                        first_byte = time.monotonic()
                    else:
                        client_input = self.peek_idle_socket(self.connection)
                        if isinstance(client_input, IdleReset):
                            return self.retire_idle_reset(client_input)
                        if client_input == b"":
                            return None
                        if client_input:
                            # Bytes can arrive between the buffered peek and
                            # this raw peek. Preserve their first observation;
                            # the stdlib parser still consumes the same bytes.
                            pending = client_input
                            first_byte = time.monotonic()
                if upstream is not None and upstream in readable:
                    upstream_input = self.peek_idle_upstream()
                    if isinstance(upstream_input, IdleReset):
                        if pending:
                            return first_byte, upstream_input.error
                        # An upstream reset can race with client input. It is
                        # not idle merely because the earlier probe was empty.
                        pending = self.peek_idle_client()
                        if isinstance(pending, IdleReset):
                            return self.retire_idle_reset(pending)
                        if pending:
                            return time.monotonic(), upstream_input.error
                        return self.retire_idle_reset(upstream_input)
                    if upstream_input == b"":
                        # Check buffered/client input again before classifying a
                        # closure as idle. Simultaneous input must retain request
                        # failure, never become a clean idle success or reconnect.
                        if not pending:
                            pending = self.peek_idle_client()
                            if isinstance(pending, IdleReset):
                                return self.retire_idle_reset(pending)
                            if pending:
                                first_byte = time.monotonic()
                        if pending:
                            return first_byte, Refusal("upstream EOF with pending client request")
                        audit = self.server.audit
                        with audit.lock:
                            audit.event({"event": "idle-upstream-eof", "phase": audit.phase,
                                         "generation": audit.generation,
                                         "peer_ip": self.client_address[0],
                                         "peer_port": self.client_address[1]})
                        self.close_connection = True
                        return None
                    if upstream_input is not None:
                        if pending:
                            return first_byte, Refusal("unexpected upstream input with pending client request")
                        raise Refusal("unexpected upstream input while no request is outstanding")
                if pending:
                    return first_byte, None
            except (Refusal, OSError, ValueError, http.client.HTTPException) as error:
                if first_byte is not None:
                    return first_byte, error
                raise


    def handle(self):
        self.close_connection = False
        while not self.close_connection:
            completed_exchange = False
            operation = "prepare_client_idle_wait"
            try:
                with self.server.audit.lock:
                    if self.pair.closing:
                        return
                    self.pair.busy = False
                self.connection.settimeout(None)
                operation = "wait_client_first_byte"
                ready = self.wait_client_first_byte()
                if ready is None:
                    return
                self.started, idle_refusal = ready
                with self.server.audit.lock:
                    if self.pair.closing or self.server.admission_state != "OPEN":
                        self.server.audit.failure("request arrived after phase admission closed", admission=True)
                        return
                    self.pair.busy = True
                    self.server.audit.request_enter()
                    self.request_held = True
                self.pair.idle_exchange_complete = False
                self.deadline = self.started + self.server.bounds["request_absolute_deadline_seconds"]
                self.connection.settimeout(max(0.001, self.deadline - time.monotonic()))
                self.rfile = HeaderReader(ReaderView(self.pair.client_reader), self.server.bounds["max_header_bytes"])

                def expire():
                    with self.server.audit.lock:
                        self.server.audit.failure("observer RPC absolute deadline expired")
                        self.pair.closing = True
                        self.pair.shutdown_all()

                self.timer = threading.Timer(max(0, self.deadline - time.monotonic()), expire)
                self.timer.daemon = True
                self.timer.start()
                if idle_refusal is not None:
                    operation = "idle_upstream_refusal_with_client_input"
                    raise idle_refusal
                operation = "parse_client_request"
                self.handle_one_request()
                operation = "post_client_request_deadline"
                self.check()
                completed_exchange = not self.close_connection
            except (Refusal, OSError, ValueError, http.client.HTTPException) as error:
                with self.server.audit.lock:
                    if operation == "wait_client_first_byte" and self.pair.closing:
                        return
                    self.server.audit.failure("request parse/transport refusal",
                                              operation=operation, error=error,
                                              request_held=self.request_held,
                                              diagnostic=forwarding_failure_details(
                                                  "idle-client-read" if not self.request_held else "request-validate", error, None))
                self.close_connection = True
            finally:
                self.finish_timer()
                if self.request_held:
                    self.server.audit.request_exit()
                    self.request_held = False
                self.pair.idle_exchange_complete = completed_exchange
            with self.server.audit.lock:
                if self.close_connection or self.pair.closing:
                    return
                self.pair.busy = False

    def check(self):
        need(time.monotonic() < self.deadline, "observer RPC absolute deadline expired")

    def forward(self):
        audit = self.server.audit
        record = None
        connection = None
        response = None
        upstream_held = False
        category = "other"
        stage = "request-validate"
        operation = "validate_request"
        try:
            self.check()
            self.rfile.header_mode = False
            need(len(self.path.encode()) <= self.server.bounds["max_request_bytes"], "request target exceeds cap")
            category = route(self.command, self.path)
            headers = list(self.headers.raw_items())
            hop, length, chunked = header_facts(headers, self.server.bounds["max_header_bytes"])
            need(not chunked, "chunked request outside frozen normal client contract")
            need(not any(k.lower() == "expect" for k, _ in headers), "Expect request outside frozen normal contract")
            hosts = [v for k, v in headers if k.lower() == "host"]
            need(len(hosts) == 1, "request Host is not unique")
            length = 0 if length is None else length
            need(length + len(self.path.encode()) <= self.server.bounds["max_request_bytes"], "request bytes exceed cap")
            record = audit.begin(self.command, self.path, category, self.client_address)
            stage = "request-body"
            operation = "read_request_body"
            body = self.rfile.read(length)
            need(len(body) == length, "truncated request body")
            self.check()
            record["request_body_bytes"] = len(body)
            record["request_body_sha256"] = sha(body)
            upstream = self.server.downstream
            stage = "upstream-open"
            operation = "connect_upstream"
            connection = self.pair.upstream
            if connection is None:
                connection = http.client.HTTPConnection(upstream.hostname, upstream.port,
                                                        timeout=max(0.001, self.deadline - time.monotonic()))
                connection.auto_open = 0
                self.pair.upstream = connection
                stage = "upstream-connect"
                connection.connect()
                self.pair.upstream_socket = connection.sock
                audit.upstream_open()
                self.pair.upstream_counted = True
                self.pair.upstream_reader = connection.sock.makefile("rb")
                connection.response_class = lambda *a, **kw: BoundedResponse(
                    *a, header_cap=self.server.bounds["max_header_bytes"],
                    reader=self.pair.upstream_reader, **kw)
            self.pair.upstream_socket.settimeout(max(0.001, self.deadline - time.monotonic()))
            self.check()
            audit.upstream_begin(category)
            upstream_held = True
            stage = "upstream-send"
            operation = "send_upstream_headers"
            connection.putrequest(self.command, self.path, skip_host=True, skip_accept_encoding=True)
            for name, value in headers:
                if name.lower() not in hop and name.lower() != "host":
                    connection.putheader(name, value)
            connection.putheader("Host", upstream.netloc)
            connection.endheaders(body)
            record["upstream_request_sent"] = True
            audit.sent(self.command)
            stage = "upstream-response"
            operation = "receive_upstream_headers"
            response = connection.getresponse()
            record["status"] = response.status
            if 300 <= response.status < 400:
                # Preserve the actual redirect, but a client following Location
                # could bypass this observer. Such a phase is not an actual
                # complete-traffic receipt for the frozen downstream owner.
                audit.failure("actual redirect creates an observer coverage gap")
            response_headers = list(response.headers.raw_items())
            response_hop, declared, transfer = header_facts(response_headers, self.server.bounds["max_header_bytes"])
            need(response.status >= 200 and response.version in (10, 11)
                 and not any(c in response.reason for c in ("\r", "\n", "\x00")), "unsupported downstream status line")
            if self.command != "HEAD" and declared is not None:
                need(declared <= self.server.bounds["max_response_bytes"], "declared response body exceeds cap")
            operation = "receive_upstream_body"
            raw = response.read(self.server.bounds["max_response_bytes"] + 1)
            self.check()
            need(len(raw) <= self.server.bounds["max_response_bytes"], "response body exceeds cap")
            if self.command != "HEAD" and declared is not None:
                need(len(raw) == declared, "truncated downstream body")
            record["status"] = response.status
            record["response_body_bytes"] = len(raw)
            record["response_body_sha256"] = sha(raw)
            record["semantic_headers_sha256"] = sha(encode([
                (k, v) for k, v in response_headers if k.lower() not in response_hop and k.lower() != "content-length"]))
            if response.will_close:
                stage = "upstream-transport-eof"
                response.require_transport_eof()
            self.check()
            response.close()
            if response.will_close:
                self.pair.close_upstream()
            audit.upstream_end(category)
            upstream_held = False
            facts = {"pages": 0, "names": 0, "name_bytes": 0}
            try:
                facts = listing_facts(category, self.path, raw, response_headers, response.status,
                                      self.server.bounds["max_decoded_json_bytes"])
            except (Refusal, UnicodeError, ValueError):
                audit.failure("actual listing body cannot be observed; forwarded unchanged")
            record["listing"] = facts
            audit.received(record, facts)
            stage = "client-send"
            operation = "send_client_headers"
            version = "HTTP/1.0" if response.version == 10 else "HTTP/1.1"
            lines = [f"{version} {response.status} {response.reason}\r\n"]
            for name, value in response_headers:
                if name.lower() not in response_hop and name.lower() != "content-length":
                    lines.append(name + ": " + value + "\r\n")
            # Keep HEAD's actual representation length; otherwise preserve the
            # original Content-Length string or expose exact dechunked body size.
            original_lengths = [v for k, v in response_headers if k.lower() == "content-length"]
            forwarded_length = original_lengths[0] if original_lengths else str(len(raw))
            with audit.lock:
                self.close_connection = self.close_connection or self.pair.closing or response.will_close
            lines.extend(["Content-Length: " + forwarded_length + "\r\n",
                          "Connection: close\r\n" if self.close_connection else "Connection: keep-alive\r\n", "\r\n"])
            self.connection.sendall("".join(lines).encode("latin-1"))
            operation = "send_client_body"
            remaining = memoryview(raw)
            while remaining:
                self.check()
                sent = self.connection.send(remaining)
                need(sent > 0, "downstream client closed during body send")
                record["emitted_body_bytes"] += sent
                audit.emitted(sent)
                remaining = remaining[sent:]
            stage = "request-completion"
            self.check()
            record["outcome"] = "actual-response-forwarded"
            audit.complete()
        except (Refusal, OSError, ValueError, http.client.HTTPException) as error:
            self.close_connection = True
            with audit.lock:
                self.pair.closing = True
            audit.failure("bounded forwarding failed; no substitute response or retry",
                          operation=operation, error=error,
                          diagnostic=forwarding_failure_details(
                              stage, error, record["sequence"] if record is not None else None))
        finally:
            if upstream_held:
                audit.upstream_end(category)
            try:
                if response is not None:
                    response.close()
            finally:
                if record is not None:
                    record["elapsed_ms"] = round((time.monotonic() - self.started) * 1000, 3)
                    audit.end(record)

    do_GET = forward
    do_POST = forward
    do_DELETE = forward
    do_HEAD = forward
    do_PUT = forward
    do_PATCH = forward
    do_OPTIONS = forward

    def handle_expect_100(self):
        self.send_error()
        return False


class PureTests(unittest.TestCase):
    def reject(self, callback):
        with self.assertRaises(Refusal):
            callback()

    def test_parser_rejects_duplicate_missing_cross_scope(self):
        headers = [("Content-Type", "application/json")]
        for body in (b'{"identifiers":[],"identifiers":[]}', b'{}',
                     b'{"identifiers":[{"namespace":["foreign"],"name":"t"}]}',
                     b'{"identifiers":[],"next-page-token":false}'):
            self.reject(lambda b=body: listing_facts("list_tables", "/v1/namespaces/ns/tables", b, headers, 200))
        self.reject(lambda: parse_json(b'{"x":NaN}'))

    def test_header_ambiguity_and_semantic_hop(self):
        for headers in ([('Content-Length', '1'), ('Content-Length', '1')],
                        [('Transfer-Encoding', 'chunked'), ('Content-Length', '1')],
                        [('X-Test', 'x\r\ny')], [('Connection', 'authorization')]):
            self.reject(lambda h=headers: header_facts(h))
        self.reject(lambda: header_facts([('X-Test', 'a' * 65536)]))

    def test_response_header_cap_applies_before_full_header_parse(self):
        class LocalBytesSocket:
            reader = io.BytesIO(b"HTTP/1.1 200 OK\r\nX-Large: " + b"x" * 1024 + b"\r\n\r\n")

            def makefile(self, *args):
                return self.reader

        sock = LocalBytesSocket()
        response = BoundedResponse(sock, method="GET", header_cap=64)
        try:
            self.reject(response.begin)
        finally:
            response.close()
        self.assertTrue(sock.reader.closed)
        self.assertIsNone(response.transport_fp)

    def test_gzip_bound_and_exact_name_bytes(self):
        body = '{"identifiers":[{"namespace":["ns"],"name":"雪"}]}'.encode()
        headers = [("Content-Type", "application/json")]
        facts = listing_facts("list_tables", "/v1/namespaces/ns/tables", body, headers, 200)
        self.assertEqual((facts["pages"], facts["names"], facts["name_bytes"]), (1, 1, 3))
        compressor = zlib.compressobj(wbits=16 + zlib.MAX_WBITS)
        gzip = compressor.compress(body) + compressor.flush()
        compressed_headers = headers + [("Content-Encoding", "gzip")]
        self.assertEqual(listing_facts("list_tables", "/v1/namespaces/ns/tables", gzip, compressed_headers, 200), facts)
        self.reject(lambda: listing_facts("list_tables", "/v1/namespaces/ns/tables", gzip,
                                         compressed_headers, 200, cap=8))

    def test_reset_requires_actual_idle(self):
        audit = Audit()
        audit.accept()
        self.reject(lambda: audit.reset("native_fe"))
        audit.release()
        audit.request_enter()
        self.reject(lambda: audit.reset("native_fe"))
        audit.request_exit()
        record = audit.begin("GET", "/v1/config", "other", ("127.0.0.1", 1))
        self.reject(lambda: audit.reset("native_fe"))
        audit.end(record)
        audit.reset("native_fe")
        self.assertEqual(audit.snapshot()["phase"], "native_fe")

    def test_no_proxy_absolute_target_or_foreign_host(self):
        self.reject(lambda: downstream_address("http://rest:8181"))
        self.reject(lambda: downstream_address("http://192.0.2.1:8181"))
        self.reject(lambda: route("GET", "http://other/v1/namespaces"))
        self.reject(lambda: route("GET", "/not-a-catalog"))

    def test_forward_connect_failure_retains_errno_without_private_diagnostics(self):
        import errno
        from types import SimpleNamespace
        from unittest import mock

        import tempfile

        ledger = self.enterContext(tempfile.TemporaryFile())
        audit = Audit(ledger)
        handler = object.__new__(ObserverHandler)
        handler.server = SimpleNamespace(audit=audit, bounds=BOUNDS,
                                         downstream=urlsplit("http://127.0.0.1:1"))
        handler.pair = ConnectionPair(handler.server, mock.Mock())
        handler.close_connection = False
        handler.request_version = "HTTP/1.1"
        handler.command = "GET"
        handler.path = "/v1/namespaces/private/tables/literal?token=private-token"
        handler.headers = http.client.parse_headers(io.BytesIO(
            b"Host: 127.0.0.1\r\nAuthorization: Bearer private-token\r\n\r\n"))
        handler.rfile = io.BytesIO()
        handler.client_address = ("127.0.0.1", 1)
        handler.started = time.monotonic()
        handler.deadline = handler.started + BOUNDS["request_absolute_deadline_seconds"]
        connection = mock.Mock()
        connection.connect.side_effect = OSError(errno.EADDRNOTAVAIL,
                                                 "private-message", "private-filename")
        with mock.patch.object(http.client, "HTTPConnection", return_value=connection):
            handler.forward()
        handler.pair.close()
        connection.connect.assert_called_once()
        connection.close.assert_called_once()
        connection.putrequest.assert_not_called()
        ledger.seek(0)
        recorded = ledger.read()
        events = [parse_json(line) for line in recorded.splitlines()]
        failure = next(event for event in events if event["event"] == "failure")
        self.assertEqual(failure["error"], {"type": "OSError", "errno": errno.EADDRNOTAVAIL,
                                           "operation": "connect_upstream"})
        self.assertEqual(failure["reason"], "bounded forwarding failed; no substitute response or retry")
        self.assertEqual(audit.snapshot()["counters"]["completed"], 0)
        self.assertEqual(audit.snapshot()["active_requests"], 0)
        self.assertFalse(audit.snapshot()["valid_observation"])
        for private in (b"private-token", b"private-message", b"private-filename", b"Authorization"):
            self.assertNotIn(private, recorded)


    def test_forward_failure_diagnostics_are_closed_and_never_render_exception_text(self):
        private = "PRIVATE Authorization=secret http://private/path?token=secret body=secret"
        cases = [(OSError(errno.EADDRNOTAVAIL, private), "os-error", errno.EADDRNOTAVAIL),
                 (Refusal(private), "refusal", None),
                 (http.client.HTTPException(private), "http-error", None),
                 (ValueError(private), "value-error", None),
                 (RuntimeError(private), "other-error", None)]
        for error, category, number in cases:
            with self.subTest(category=category):
                details = forwarding_failure_details("upstream-connect", error, 7)
                self.assertEqual(details, {"operation_stage": "upstream-connect",
                                          "exception_category": category, "errno": number,
                                          "request_sequence": 7})
                self.assertNotIn(private.encode(), encode(details))
                ledger = io.BytesIO()
                audit = Audit(ledger)
                audit.failure("bounded forwarding failed; no substitute response or retry",
                              diagnostic=details)
                self.assertNotIn(private.encode(), ledger.getvalue())
                audit.ledger = None
                snapshot = audit.snapshot()
                self.assertFalse(snapshot["valid_observation"])
                self.assertEqual(snapshot["lifetime_failures"], 1)
                self.assertEqual(snapshot["counters"]["completed"], 0)
        error = OSError(errno.EIO, private)
        error.errno = private
        details = forwarding_failure_details(private, error, True)
        self.assertEqual(details, {"operation_stage": "unknown-operation",
                                  "exception_category": "os-error", "errno": None,
                                  "request_sequence": None})
        self.assertNotIn(private.encode(), encode(details))

    def test_idle_read_diagnostic_keeps_failure_without_a_request(self):
        private = "PRIVATE peer URL Authorization password body"
        for error, number in [(OSError(errno.EIO, private), errno.EIO), (OSError(private), None)]:
            with self.subTest(errno=number):
                ledger = io.BytesIO()
                audit = Audit(ledger)
                handler = ObserverHandler.__new__(ObserverHandler)
                handler.server = mock.Mock(audit=audit, bounds=BOUNDS)
                handler.connection = mock.Mock()
                handler.pair = mock.Mock(closing=False, upstream_socket=None)
                handler.request_held = False
                handler.timer = None
                handler.pair.client_reader.peek.side_effect = error
                handler.handle()
                events = [parse_json(line) for line in ledger.getvalue().splitlines()]
                self.assertEqual(events, [{
                    "event": "failure", "phase": "unclassified", "generation": 1,
                    "reason": "request parse/transport refusal",
                    "error": {"type": type(error).__name__, "errno": number, "operation": "wait_client_first_byte"},
                    "request_held": False,
                    "diagnostic": {"operation_stage": "idle-client-read",
                                   "exception_category": "os-error", "errno": number,
                                   "request_sequence": None}}])
                self.assertNotIn(private.encode(), ledger.getvalue())
                audit.ledger = None
                snapshot = audit.snapshot()
                self.assertFalse(snapshot["valid_observation"])
                self.assertEqual(snapshot["lifetime_failures"], 1)
                self.assertEqual(snapshot["counters"]["requests"], 0)
                self.assertEqual(snapshot["active_requests"], 0)


    def test_request_boundary_failures_keep_safe_cause_and_original_taint(self):
        import errno
        import select as select_module
        import tempfile
        from types import SimpleNamespace
        from unittest import mock

        cases = (
            (False, OSError(errno.ECONNRESET, "private-message", "private-filename"),
             "wait_client_first_byte"),
            (False, OSError(errno.EIO, "private-message", "private-filename"),
             "wait_client_first_byte"),
            (True, OSError(errno.ECONNRESET, "private-message", "private-filename"),
             "parse_client_request"),
            (True, Refusal("private-refusal-message"), "parse_client_request"),
        )
        for first_byte, error, operation in cases:
            with self.subTest(first_byte=first_byte, error=type(error).__name__), tempfile.TemporaryFile() as ledger:
                audit = Audit(ledger)
                handler = object.__new__(ObserverHandler)
                handler.server = SimpleNamespace(audit=audit, bounds=BOUNDS, admission_state="OPEN")
                handler.connection = mock.Mock()
                handler.rfile = mock.Mock()
                handler.pair = SimpleNamespace(client_reader=handler.rfile, upstream_socket=None, idle_exchange_complete=False, closing=False, busy=False)
                handler.close_connection = False
                handler.request_held = False
                handler.timer = None
                handler.socket_lock = threading.Lock()
                handler.path = "/v1/namespaces/private?token=private-token"
                handler.headers = {"Authorization": "Bearer private-token"}
                handler.handle_one_request = mock.Mock(side_effect=error)
                if first_byte:
                    handler.rfile.peek.return_value = b"G"
                else:
                    handler.rfile.peek.side_effect = error
                timer = mock.Mock()
                with mock.patch.object(threading, "Timer", return_value=timer), \
                        mock.patch.object(select_module, "select", return_value=([], [], [])):
                    handler.handle()
                ledger.seek(0)
                recorded = ledger.read()
                events = [parse_json(line) for line in recorded.splitlines()]
                self.assertEqual(len(events), 1)
                failure = events[0]
                self.assertEqual(failure["reason"], "request parse/transport refusal")
                self.assertEqual(failure["request_held"], first_byte)
                self.assertEqual(failure["error"], {
                    "type": type(error).__name__,
                    "errno": error.errno if isinstance(error, OSError) else None,
                    "operation": operation,
                })
                self.assertTrue(handler.close_connection)
                self.assertFalse(handler.request_held)
                self.assertEqual(audit.snapshot()["active_http_requests"], 0)
                self.assertEqual(audit.snapshot()["counters"]["requests"], 0)
                self.assertEqual(audit.snapshot()["counters"]["failures"], 1)
                self.assertEqual(audit.snapshot()["lifetime_failures"], 1)
                self.assertFalse(audit.snapshot()["valid_observation"])
                if first_byte:
                    handler.handle_one_request.assert_called_once()
                    timer.cancel.assert_called_once()
                    timer.join.assert_called_once()
                else:
                    handler.handle_one_request.assert_not_called()
                    timer.start.assert_not_called()
                for private in (b"private-token", b"private-message", b"private-filename",
                                b"private-refusal-message", b"Authorization", b"/v1/"):
                    self.assertNotIn(private, recorded)


    def test_upstream_reset_rechecks_buffered_client_before_idle_classification(self):
        import errno
        import select as select_module
        import tempfile
        from types import SimpleNamespace
        from unittest import mock

        with tempfile.TemporaryFile() as ledger:
            audit = Audit(ledger)
            handler = object.__new__(ObserverHandler)
            handler.server = SimpleNamespace(audit=audit, bounds=BOUNDS, admission_state="OPEN")
            handler.connection = mock.Mock()
            handler.rfile = mock.Mock()
            handler.rfile.peek.side_effect = [b"", b"POST"]
            handler.pair = SimpleNamespace(client_reader=handler.rfile, upstream_socket=mock.Mock(), upstream_reader=mock.Mock(), idle_exchange_complete=True, closing=False, busy=False)
            handler.pair.upstream_reader.peek.side_effect = ConnectionResetError(errno.ECONNRESET, "private-reset")
            handler.close_connection = False
            handler.request_held = False
            handler.timer = None
            handler.socket_lock = threading.Lock()
            handler.handle_one_request = mock.Mock(side_effect=AssertionError("pending reset cannot dispatch"))
            timer = mock.Mock()
            with mock.patch.object(select_module, "select", return_value=([handler.pair.upstream_socket], [], [])), \
                    mock.patch.object(threading, "Timer", return_value=timer):
                handler.handle()
            handler.handle_one_request.assert_not_called()
            timer.start.assert_called_once()
            timer.cancel.assert_called_once()
            timer.join.assert_called_once()
            self.assertEqual(handler.deadline - handler.started, BOUNDS["request_absolute_deadline_seconds"])
            ledger.seek(0)
            events = [parse_json(line) for line in ledger.read().splitlines()]
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["event"], "failure")
            self.assertTrue(events[0]["request_held"])
            self.assertEqual(events[0]["error"]["errno"], errno.ECONNRESET)
            self.assertNotIn(b"private-reset", encode(events))
            self.assertFalse(audit.snapshot()["valid_observation"])



class LoopbackTests(unittest.TestCase):
    """Synthetic HTTP byte transparency only, deliberately outside provider gates."""

    def setUp(self):
        self.captured = []
        self.backend_peers = []
        self.upstream_peers = self.backend_peers
        self.upstream_closed = threading.Event()
        self.allow_idle_reset = threading.Event()
        allow_idle_reset = self.allow_idle_reset
        upstream_closed = self.upstream_closed
        self.backend_eof = threading.Event()
        captured = self.captured
        backend_peers = self.backend_peers
        backend_eof = self.backend_eof
        literal = b'{ "identifiers" : [ {"namespace":["ns"],"name":"literal"} ], "next-page-token":null }\n'
        self.literal = literal
        self.body_sent = threading.Event()
        self.peer_checked = threading.Event()
        self.allow_fin = threading.Event()
        self.peer_closed = threading.Event()
        self.premature_close = []
        body_sent, peer_checked, allow_fin = self.body_sent, self.peer_checked, self.allow_fin
        peer_closed, premature_close = self.peer_closed, self.premature_close

        class LiteralHandler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def handle_literal(inner):
                length = int(inner.headers.get("Content-Length", "0"))
                captured.append((inner.command, inner.path, inner.rfile.read(length),
                                 inner.headers.get("Authorization"), inner.headers.get_all("X-Exact")))
                backend_peers.append(inner.client_address)
                if inner.command == "POST" and "reset_before_headers" in inner.path:
                    inner.close_connection = True
                    inner.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                    inner.connection.close()
                    upstream_closed.set()
                    return
                if inner.command == "POST" and "close_before_headers" in inner.path:
                    inner.close_connection = True
                    inner.connection.shutdown(socket.SHUT_RDWR)
                    upstream_closed.set()
                    return
                status = 409 if "error" in inner.path else 200
                if "slow" in inner.path:
                    time.sleep(0.2)
                inner.send_response_only(status, "Literal reason")
                inner.send_header("Content-Type", "application/json")
                chunked = "chunked" in inner.path
                payload = b"" if "empty" in inner.path else literal
                if chunked:
                    inner.send_header("Transfer-Encoding", "chunked")
                elif "close-delimited" not in inner.path:
                    inner.send_header("Content-Length", str(len(payload)))
                inner.send_header("X-Downstream", "unchanged")
                inner.send_header("Set-Cookie", "one=1")
                inner.send_header("Set-Cookie", "two=2")
                keepalive = "keepalive" in inner.path
                inner.send_header("Connection", "keep-alive" if keepalive else "close")
                inner.end_headers()
                if inner.command != "HEAD":
                    try:
                        if chunked:
                            for start in range(0, len(literal), 7):
                                chunk = literal[start:start + 7]
                                inner.wfile.write(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
                            if "truncated" not in inner.path:
                                inner.wfile.write(b"0\r\n\r\n")
                            elif "trailer" in inner.path:
                                inner.wfile.write(b"0\r\n")
                        else:
                            inner.wfile.write(payload[:-1] if "truncated" in inner.path else payload + (b"EXTRA" if "extra" in inner.path else b""))
                    except OSError:
                        pass
                if "delayed-fin" in inner.path:
                    body_sent.set()
                    inner.connection.settimeout(0.05)
                    try:
                        premature_close.append(inner.connection.recv(1))
                    except TimeoutError:
                        pass
                    except OSError:
                        premature_close.append(b"closed")
                    peer_checked.set()
                    allow_fin.wait(2)
                elif "no-fin" in inner.path:
                    body_sent.set()
                    inner.connection.settimeout(1)
                    try:
                        if inner.connection.recv(1) == b"":
                            peer_closed.set()
                    except ConnectionResetError:
                        peer_closed.set()
                inner.close_connection = (not keepalive or "truncated" in inner.path
                                          or "silent_close" in inner.path or "idle_reset" in inner.path)

            do_GET = handle_literal
            do_POST = handle_literal
            do_HEAD = handle_literal

            def log_message(inner, *args):
                pass

            def finish(inner):
                try:
                    super().finish()
                finally:
                    if "idle_reset" in getattr(inner, "path", ""):
                        allow_idle_reset.wait(2)
                        inner.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
                        inner.connection.close()
                        upstream_closed.set()
                    if "silent_close" in getattr(inner, "path", ""):
                        inner.connection.shutdown(socket.SHUT_RDWR)
                        upstream_closed.set()
                    if getattr(inner, "raw_requestline", None) == b"":
                        backend_eof.set()

        class LocalLiteralServer(ThreadingHTTPServer):
            def server_bind(inner):
                TCPServer.server_bind(inner)
                inner.server_name, inner.server_port = inner.server_address[:2]

        self.backend = LocalLiteralServer(("127.0.0.1", 0), LiteralHandler)
        self.backend_thread = threading.Thread(target=self.backend.serve_forever, kwargs={"poll_interval": 0.01})
        self.backend_thread.start()
        self.audit = Audit()
        self.observer = ObserverServer(("127.0.0.1", 0), "http://127.0.0.1:" + str(self.backend.server_port), self.audit)
        self.observer.reset("synthetic")
        self.observer_thread = threading.Thread(target=self.observer.serve_forever, kwargs={"poll_interval": 0.01})
        self.observer_thread.start()

    def tearDown(self):
        self.allow_idle_reset.set()
        self.allow_fin.set()
        self.observer.shutdown()
        self.observer.server_close()
        self.observer_thread.join()
        self.backend.shutdown()
        self.backend.server_close()
        self.backend_thread.join()

    def call(self, method, path, body=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        connection.putrequest(method, path, skip_accept_encoding=True)
        connection.putheader("Authorization", "Bearer synthetic-local-secret")
        connection.putheader("X-Exact", "alpha")
        connection.putheader("X-Exact", "beta")
        connection.putheader("Content-Length", str(len(body or b"")))
        connection.endheaders(body)
        response = connection.getresponse()
        result = (response.status, response.reason, list(response.headers.raw_items()), response.read())
        response.close()
        connection.close()
        # The server socket closes before HTTP owner release. Wait for actual
        # idle rather than guessing completion from the client's EOF.
        deadline = time.monotonic() + 2
        while self.audit.snapshot()["active_connections"]:
            need(time.monotonic() < deadline, "local observer did not release connection")
            time.sleep(0.001)
        return result

    def test_literal_body_and_duplicate_semantic_headers(self):
        status, reason, headers, body = self.call("GET", "/v1/namespaces/ns/tables?pageSize=256&pageToken=opaque%2Btoken")
        self.assertEqual((status, reason, body), (200, "Literal reason", self.literal))
        self.assertEqual([v for k, v in headers if k.lower() == "set-cookie"], ["one=1", "two=2"])
        self.assertIn(("X-Downstream", "unchanged"), headers)
        self.assertEqual(self.captured[0][1], "/v1/namespaces/ns/tables?pageSize=256&pageToken=opaque%2Btoken")
        self.assertEqual(self.captured[0][3], "Bearer synthetic-local-secret")
        self.assertEqual(self.captured[0][4], ["alpha", "beta"])
        facts = self.audit.snapshot()
        self.assertTrue(facts["valid_observation"])
        self.assertEqual(facts["counters"]["names"], 1)
        self.assertEqual(facts["counters"]["emitted_body_bytes"], len(self.literal))

    def test_actual_error_and_request_body_are_not_rewritten(self):
        request = b'{ "properties": {"literal":"\\u96ea"} }\n'
        status, reason, headers, body = self.call("POST", "/v1/namespaces/ns/error?literal=%2F", request)
        self.assertEqual((status, reason, body), (409, "Literal reason", self.literal))
        self.assertEqual(self.captured[0][0:3], ("POST", "/v1/namespaces/ns/error?literal=%2F", request))
        snapshot = self.audit.snapshot()
        self.assertEqual(snapshot["counters"]["mutations"], 1)
        self.assertEqual(snapshot["statuses"], {"409": 1})

    def test_head_keeps_actual_representation_length(self):
        status, reason, headers, body = self.call("HEAD", "/v1/namespaces/ns/tables/literal")
        self.assertEqual((status, body), (200, b""))
        self.assertEqual([v for k, v in headers if k.lower() == "content-length"], [str(len(self.literal))])

    def test_chunked_actual_payload_and_semantic_headers_are_preserved(self):
        status, reason, headers, body = self.call("GET", "/v1/namespaces/ns/tables?pageSize=256&chunked=true")
        self.assertEqual((status, reason, body), (200, "Literal reason", self.literal))
        self.assertEqual([v for k, v in headers if k.lower() == "set-cookie"], ["one=1", "two=2"])
        self.assertEqual([v for k, v in headers if k.lower() == "content-length"], [str(len(self.literal))])
        self.assertTrue(self.audit.snapshot()["valid_observation"])

    def assert_idle(self):
        deadline = time.monotonic() + 2
        while self.audit.snapshot()["active_connections"]:
            need(time.monotonic() < deadline, "local observer transport did not exit")
            time.sleep(0.001)
        snapshot = self.audit.snapshot()
        self.assertEqual([snapshot[field] for field in (
            "active_connections", "active_requests", "active_upstream_requests", "active_upstream_listings")],
            [0, 0, 0, 0])

    def test_empty_and_close_delimited_responses_are_transparent(self):
        for path, expected in (("/v1/config?empty=true", b""),
                               ("/v1/config?close-delimited=true", self.literal)):
            with self.subTest(path=path):
                status, reason, headers, body = self.call("GET", path)
                self.assertEqual((status, reason, body), (200, "Literal reason", expected))
                self.assertEqual([v for k, v in headers if k.lower() == "set-cookie"], ["one=1", "two=2"])
                self.assertEqual([v for k, v in headers if k.lower() == "content-length"], [str(len(expected))])
                self.assert_idle()
        self.assertEqual(len(self.captured), 2)
        self.assertTrue(self.audit.snapshot()["valid_observation"])

    def test_complete_body_waits_for_server_fin_before_forwarding_or_idle(self):
        results, failures = [], []

        def client():
            try:
                results.append(self.call("GET", "/v1/config?delayed-fin=true"))
            except Exception as error:
                failures.append(error)

        worker = threading.Thread(target=client)
        worker.start()
        try:
            self.assertTrue(self.body_sent.wait(1))
            self.assertTrue(self.peer_checked.wait(1))
            self.assertEqual(self.premature_close, [])
            self.assertTrue(worker.is_alive())
            snapshot = self.audit.snapshot()
            self.assertEqual(snapshot["active_upstream_requests"], 1)
            self.assertEqual(snapshot["counters"]["completed"], 0)
            self.reject_reset_busy()
        finally:
            self.allow_fin.set()
            worker.join(2)
        self.assertFalse(worker.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(results[0][0:2], (200, "Literal reason"))
        self.assertEqual(results[0][3], self.literal)
        self.assertEqual(len(self.captured), 1)
        self.assertTrue(self.audit.snapshot()["valid_observation"])
        self.assert_idle()

    def test_buffered_bytes_after_body_are_rejected_without_retry(self):
        with self.assertRaises(http.client.RemoteDisconnected):
            self.call("GET", "/v1/config?extra=true")
        self.assertEqual(len(self.captured), 1)
        snapshot = self.audit.snapshot()
        self.assertFalse(snapshot["valid_observation"])
        self.assertEqual(snapshot["counters"]["completed"], 0)
        self.assert_idle()

    def test_missing_fin_expires_original_deadline_and_releases_transport(self):
        self.restart_observer({"request_absolute_deadline_seconds": 0.15})
        started = time.monotonic()
        with self.assertRaises(http.client.RemoteDisconnected):
            self.call("GET", "/v1/config?no-fin=true")
        self.assertLess(time.monotonic() - started, 0.8)
        self.assertTrue(self.body_sent.is_set())
        self.assertTrue(self.peer_closed.wait(1))
        self.assertEqual(len(self.captured), 1)
        self.assertFalse(self.audit.snapshot()["valid_observation"])
        self.assertEqual(self.audit.snapshot()["counters"]["completed"], 0)
        self.assert_idle()

    def restart_observer(self, overrides):
        self.observer.shutdown()
        self.observer.server_close()
        self.observer_thread.join()
        self.observer = ObserverServer(("127.0.0.1", 0), "http://127.0.0.1:" + str(self.backend.server_port),
                                       self.audit, bounds={**BOUNDS, **overrides})
        self.observer.reset("synthetic")
        self.observer_thread = threading.Thread(target=self.observer.serve_forever, kwargs={"poll_interval": 0.01})
        self.observer_thread.start()

    def test_response_cap_closes_without_substitute_status(self):
        self.restart_observer({"max_response_bytes": 8})
        for path in ("/v1/namespaces/ns/tables", "/v1/config?close-delimited=true"):
            with self.assertRaises(http.client.RemoteDisconnected):
                self.call("GET", path)
            self.assert_idle()
        self.assertEqual(len(self.captured), 2)
        self.assertGreater(self.audit.snapshot()["lifetime_failures"], 0)
        self.assertEqual(self.audit.snapshot()["counters"]["completed"], 0)

    def test_connect_errno_is_correlated_and_taints_without_response_or_retry(self):
        ledger = tempfile.TemporaryFile()
        self.addCleanup(ledger.close)
        self.audit.ledger = ledger
        private = "PRIVATE native URL Authorization password body"
        original_connect = http.client.HTTPConnection.connect
        attempts = []

        def connect(connection):
            if connection.port == self.backend.server_port:
                attempts.append(1)
                raise OSError(errno.EADDRNOTAVAIL, private)
            return original_connect(connection)

        with mock.patch.object(http.client.HTTPConnection, "connect", connect):
            with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2) as client:
                client.sendall(b"GET /v1/namespaces/ns/tables/PRIVATE_TARGET HTTP/1.1\r\n"
                               b"Host: 127.0.0.1\r\nAuthorization: PRIVATE_CREDENTIAL\r\n"
                               b"Content-Length: 0\r\nConnection: close\r\n\r\n")
                self.assertEqual(client.recv(65536), b"", "failure cannot synthesize an HTTP response")
        deadline = time.monotonic() + 2
        while self.audit.snapshot()["active_connections"]:
            need(time.monotonic() < deadline, "failed observer connection did not exit")
            time.sleep(0.001)
        snapshot = self.audit.snapshot()
        self.assertFalse(snapshot["valid_observation"])
        self.assertEqual(snapshot["counters"]["failures"], 1)
        self.assertEqual(snapshot["counters"]["upstream_requests_sent"], 0)
        self.assertEqual(snapshot["counters"]["completed"], 0)
        self.assertEqual(attempts, [1])
        self.assertFalse(self.captured)
        ledger.seek(0)
        raw = ledger.read()
        for value in (private, "PRIVATE_TARGET", "PRIVATE_CREDENTIAL"):
            self.assertNotIn(value.encode(), raw)
        events = [parse_json(line) for line in raw.splitlines()]
        failure = next(event for event in events if event["event"] == "failure")
        request = next(event for event in events if event["event"] == "request")
        self.assertEqual(failure["diagnostic"], {"operation_stage": "upstream-connect",
                                               "exception_category": "os-error",
                                               "errno": errno.EADDRNOTAVAIL,
                                               "request_sequence": request["sequence"]})
        self.assertEqual(request["outcome"], "incomplete")
        self.assertIsNone(request["status"])

    def test_absolute_deadline_closes_without_retry(self):
        self.restart_observer({"request_absolute_deadline_seconds": 0.05})
        started = time.monotonic()
        with self.assertRaises(http.client.RemoteDisconnected):
            self.call("GET", "/v1/namespaces/ns/tables/slow")
        self.assertLess(time.monotonic() - started, 0.5)
        self.assertEqual(len(self.captured), 1)
        self.assertGreater(self.audit.snapshot()["lifetime_failures"], 0)

    def test_connection_admission_and_reuse(self):
        self.restart_observer({"max_observers": 1})
        first = socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)
        try:
            deadline = time.monotonic() + 2
            while self.audit.snapshot()["active_connections"] != 1:
                need(time.monotonic() < deadline, "local admission not observed")
                time.sleep(0.001)
            self.reject_reset_busy()
            second = socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)
            try:
                self.assertEqual(second.recv(1), b"")
            finally:
                second.close()
            self.assertEqual(self.audit.snapshot()["counters"]["observer_connections_peak"], 1)
            self.assertEqual(self.audit.snapshot()["counters"]["admission_refusals"], 1)
        finally:
            first.shutdown(socket.SHUT_RDWR)
            first.close()
        deadline = time.monotonic() + 2
        while self.audit.snapshot()["active_connections"]:
            need(time.monotonic() < deadline, "local admission not released")
            time.sleep(0.001)
        self.assertEqual(self.call("GET", "/v1/namespaces/ns/tables")[0], 200)

    def reject_reset_busy(self):
        with self.assertRaises(Refusal):
            self.audit.reset("native_fe")

    def wait_for(self, predicate, reason):
        deadline = time.monotonic() + 2
        while not predicate():
            need(time.monotonic() < deadline, reason)
            time.sleep(0.001)

    def assert_quiescent(self, result):
        self.assertEqual((result["command"], result["state"]), ("quiesce", "QUIESCENT"))
        for key in ("active_connections", "active_requests", "active_upstream_requests", "active_upstream_listings"):
            self.assertEqual(result["audit"][key], 0)
        self.assertFalse(self.observer.pairs, "quiesce must join and retire every registered handler")

    def await_audit(self, predicate):
        deadline = time.monotonic() + 2
        while True:
            snapshot = self.audit.snapshot()
            if predicate(snapshot):
                return snapshot
            need(time.monotonic() < deadline, "local audit predicate did not become true")
            threading.Event().wait(0.001)


    def test_two_requests_reuse_both_sockets_and_preserve_phase_body_sha(self):
        import tempfile
        from contextlib import closing

        self.audit.ledger = self.enterContext(tempfile.TemporaryFile())
        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=1")
            first_socket = client.sock
            first = client.getresponse()
            self.assertEqual((first.status, first.read()), (200, self.literal))
            first.close()
            idle = self.await_audit(lambda a: a["active_http_requests"] == 0)
            self.assertEqual((idle["active_connections"], idle["active_upstream_connections"]), (1, 1))
            with self.assertRaises(Refusal):
                self.observer.reset("cannot_cross_live_pair")
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=2")
            self.assertIs(client.sock, first_socket)
            second = client.getresponse()
            self.assertEqual((second.status, second.read()), (200, self.literal))
            second.close()
            self.await_audit(lambda a: a["active_http_requests"] == 0)
            self.assertEqual(self.upstream_peers[0], self.upstream_peers[1])
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertEqual(final["active_upstream_connections"], 0)
        self.audit.ledger.seek(0)
        events = [parse_json(line) for line in self.audit.ledger.read().splitlines()]
        requests = [e for e in events if e["event"] == "request"]
        self.assertEqual([e["phase"] for e in requests], ["synthetic", "synthetic"])
        self.assertEqual([e["response_body_sha256"] for e in requests], [sha(self.literal)] * 2)
        self.assertEqual([e["request_body_sha256"] for e in requests], [sha(b"")] * 2)
        self.assertEqual([e["peer_port"] for e in requests], [requests[0]["peer_port"]] * 2)
        self.assertTrue(final["valid_observation"])


    def test_chunk_terminator_and_head_complete_before_socket_reuse(self):
        from contextlib import closing

        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            for method, suffix, expected in (
                ("GET", "?keepalive=1&chunked=1", self.literal),
                ("HEAD", "?keepalive=1", b""),
                ("GET", "?keepalive=1", self.literal),
            ):
                client.request(method, "/v1/namespaces/ns/tables" + suffix)
                response = client.getresponse()
                self.assertEqual((response.status, response.read()), (200, expected))
                response.close()
                self.await_audit(lambda a: a["active_http_requests"] == 0)
            self.assertEqual(len(self.upstream_peers), 3)
            self.assertEqual(len(set(self.upstream_peers)), 1)
        self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertTrue(self.audit.snapshot()["valid_observation"])


    def test_partial_headers_and_body_prevent_phase_reset(self):
        from contextlib import closing

        for prefix, suffix in (
            (b"GET /v1/namespaces/ns/tables HTTP/1.1\r\nHost: localhost\r\nX-Part:",
             b" complete\r\nConnection: close\r\n\r\n"),
            (b"POST /v1/namespaces/ns/error HTTP/1.1\r\nHost: localhost\r\n"
             b"Connection: close\r\nContent-Length: 2\r\n\r\nx", b"y"),
        ):
            with closing(socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)) as client:
                client.sendall(prefix)
                busy = self.await_audit(lambda a: a["active_http_requests"] == 1)
                self.assertEqual(busy["active_connections"], 1)
                with self.assertRaises(Refusal):
                    self.audit.reset("must_not_reset")
                client.sendall(suffix)
                response = http.client.HTTPResponse(client)
                response.begin()
                self.assertEqual(response.read(), self.literal)
                response.close()
            self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertTrue(self.audit.snapshot()["valid_observation"])


    def test_idle_has_no_rpc_timer_but_each_request_keeps_its_absolute_deadline(self):
        from contextlib import closing

        self.restart_observer({"request_absolute_deadline_seconds": 0.1})
        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            for index in range(2):
                client.request("GET", "/v1/namespaces/ns/tables?keepalive=1")
                response = client.getresponse()
                self.assertEqual(response.read(), self.literal)
                response.close()
                self.await_audit(lambda a: a["active_http_requests"] == 0)
                if index == 0:
                    # Exceeds the old connection watchdog, not the next RPC's
                    # original deadline; this is deliberately idle time.
                    threading.Event().wait(0.15)
            self.assertEqual(len(set(self.upstream_peers)), 1)
        self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertTrue(self.audit.snapshot()["valid_observation"])


    def test_stop_waits_for_actual_idle_handler_and_upstream_exit(self):
        from contextlib import closing

        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=1")
            response = client.getresponse()
            self.assertEqual(response.read(), self.literal)
            response.close()
            self.await_audit(lambda a: a["active_http_requests"] == 0)
            self.observer.shutdown()
            joined = threading.Event()
            def close_observer():
                self.observer.server_close()
                joined.set()
            closer = threading.Thread(target=close_observer)
            closer.start()
            try:
                # Explicit quiesce owns idle close and joins before returning.
                self.assertTrue(joined.wait(2))
            finally:
                client.close()
                closer.join(2)
            self.assertFalse(closer.is_alive())
            self.assertTrue(joined.is_set())
            self.assertTrue(all(self.audit.snapshot()[key] == 0 for key in (
                "active_connections", "active_upstream_connections", "active_http_requests",
                "active_requests", "active_upstream_requests", "active_upstream_listings")))


    def test_truncated_length_or_chunked_mutation_closes_without_retry(self):
        from contextlib import closing

        for shape in ("length", "chunked", "chunked_trailer"):
            with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
                client.request("POST", "/v1/namespaces/ns/error?truncated=1&" + shape, body=b"literal")
                with self.assertRaises(http.client.RemoteDisconnected):
                    client.getresponse()
            final = self.await_audit(lambda a: a["active_connections"] == 0)
            self.assertEqual(final["active_upstream_connections"], 0)
        self.assertEqual(len(self.captured), 3)
        self.assertEqual(self.audit.snapshot()["counters"]["upstream_mutations_sent"], 3)
        self.assertEqual(self.audit.snapshot()["counters"]["completed"], 0)
        self.assertFalse(self.audit.snapshot()["valid_observation"])


    def test_stale_persistent_upstream_does_not_reconnect_or_replay_mutation(self):
        from contextlib import closing

        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=1")
            response = client.getresponse()
            self.assertEqual(response.read(), self.literal)
            response.close()
            self.await_audit(lambda a: a["active_http_requests"] == 0)
            client.request("POST", "/v1/namespaces/ns/error?close_before_headers=1", body=b"literal")
            with self.assertRaises(http.client.RemoteDisconnected):
                client.getresponse()
            self.assertTrue(self.upstream_closed.wait(2))
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertEqual(len(self.upstream_peers), 2)
        self.assertEqual(len(set(self.upstream_peers)), 1)
        self.assertEqual([request[0] for request in self.captured], ["GET", "POST"])
        self.assertEqual(self.captured[1][2], b"literal")
        self.assertEqual(final["active_upstream_connections"], 0)
        self.assertEqual(final["counters"]["upstream_mutations_sent"], 1)
        self.assertEqual(final["counters"]["completed"], 1)
        self.assertFalse(final["valid_observation"])


    def test_upstream_connection_close_is_not_reused_or_retried(self):
        from contextlib import closing

        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("POST", "/v1/namespaces/ns/error", body=b"literal")
            response = client.getresponse()
            self.assertTrue(response.will_close)
            self.assertEqual((response.status, response.read()), (409, self.literal))
            response.close()
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertEqual(len(self.captured), 1)
        self.assertEqual(final["active_upstream_connections"], 0)
        self.assertEqual(final["counters"]["upstream_mutations_sent"], 1)
        self.assertTrue(final["valid_observation"])


    def test_two_hop_pair_reuse_quiesce_peer_eof_and_new_generation(self):
        client = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        self.addCleanup(client.close)
        retained_reader = None
        for _ in range(3):
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=true")
            response = client.getresponse()
            self.assertEqual((response.status, response.read()), (200, self.literal))
            response.close()
            self.wait_for(lambda: self.audit.snapshot()["active_requests"] == 0, "request did not finish")
            with self.audit.lock:
                self.assertEqual(len(self.observer.pairs), 1)
                pair = next(iter(self.observer.pairs.values()))
                if retained_reader is None:
                    retained_reader = pair.upstream_reader
                self.assertIs(pair.upstream_reader, retained_reader)
            self.assertEqual(self.audit.snapshot()["active_connections"], 1,
                             "an idle live pair still owns its admission")
        self.assertEqual(len(set(self.backend_peers)), 1, "upstream TCP connection must actually be reused")
        before = self.audit.snapshot()
        result = self.observer.quiesce()
        self.assert_quiescent(result)
        self.assertTrue(result["audit"]["valid_observation"])
        self.assertEqual(client.sock.recv(1), b"", "quiesce must close the real client transport")
        self.assertTrue(self.backend_eof.wait(2), "real upstream peer must observe EOF")
        self.assertTrue(retained_reader.closed)
        opened = self.observer.reset("next_phase")
        self.assertEqual(opened["state"], "OPEN")
        self.assertEqual(opened["previous"], result["audit"])
        self.assertEqual(opened["current"]["generation"], before["generation"] + 1)
        self.assertEqual(self.call("GET", "/v1/namespaces/ns/tables")[0], 200)

    def test_explicit_upstream_close_opens_only_for_the_next_original_rpc(self):
        client = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        self.addCleanup(client.close)
        for _ in range(2):
            client.request("GET", "/v1/namespaces/ns/tables")
            response = client.getresponse()
            self.assertEqual(response.read(), self.literal)
            response.close()
        self.assertEqual(len(self.captured), 2)
        self.assertEqual(len(set(self.backend_peers)), 2)
        result = self.observer.quiesce()
        self.assert_quiescent(result)
        self.assertEqual(result["audit"]["counters"]["requests"], 2)
        self.assertEqual(result["audit"]["counters"]["completed"], 2)
        self.assertTrue(result["audit"]["valid_observation"])

    def test_quiesce_busy_rpc_finishes_without_an_extended_deadline(self):
        client = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        self.addCleanup(client.close)
        client.request("GET", "/v1/namespaces/ns/tables?keepalive=true&slow=true")
        self.wait_for(lambda: len(self.captured) == 1, "upstream request did not start")
        result = []
        closer = threading.Thread(target=lambda: result.append(self.observer.quiesce()))
        closer.start()
        self.wait_for(lambda: self.observer.admission_state == "QUIESCING", "quiesce did not close admission")
        self.assertTrue(closer.is_alive(), "quiesce cannot report idle while RPC work is live")
        response = client.getresponse()
        self.assertEqual((response.status, response.read()), (200, self.literal))
        response.close()
        closer.join(2)
        self.assertFalse(closer.is_alive())
        self.assert_quiescent(result[0])
        self.assertTrue(result[0]["audit"]["valid_observation"])

    def test_quiesce_ack_late_connection_stays_tainted_and_cannot_reset(self):
        result = self.observer.quiesce()
        self.assert_quiescent(result)
        with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2) as client:
            self.assertEqual(client.recv(1), b"")
        self.wait_for(lambda: self.audit.snapshot()["lifetime_failures"] == 1, "late admission was not observed")
        with self.assertRaises(Refusal):
            self.observer.reset("forbidden_next_phase")
        after = self.observer.snapshot()
        self.assertEqual(after["state"], "QUIESCENT")
        self.assertEqual(after["audit"]["generation"], result["audit"]["generation"])
        self.assertFalse(after["audit"]["valid_observation"])

    def test_late_buffered_request_cannot_cross_quiesce_or_receive_a_fake_response(self):
        client = socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)
        self.addCleanup(client.close)
        client.sendall(b"GET /v1/namespaces/ns/tables?keepalive=true&slow=true HTTP/1.1\r\nHost: local\r\n\r\n")
        self.wait_for(lambda: len(self.captured) == 1, "busy request did not start")
        result = []
        closer = threading.Thread(target=lambda: result.append(self.observer.quiesce()))
        closer.start()
        self.wait_for(lambda: self.observer.admission_state == "QUIESCING", "quiesce admission cut missing")
        client.sendall(b"GET /v1/namespaces/ns/tables?late=true HTTP/1.1\r\nHost: local\r\n\r\n")
        chunks = []
        while True:
            data = client.recv(65536)
            if not data:
                break
            chunks.append(data)
        closer.join(2)
        self.assertFalse(closer.is_alive())
        self.assert_quiescent(result[0])
        self.assertEqual(b"".join(chunks).count(b"HTTP/1.1"), 1)
        self.assertEqual(len(self.captured), 1, "late request cannot reach the upstream")
        self.assertFalse(result[0]["audit"]["valid_observation"])
        with self.assertRaises(Refusal):
            self.observer.reset("forbidden_next_phase")

    def test_quiesce_tracks_accepted_socket_before_handler_setup(self):
        entered, proceed = threading.Event(), threading.Event()
        original = ObserverHandler.setup

        def setup(handler):
            entered.set()
            need(proceed.wait(2), "setup test owner was not released")
            original(handler)

        with mock.patch.object(ObserverHandler, "setup", setup):
            with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2):
                self.assertTrue(entered.wait(2))
                result = []
                closer = threading.Thread(target=lambda: result.append(self.observer.quiesce()))
                closer.start()
                self.wait_for(lambda: self.observer.admission_state == "QUIESCING", "quiesce cut missing")
                self.assertEqual(self.audit.snapshot()["active_connections"], 1)
                self.assertTrue(closer.is_alive())
                proceed.set()
                closer.join(2)
                self.assertFalse(closer.is_alive())
                self.assert_quiescent(result[0])
                self.assertTrue(result[0]["audit"]["valid_observation"])

    def test_quiesce_cannot_cross_actual_accept_before_pair_registration(self):
        accepted, register = threading.Event(), threading.Event()
        original = self.observer.get_request

        def get_request():
            request = original()
            accepted.set()
            need(register.wait(2), "accepted test socket was not registered")
            return request

        result, quiesce_started = [], threading.Event()

        def quiesce():
            quiesce_started.set()
            result.append(self.observer.quiesce())

        with mock.patch.object(self.observer, "get_request", get_request):
            with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2) as client:
                self.assertTrue(accepted.wait(2), "real accept did not reach its registration frontier")
                closer = threading.Thread(target=quiesce)
                closer.start()
                self.assertTrue(quiesce_started.wait(2))
                acquired = self.audit.lock.acquire(blocking=False)
                if acquired:
                    self.audit.lock.release()
                self.assertFalse(acquired, "real accept must retain the admission frontier lock")
                self.assertFalse(result, "quiesce cannot ACK beyond an unregistered accepted socket")
                self.assertTrue(closer.is_alive())
                register.set()
                closer.join(2)
                self.assertFalse(closer.is_alive())
                self.assertEqual(client.recv(1), b"")
                self.assert_quiescent(result[0])
                self.assertTrue(result[0]["audit"]["valid_observation"])
                self.assertEqual(self.observer.socket.gettimeout(), 0.0)
                opened = self.observer.reset("after_accepted_owner_join")
                self.assertEqual(opened["previous"], result[0]["audit"])

    def test_persistent_response_extra_bytes_survive_until_retirement_refusal(self):
        client = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        self.addCleanup(client.close)
        client.request("GET", "/v1/namespaces/ns/tables?keepalive=true&extra=true")
        response = client.getresponse()
        self.assertEqual(response.read(), self.literal)
        response.close()
        result = self.observer.quiesce()
        self.assert_quiescent(result)
        self.assertFalse(result["audit"]["valid_observation"], "unconsumed upstream bytes cannot disappear")
        self.assertEqual(len(self.captured), 1)
        with self.assertRaises(Refusal):
            self.observer.reset("forbidden_next_phase")

    def test_reset_rejects_kernel_queued_connection_before_listener_dispatch(self):
        before = self.observer.quiesce()
        self.assert_quiescent(before)
        dispatch_pending, dispatch_allowed = threading.Event(), threading.Event()
        original = self.observer._handle_request_noblock

        def dispatch():
            if threading.current_thread() is self.observer_thread:
                dispatch_pending.set()
                need(dispatch_allowed.wait(2), "test listener dispatch was not released")
            original()

        with mock.patch.object(self.observer, "_handle_request_noblock", dispatch):
            try:
                with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2) as client:
                    self.assertTrue(dispatch_pending.wait(2), "real queued socket did not wake the listener")
                    self.assertFalse(self.observer.pairs, "listener must not have accepted this socket yet")
                    with self.assertRaises(Refusal):
                        self.observer.reset("forbidden_queued_phase")
                    self.assertEqual(client.recv(1), b"", "old-generation late socket must actually close")
                    after = self.observer.snapshot()
                    self.assertEqual(after["state"], "QUIESCENT")
                    self.assertEqual(after["audit"]["generation"], before["audit"]["generation"])
                    self.assertEqual(after["audit"]["counters"]["failures"], 1)
                    self.assertEqual(after["audit"]["lifetime_failures"], 1)
                    self.assertEqual(after["audit"]["counters"]["requests"], 0)
                    self.assertFalse(after["audit"]["valid_observation"])
            finally:
                dispatch_allowed.set()

    def test_completed_response_idle_reset_retires_actual_pair_without_taint(self):
        ledger = tempfile.TemporaryFile()
        self.addCleanup(ledger.close)
        self.audit.ledger = ledger
        client = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        self.addCleanup(client.close)
        client.request("GET", "/v1/namespaces/ns/tables?keepalive=true")
        response = client.getresponse()
        self.assertEqual((response.status, response.read()), (200, self.literal))
        response.close()
        self.wait_for(lambda: self.audit.snapshot()["counters"]["completed"] == 1
                      and self.audit.snapshot()["active_requests"] == 0,
                      "original response did not complete before the idle reset")
        with self.audit.lock:
            pair = next(iter(self.observer.pairs.values()))
            client_reader, upstream_reader = pair.client_reader, pair.upstream_reader
            self.assertEqual((self.audit.active, self.audit.upstream_active, self.audit.list_active), (0, 0, 0))
        client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        client.close()
        self.wait_for(lambda: self.audit.snapshot()["active_connections"] == 0,
                      "reset idle pair did not physically exit")
        result = self.observer.quiesce()
        self.assert_quiescent(result)
        self.assertTrue(client_reader.closed)
        self.assertTrue(upstream_reader.closed)
        self.assertTrue(self.backend_eof.wait(2), "upstream peer did not observe actual pair exit")
        self.assertTrue(result["audit"]["valid_observation"])
        for key in ("requests", "upstream_requests_sent", "completed"):
            self.assertEqual(result["audit"]["counters"][key], 1)
        self.assertEqual(len(self.captured), 1)
        ledger.seek(0)
        events = [parse_json(line) for line in ledger.read().splitlines()]
        resets = [event for event in events if event["event"] == "idle-peer-reset"]
        self.assertEqual(resets, [{"event": "idle-peer-reset", "phase": "synthetic", "generation": 2,
                                  "diagnostic": {"operation_stage": "idle-client-read",
                                                 "exception_category": "os-error", "errno": errno.ECONNRESET,
                                                 "request_sequence": None}}])
        self.assertFalse(any(event["event"] == "failure" for event in events))

    def test_active_request_client_reset_still_taints_without_retry(self):
        client = socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)
        self.addCleanup(client.close)
        client.sendall(b"GET /v1/namespaces/ns/tables?keepalive=true&slow=true HTTP/1.1\r\nHost: local\r\n\r\n")
        self.wait_for(lambda: len(self.captured) == 1 and self.audit.snapshot()["active_requests"] == 1,
                      "original request did not enter its active upstream call")
        client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        client.close()
        self.wait_for(lambda: self.audit.snapshot()["active_connections"] == 0,
                      "reset active request did not physically exit")
        result = self.observer.quiesce()
        self.assert_quiescent(result)
        self.assertFalse(result["audit"]["valid_observation"])
        self.assertEqual(result["audit"]["counters"]["completed"], 0)
        self.assertEqual(result["audit"]["counters"]["requests"], 1)
        self.assertEqual(result["audit"]["counters"]["upstream_requests_sent"], 1)
        self.assertEqual(len(self.captured), 1)

    def test_idle_pair_has_no_rpc_timer_and_quiesce_retires_it_without_fake_rpc(self):
        self.restart_observer({"request_absolute_deadline_seconds": 0.05})
        with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2) as client:
            self.wait_for(lambda: self.audit.snapshot()["active_connections"] == 1, "idle owner not admitted")
            time.sleep(0.1)
            self.assertEqual(self.audit.snapshot()["active_connections"], 1)
            self.assertEqual(self.audit.snapshot()["active_http_requests"], 0)
            self.assert_quiescent(self.observer.quiesce())
            self.assertEqual(client.recv(1), b"")
        self.assertEqual(self.audit.snapshot()["counters"]["requests"], 0)
        self.assertTrue(self.audit.snapshot()["valid_observation"])

    def test_request_deadline_starts_at_first_byte_instead_of_prior_idle_time(self):
        self.restart_observer({"request_absolute_deadline_seconds": 0.3})
        client = http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)
        self.addCleanup(client.close)
        client.connect()
        self.wait_for(lambda: self.audit.snapshot()["active_connections"] == 1, "idle connection not admitted")
        time.sleep(0.2)
        client.request("GET", "/v1/namespaces/ns/tables?keepalive=true&slow=true")
        response = client.getresponse()
        self.assertEqual(response.read(), self.literal)
        response.close()
        result = self.observer.quiesce()
        self.assert_quiescent(result)
        self.assertTrue(result["audit"]["valid_observation"])

    def test_connection_position_is_not_reused_before_actual_handler_thread_exit(self):
        self.restart_observer({"max_observers": 1})
        exited_io, allow_thread_exit = threading.Event(), threading.Event()
        original = ObserverServer.process_request_thread

        def worker(server, request, address):
            original(server, request, address)
            exited_io.set()
            need(allow_thread_exit.wait(2), "test handler exit was not released")

        with mock.patch.object(ObserverServer, "process_request_thread", worker):
            with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2) as first:
                self.wait_for(lambda: self.audit.snapshot()["active_connections"] == 1, "first owner missing")
                first.shutdown(socket.SHUT_RDWR)
            self.assertTrue(exited_io.wait(2))
            self.assertEqual(self.audit.snapshot()["active_connections"], 0)
            self.assertTrue(any(pair.thread.is_alive() for pair in self.observer.pairs.values()))
            with socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2) as second:
                self.assertEqual(second.recv(1), b"")
            self.assertEqual(self.audit.snapshot()["counters"]["admission_refusals"], 1)
            allow_thread_exit.set()
            self.wait_for(lambda: all(not pair.thread.is_alive() for pair in self.observer.pairs.values()),
                          "original handler thread did not exit")
        self.assertEqual(self.call("GET", "/v1/namespaces/ns/tables")[0], 200)


    def test_idle_upstream_eof_retires_pair_without_rpc_or_reconnect(self):
        from contextlib import closing

        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=1&silent_close=1")
            response = client.getresponse()
            self.assertEqual(response.read(), self.literal)
            response.close()
            self.assertTrue(self.upstream_closed.wait(2))
            # No next RPC wakes the observer: the same owner must expose the
            # original upstream EOF and actually release both idle sockets.
            final = self.await_audit(lambda a: a["active_connections"] == 0)
            self.assertEqual(client.sock.recv(1), b"")
        self.assertEqual(len(self.upstream_peers), 1)
        self.assertEqual([request[0] for request in self.captured], ["GET"])
        self.assertEqual(final["active_upstream_connections"], 0)
        self.assertEqual(final["counters"]["requests"], 1)
        self.assertEqual(final["counters"]["completed"], 1)
        self.assertEqual(final["counters"]["failures"], 0)
        self.assertTrue(final["valid_observation"])


    def test_buffered_next_request_keeps_both_messages_and_upstream_reuse(self):
        from contextlib import closing

        with closing(socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)) as client:
            # Both messages may be prefetched by the existing BufferedReader;
            # raw client-fd readiness is not the parser's only source of work.
            client.sendall(
                b"GET /v1/namespaces/ns/tables?keepalive=one HTTP/1.1\r\nHost: localhost\r\n\r\n"
                b"GET /v1/namespaces/ns/tables?keepalive=two HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            received = bytearray()
            while chunk := client.recv(4096):
                received.extend(chunk)
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertEqual(received.count(b"HTTP/1.1 200 Literal reason\r\n"), 2)
        self.assertEqual(received.count(self.literal), 2)
        self.assertEqual(len(self.captured), 2)
        self.assertEqual(len(set(self.upstream_peers)), 1)
        self.assertEqual(final["counters"]["completed"], 2)
        self.assertTrue(final["valid_observation"])


    def test_pending_mutation_and_upstream_close_preserve_failure_without_replay(self):
        from contextlib import closing

        with closing(socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)) as client:
            # EOF and the next buffered request can become ready together.
            # Either pre-dispatch refusal or the original dispatch-time EOF is
            # a failure, never an idle-success reclassification or replay.
            client.sendall(
                b"GET /v1/namespaces/ns/tables?keepalive=1&silent_close=1 HTTP/1.1\r\nHost: localhost\r\n\r\n"
                b"POST /v1/namespaces/ns/error HTTP/1.1\r\nHost: localhost\r\nContent-Length: 7\r\n\r\nliteral")
            received = bytearray()
            while chunk := client.recv(4096):
                received.extend(chunk)
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertEqual(received.count(b"HTTP/1.1 200 Literal reason\r\n"), 1)
        self.assertEqual(received.count(self.literal), 1)
        self.assertEqual([request[0] for request in self.captured], ["GET"])
        self.assertEqual(len(self.upstream_peers), 1)
        self.assertLessEqual(final["counters"]["upstream_mutations_sent"], 1)
        self.assertEqual(final["counters"]["completed"], 1)
        self.assertGreater(final["counters"]["failures"], 0)
        self.assertFalse(final["valid_observation"])


    def test_idle_client_reset_retires_settled_pair_without_new_rpc(self):
        import errno
        import struct
        import tempfile
        from contextlib import closing

        self.audit.ledger = self.enterContext(tempfile.TemporaryFile())
        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=1")
            response = client.getresponse()
            self.assertEqual(response.read(), self.literal)
            response.close()
            self.await_audit(lambda a: a["active_http_requests"] == 0)
            client.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            client.close()
            final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertEqual(final["counters"]["requests"], 1)
        self.assertEqual(final["counters"]["completed"], 1)
        self.assertEqual(final["counters"]["failures"], 0)
        self.assertEqual(final["active_upstream_connections"], 0)
        self.assertEqual(len(self.captured), 1)
        self.assertEqual(len(self.upstream_peers), 1)
        self.assertTrue(final["valid_observation"])
        self.audit.ledger.seek(0)
        events = [parse_json(line) for line in self.audit.ledger.read().splitlines()]
        reset = [event for event in events if event["event"] == "idle-pair-reset"]
        self.assertEqual(len(reset), 1)
        self.assertEqual(reset[0]["side"], "client")
        self.assertIn(reset[0]["probe"], {"buffered_client_peek", "raw_client_peek"})
        self.assertEqual(reset[0]["errno"], errno.ECONNRESET)
        self.assertTrue(reset[0]["previous_exchange_complete"])
        self.assertFalse(reset[0]["first_byte_observed"])


    def test_idle_upstream_reset_retires_settled_pair_without_new_rpc(self):
        import errno
        import tempfile
        from contextlib import closing

        self.audit.ledger = self.enterContext(tempfile.TemporaryFile())
        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=1&idle_reset=1")
            response = client.getresponse()
            self.assertEqual(response.read(), self.literal)
            response.close()
            self.await_audit(lambda a: a["active_http_requests"] == 0)
            self.allow_idle_reset.set()
            self.assertTrue(self.upstream_closed.wait(2))
            final = self.await_audit(lambda a: a["active_connections"] == 0)
            self.assertEqual(client.sock.recv(1), b"")
        self.assertEqual(final["counters"]["requests"], 1)
        self.assertEqual(final["counters"]["completed"], 1)
        self.assertEqual(final["counters"]["failures"], 0)
        self.assertEqual(final["active_upstream_connections"], 0)
        self.assertEqual(len(self.captured), 1)
        self.assertTrue(final["valid_observation"])
        self.audit.ledger.seek(0)
        events = [parse_json(line) for line in self.audit.ledger.read().splitlines()]
        reset = [event for event in events if event["event"] == "idle-pair-reset"]
        self.assertEqual(len(reset), 1)
        self.assertEqual((reset[0]["side"], reset[0]["probe"], reset[0]["errno"]),
                         ("upstream", "buffered_upstream_peek", errno.ECONNRESET))


    def test_partial_request_reset_keeps_request_failure_and_original_deadline(self):
        import struct
        import tempfile

        self.audit.ledger = self.enterContext(tempfile.TemporaryFile())
        client = socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)
        try:
            client.sendall(b"GET /v1/namespaces/ns/tables HTTP/1.1\r\nHost:")
            self.await_audit(lambda a: a["active_http_requests"] == 1)
            client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        finally:
            client.close()
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertFalse(final["valid_observation"])
        self.assertEqual(final["counters"]["completed"], 0)
        self.assertEqual(len(self.captured), 0)
        self.audit.ledger.seek(0)
        events = [parse_json(line) for line in self.audit.ledger.read().splitlines()]
        failures = [event for event in events if event["event"] == "failure"]
        self.assertEqual(len(failures), 1)
        self.assertTrue(failures[0]["request_held"])
        self.assertEqual(failures[0]["error"]["operation"], "parse_client_request")
        self.assertFalse(any(event["event"] == "idle-pair-reset" for event in events))


    def test_pending_request_and_upstream_reset_keep_failure_without_replay(self):
        from contextlib import closing

        with closing(socket.create_connection(("127.0.0.1", self.observer.server_port), timeout=2)) as client:
            client.sendall(b"GET /v1/namespaces/ns/tables?keepalive=1&idle_reset=1 HTTP/1.1\r\nHost: localhost\r\n\r\n")
            response = http.client.HTTPResponse(client)
            response.begin()
            self.assertEqual(response.read(), self.literal)
            response.close()
            self.await_audit(lambda a: a["active_http_requests"] == 0)
            client.sendall(b"POST /v1/namespaces/ns/error HTTP/1.1\r\nHost:")
            self.await_audit(lambda a: a["active_http_requests"] == 1)
            self.allow_idle_reset.set()
            self.assertTrue(self.upstream_closed.wait(2))
            client.sendall(b" localhost\r\nContent-Length: 7\r\n\r\nliteral")
            self.assertEqual(client.recv(4096), b"")
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertFalse(final["valid_observation"])
        self.assertEqual(final["counters"]["completed"], 1)
        self.assertEqual([request[0] for request in self.captured], ["GET"])
        self.assertEqual(len(self.upstream_peers), 1)
        self.assertLessEqual(final["counters"]["upstream_mutations_sent"], 1)
        self.assertGreater(final["counters"]["failures"], 0)


    def test_dispatched_mutation_reset_before_headers_keeps_failure_and_single_send(self):
        from contextlib import closing

        with closing(http.client.HTTPConnection("127.0.0.1", self.observer.server_port, timeout=2)) as client:
            client.request("GET", "/v1/namespaces/ns/tables?keepalive=1")
            response = client.getresponse()
            self.assertEqual(response.read(), self.literal)
            response.close()
            self.await_audit(lambda a: a["active_http_requests"] == 0)
            client.request("POST", "/v1/namespaces/ns/error?reset_before_headers=1", body=b"literal")
            with self.assertRaises(http.client.RemoteDisconnected):
                client.getresponse()
        final = self.await_audit(lambda a: a["active_connections"] == 0)
        self.assertEqual([request[0] for request in self.captured], ["GET", "POST"])
        self.assertEqual(self.captured[1][2], b"literal")
        self.assertEqual(len(set(self.upstream_peers)), 1)
        self.assertEqual(final["counters"]["upstream_mutations_sent"], 1)
        self.assertEqual(final["counters"]["completed"], 1)
        self.assertFalse(final["valid_observation"])



def stdout(value):
    print(encode(value).decode(), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--freeze", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.self_test:
        need(not args.freeze and not args.output, "self-test cannot accept actual service input")
        suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(case)
                                   for case in (PureTests, LoopbackTests))
        passed = unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful()
        print("Pure parser and synthetic loopback transparency only; no provider/native acceptance.")
        return 0 if passed else 1
    need(args.freeze and args.output, "explicit frozen input and new output directory required")
    freeze, freeze_sha, binding, source = validate_freeze(args.freeze)
    args.output.mkdir(mode=0o700, parents=False, exist_ok=False)
    ledger = (args.output / "observer-audit.jsonl").open("xb")
    audit = Audit(ledger)
    server = ObserverServer(("127.0.0.1", 0), binding["rest_uri"], audit)
    receipt = {"schema_version": 1, "state": "OBSERVER_BOUND", "pid": os.getpid(),
               "started_unix_ns": time.time_ns(), "observer_freeze_sha256": freeze_sha,
               "observer_sha256": freeze["observer_sha256"],
               "observer_uri": "http://127.0.0.1:" + str(server.server_port),
               "admission_state": "QUIESCENT",
               "downstream_binding": binding, "source": source, "bounds": BOUNDS,
               "scope": "test-only observer process; exclude from FE allocator samples"}
    with (args.output / "OBSERVER_BOUND.json").open("xb") as handle:
        handle.write(encode(receipt) + b"\n")
        handle.flush()
        os.fsync(handle.fileno())
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.1})
    thread.start()
    stdout(receipt)
    try:
        while True:
            line = sys.stdin.buffer.readline(65537)
            if not line:
                break
            try:
                need(len(line) <= 65536 and line.endswith(b"\n"), "control input exceeds cap")
                command = parse_json(line)
                need(type(command) is dict, "invalid control command")
                if command == {"command": "snapshot"}:
                    stdout(server.snapshot())
                elif command == {"command": "quiesce"}:
                    stdout(server.quiesce())
                elif command == {"command": "stop"}:
                    break
                elif set(command) == {"command", "phase"} and command["command"] == "reset":
                    stdout(server.reset(command["phase"]))
                else:
                    raise Refusal("unknown control command")
            except Refusal as error:
                stdout({"control_refused": str(error)})
    finally:
        server.quiesce()
        server.shutdown()
        server.server_close()
        thread.join()
        stdout({"state": "OBSERVER_STOPPED", "audit": audit.snapshot()})
        ledger.close()
    return 0 if audit.snapshot()["lifetime_failures"] == 0 else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (Refusal, OSError, ValueError, KeyError, TypeError):
        # Exceptions may contain private paths/config data: fixed output only.
        print("REFUSED: observer input, local IO or lifecycle failure", file=sys.stderr)
        sys.exit(1)
