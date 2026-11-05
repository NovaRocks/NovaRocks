# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
"""Independent final-verifier draft for the single held-response late-ACK case.

No CLI or process/RPC execution. Inputs are original source/build/binary admission,
LIVE independent four-role births and descriptors, the before-first-role prepared
artifact, and original durable-log prefixes. The source-pinned runner path plus
actual original wait/role exit remains mandatory. Context census is not last-alias
or allocator-deallocation proof. This draft has not run and mints no Native READY.
"""
from __future__ import annotations

# Companion only: no CLI, executable launch, RPC, formatter, mutation or shared
# CASES patch. The old ten-case verifier and immutable inputs remain unchanged.
import errno
import os
import re
import uuid
from dataclasses import dataclass
import verify_exact_native_final as old

CASE = "result-delivery/held-response-late-ack"
INPUT_SHA = "5c8975f987c9ece83cbae75c582e91535439d763557d0d5285060dacbb827812"
LARGE_SHA = "d4bbd8c0cd3c3d647c6a0c948692db381360406584f25e3f6653fc91b73feff2"
TINY_SHA = "a250875085fd54bc3c35e0c1e7a08173d2f36920bc7dc6ab83ba6dbf2745d127"
ROW_SHA = "e5a05e54f4636fe6e87eb8094fceda8e002bff799f3de76413a2a77c52fd50b8"
HEALTH_SHA = "6dab454b19ecc06d337eb421a7ecc69aaa272a74fdf3a6bf7b81900ca94758b1"
ROOT_FIELDS = frozenset(("channels", "terminal_task_records", "producers_running",
    "producers_exited", "ends_published", "ends_acknowledged", "sealed",
    "data_positions", "payload_bytes", "segments", "deliveries",
    "retained_reservations", "metadata_holders", "metadata_bytes"))
RECEIPT_FIELDS = frozenset(("schema_version", "status", "native_acceptance", "source_pins",
    "original_roles", "selected_root", "independent_target_source", "closed_acks",
    "protocol_complete", "actor", "actor_actual_join", "mysql_actual_join", "kill_attempted",
    "kill_returned", "timings_origin", "protocol_started_us", "kill_started_us", "kill_returned_us",
    "settled_us", "expected_server_error_actual_source_retained", "expected_connection_joinerror_retained",
    "root_samples", "original_wire", "same_socket_health", "failed_source_slots",
    "raw_error_formatter_invoked", "scope", "mysql_terminal_code", "health_before_tasks_created", "health_after_tasks_created"))
TASK_FIELDS = frozenset(("query_hi", "query_lo", "attempt", "stage", "task", "backend"))
CREATE = "NOVAROCKS_TASK_CREATE_APPLIED"
CONTEXT = "NOVAROCKS_PARTICIPATION_ESTABLISH_APPLIED"
ROOT = "NOVAROCKS_TASK_PREPARED_CLIENT_ROOT"
require = old.require

@dataclass(frozen=True, slots=True, repr=False)
class HeldExpected:
    # All provenance/live roles/effective hashes are independently admitted.
    # roles/effective must come from original stdout+birth and the immutable
    # before-first-role prepared artifact, never either receipt being verified.
    case: str
    provenance: dict
    server_build_commit: str
    runner_build_commit: str
    runner_build_identity: str
    cargo_lock_sha256: str
    effective_launch_config_sha256: str
    roles: tuple[old.LaunchIdentity, ...]
    server_path: str
    runner_path: str
    base_config_path: str
    runtime_dir: str
    evidence_path: str
    original_execution_binding_sha256: str
    held_input_sha256: str
    def __repr__(self): return "HeldExpected(independent inputs omitted)"

@dataclass(frozen=True, slots=True, repr=False)
class HeldIndependentMaterials:
    # Descriptors are independently captured LIVE authenticated original BE
    # descriptors. FE line is from the original durable log/live owner, not RPC.
    # Prefixes are independent original-log bytes of exact recorded prefix size;
    # they may be read from retained logs after the runner exits, but their file
    # identities must be the same originals pinned at actual role launch.
    common: old.ActualMaterials
    neutral_build_stdout: bytes
    neutral_build_actual_wait_zero: bool
    held_input_bytes: bytes
    held_binding_bytes: bytes
    original_binding_bytes: bytes
    frontend_marker_line: bytes
    actual_backend_descriptors: tuple[str, str, str]
    original_backend_log_prefixes: tuple[bytes, bytes, bytes]
    def __repr__(self): return "HeldIndependentMaterials(raw source objects omitted)"

def _uuid(value):
    require(type(value) is str and len(value)==36, "process_uuid")
    try: parsed=uuid.UUID(value)
    except (ValueError, AttributeError): raise old.VerificationFailure("process_uuid") from None
    require(str(parsed)==value and parsed.version==7, "process_uuid")
    return value

def _execution(value):
    for key in ("query_hi", "query_lo"):
        require(type(value[key]) is int and -(1<<63)<=value[key]<(1<<63), "query_bits")
    require(value["query_hi"]!=0 or value["query_lo"]!=0,"query_identity_zero")
    require(old._uint(value["attempt"]) and value["attempt"]>0,"attempt_position")
    return {key:value[key] for key in ("query_hi","query_lo","attempt")}

def _task(value):
    require(type(value) is dict and set(value)==TASK_FIELDS, "task_fields")
    _execution(value)
    for key in ("stage", "task"):
        require(old._uint(value[key], (1<<32)-1) and value[key]>0, "task_position")
    _uuid(value["backend"])
    return value

def _hex_array(value):
    require(type(value) is list and len(value)==32
        and all(old._uint(n,255) for n in value), "digest_scalar_array")
    return bytes(value).hex()

def _target(expected, actual, target):
    keys={"schema_version", "source", "frontend", "root", "root_backend_index",
        "backend_process_from_descriptor", "actual_backend_descriptors", "original_roles", "contexts",
        "fresh_tasks", "marker_counts", "baseline_log_bytes", "baseline_sha256", "after_log_bytes", "after_sha256"}
    require(type(target) is dict and set(target)==keys and target["schema_version"]==1
        and type(target["schema_version"]) is int and target["source"]=="RootObservation", "target_fields")
    line=actual.frontend_marker_line
    require(type(line) is bytes and len(line)<=384, "frontend_marker_bound")
    match=re.fullmatch(rb"NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_FE frontend_process_id=([0-9a-f-]{36})\n",line)
    require(match is not None, "frontend_source_marker")
    frontend=_uuid(match[1].decode("ascii"))
    require(target["frontend"]==frontend, "frontend_independent_mismatch")
    require(type(actual.actual_backend_descriptors) is tuple and len(actual.actual_backend_descriptors)==3,
        "descriptor_inventory")
    descriptors=tuple(_uuid(v) for v in actual.actual_backend_descriptors)
    require(len(set(descriptors))==3 and target["actual_backend_descriptors"]==list(descriptors), "descriptor_inventory")
    root=_task(target["root"]); slot=target["root_backend_index"]
    require(old._uint(slot,2) and root["backend"]==descriptors[slot]
        and target["backend_process_from_descriptor"]==descriptors[slot], "root_descriptor")
    require(old.parse_roles(target["original_roles"])==expected.roles, "target_original_roles")
    arrays=("contexts", "marker_counts", "baseline_log_bytes", "baseline_sha256", "after_log_bytes", "after_sha256")
    require(all(type(target[k]) is list and len(target[k])==3 for k in arrays), "target_arrays")
    require(type(actual.original_backend_log_prefixes) is tuple and len(actual.original_backend_log_prefixes)==3,
        "original_logs_inventory")
    tasks=[]; roots=[]; contexts=[None,None,None]
    for backend,raw in enumerate(actual.original_backend_log_prefixes):
        base=target["baseline_log_bytes"][backend]; after=target["after_log_bytes"][backend]
        require(old._uint(base,2097152) and old._uint(after,2097152) and base<=after
            and type(raw) is bytes and len(raw)==after, "original_log_prefix_bound")
        require(old._digest(raw[:base])==_hex_array(target["baseline_sha256"][backend])
            and old._digest(raw)==_hex_array(target["after_sha256"][backend]), "original_log_anchor")
        require((base==0 or raw[base-1:base]==b"\n") and (not raw or raw.endswith(b"\n")), "original_log_line_boundary")
        try:
            raw.decode("utf-8","strict")  # The original scanner checks the whole source.
            # Only LF ends a source line. VT/CR/NEL/Unicode separators are data,
            # so an embedded marker cannot acquire a synthetic line boundary.
            lines=[line.decode("utf-8","strict") for line in raw[base:].split(b"\n")[:-1]]
        except UnicodeError: raise old.VerificationFailure("original_log_encoding") from None
        count=0
        for line in lines:
            names=[name for name in (CREATE,CONTEXT,ROOT) if name in line]
            if not names: continue
            require(len(names)==1 and len(line.encode("utf-8"))<=384 and line.startswith(names[0]+" "), "original_marker_boundary")
            count+=1; require(count<=8, "original_marker_positions")
            name=names[0]
            if name in (CREATE,ROOT):
                match=re.fullmatch(re.escape(name)+r" execution_id=(0|-?[1-9][0-9]*):(0|-?[1-9][0-9]*):([1-9][0-9]*) stage=([1-9][0-9]*) task=([1-9][0-9]*) backend=([0-9a-f-]{36})",line)
                require(match is not None, "original_task_marker")
                hi,lo,attempt,stage,task=map(int,match.groups()[:5])
                item=_task(dict(query_hi=hi,query_lo=lo,attempt=attempt,stage=stage,task=task,backend=match[6]))
                require(item["backend"]==descriptors[backend], "marker_backend_mismatch")
                if name==CREATE: tasks.append(dict(backend_index=backend,identity=item))
                else: roots.append((backend,item))
            else:
                match=re.fullmatch(re.escape(CONTEXT)+r" execution_id=(0|-?[1-9][0-9]*):(0|-?[1-9][0-9]*):([1-9][0-9]*) frontend=([0-9a-f-]{36}) backend=([0-9a-f-]{36})",line)
                require(match is not None and contexts[backend] is None, "original_context_marker")
                hi,lo,attempt=map(int,match.groups()[:3]); fe,be=match.groups()[3:]
                context_execution=_execution(dict(query_hi=hi,query_lo=lo,attempt=attempt))
                require(old._json_equal(context_execution,_execution(root)), "context_execution_mismatch")
                require(fe==frontend and be==descriptors[backend], "context_descriptor_mismatch")
                contexts[backend]=dict(query_hi=hi,query_lo=lo,attempt=attempt,frontend=fe,backend=be)
        require(target["marker_counts"][backend]==count and type(target["marker_counts"][backend]) is int,
            "original_marker_count")
    require(len(roots)==1 and roots[0]==(slot,root), "independent_unique_root")
    require(len(tasks)>0 and len(tasks)<=24 and len({(t["backend_index"],tuple(t["identity"].values())) for t in tasks})==len(tasks), "fresh_task_positions")
    # Fixed primitive tuples only; no arbitrary source object formatter is called.
    require(all(all(t["identity"][k]==root[k] for k in ("query_hi","query_lo","attempt")) for t in tasks), "fresh_query_mismatch")
    require(dict(backend_index=slot,identity=root) in tasks, "root_missing_create")
    require(old._json_equal(target["fresh_tasks"],tasks) and old._json_equal(target["contexts"],contexts), "target_inventory_mismatch")
    require(all((contexts[i] is not None)==any(t["backend_index"]==i for t in tasks) for i in range(3)), "context_membership")
    return root,slot

SAMPLE_PHASES=frozenset(("W2","W2_repeat","quiet_before_replay","held_open",
    "sealed_held","ACK1_first","ACK1_repeat"))

def _sample_dto(sample):
    require(type(sample) is dict and set(sample)=={"phase","roots","tasks_created","observed_from_original_prelaunch_us"}, "census_fields")
    require(type(sample["phase"]) is str and sample["phase"] in SAMPLE_PHASES, "census_phase")
    require(old._uint(sample["observed_from_original_prelaunch_us"],20000000), "census_timestamp")
    require(type(sample["roots"]) is list and len(sample["roots"])==3, "census_inventory")
    require(type(sample["tasks_created"]) is list and len(sample["tasks_created"])==3
        and all(old._uint(n) for n in sample["tasks_created"]),"census_task_counts")
    for root in sample["roots"]:
        require(type(root) is dict and set(root)==ROOT_FIELDS and all(old._uint(n) for n in root.values()), "census_values")

def _samples_dto(samples):
    require(type(samples) is list and 0<len(samples)<=51, "root_sample_positions")
    for sample in samples: _sample_dto(sample)

def _root_sample(sample,slot,sealed,holder):
    _sample_dto(sample)
    if any(any(n!=0 for n in root.values()) for i,root in enumerate(sample["roots"]) if i!=slot): return False
    root=sample["roots"][slot]
    common=root["channels"]==1 and root["terminal_task_records"]==1 and root["producers_running"]==0 and root["producers_exited"]==1 and root["ends_published"]==1 and root["ends_acknowledged"]==0 and root["sealed"]==int(sealed)
    # Native DATA retains a full encoded copy, independent of Worker segments.
    geometry=(root["data_positions"],root["payload_bytes"],root["segments"])==((0,0,0) if sealed else (2,1048584,2))
    holds=all(root[k]>0 for k in ("deliveries","retained_reservations","metadata_holders","metadata_bytes")) if holder else root["deliveries"]==root["retained_reservations"]==root["metadata_holders"]==0
    return common and geometry and holds

def _wire_dto(row):
    require(type(row) is dict and set(row)=={"rows","row_payload_bytes","wire_bytes","packets","columns","schema",
        "metadata_sha256","wire_prefix_sha256","first_payload_chunk_micros","first_row_micros","elapsed_micros","row_sha256","error"},"mysql_wire_fields")
    require(all(old._uint(row[k]) for k in ("rows","row_payload_bytes","wire_bytes","packets","columns")), "mysql_wire_scalars")
    require(all(old._hex(row[k]) for k in ("metadata_sha256","wire_prefix_sha256","row_sha256")), "mysql_wire_hashes")
    require(old._uint(row["elapsed_micros"],(1<<128)-1)
        and all(row[k] is None or old._uint(row[k],(1<<128)-1)
            for k in ("first_payload_chunk_micros","first_row_micros")), "mysql_wire_timestamps")
    require(row["error"] is None or old._text(row["error"]), "mysql_wire_error")
    schema=row["schema"]
    require(type(schema) is list and len(schema)==1, "mysql_schema_inventory")
    require(type(schema[0]) is dict and set(schema[0])=={"name","mysql_type"}
        and old._text(schema[0]["name"]) and old._uint(schema[0]["mysql_type"],255), "mysql_schema_fields")

def _wire(row,health):
    _wire_dto(row)
    require(all(type(row[k]) is int and row[k]==v for k,v in (("columns",1),("rows",1),("packets",5))),"mysql_cardinality")
    require(old._json_equal(row["schema"],[{"name":"total" if health else "payload","mysql_type":8 if health else 253}]), "mysql_schema")
    require(type(row["row_payload_bytes"]) is int and row["row_payload_bytes"]==(5 if health else 1048580)
        and row["row_sha256"]==(HEALTH_SHA if health else ROW_SHA), "mysql_independent_row_oracle")
    require(row["error"] is None if health else type(row["error"]) is str and row["error"]=="original result failure; actual source retained", "mysql_terminal")

ACTOR_FIELDS={"phase","first_failure","backend_index","actual_grpc_port","proven_offered_sequence",
        "received_frames","received_bytes","received_sha256","received_digest_complete","captured_prefix_bytes",
        "captured_prefix_sha256","declared_message_bytes","withheld_stream_credit_bytes","released_stream_credit_bytes",
        "reply_fully_decoded","reset_requested","response_abandoned","abort_requested","connection_exit"}

def _actor_dto(actor):
    require(type(actor) is dict and set(actor)==ACTOR_FIELDS, "held_actor_fields")
    require(type(actor["phase"]) is str and actor["phase"] in
        ("Prepared","Opening","Held","Settling","Settled"), "held_actor_phase_type")
    require(actor["first_failure"] is None or (type(actor["first_failure"]) is str
        and actor["first_failure"] in ("Preparation","Transition","Clock","Transport","Framing","ConnectionJob","PriorFailure")), "held_actor_failure_type")
    require(type(actor["connection_exit"]) is str and actor["connection_exit"] in
        ("NotSpawned","Live","Closed","AbortedAndJoined","ConnectionFailed","JoinFailed"), "held_actor_exit_type")
    require(all(type(actor[key]) is bool for key in ("received_digest_complete","reply_fully_decoded",
        "reset_requested","response_abandoned","abort_requested")), "held_actor_boolean_types")
    require(old._uint(actor["backend_index"],2) and old._uint(actor["actual_grpc_port"],65535)
        and old._uint(actor["proven_offered_sequence"]) and old._uint(actor["received_frames"],(1<<32)-1)
        and all(old._uint(actor[key]) for key in ("received_bytes","captured_prefix_bytes",
            "withheld_stream_credit_bytes","released_stream_credit_bytes")), "held_actor_numeric_types")
    require(actor["declared_message_bytes"] is None or old._uint(actor["declared_message_bytes"],(1<<32)-1), "held_actor_declared_type")
    _hex_array(actor["received_sha256"]); _hex_array(actor["captured_prefix_sha256"])

def _identity_copy(value,root,code):
    _task(value)
    require(old._json_equal(value,root),code)

def _ack_dto(ack):
    require(type(ack) is dict and set(ack)=={"label","root","profile","kind","wanted","consumed",
        "accepted_consumed","outcome","observed_from_original_prelaunch_us"}, "closed_ack_fields")
    _task(ack["root"])
    require(all(old._text(ack[key]) for key in ("label","kind","outcome")), "closed_ack_text_types")
    require(old._uint(ack["profile"],(1<<32)-1) and old._uint(ack["consumed"])
        and old._uint(ack["accepted_consumed"])
        and (ack["wanted"] is None or old._uint(ack["wanted"]))
        and old._uint(ack["observed_from_original_prelaunch_us"],20000000), "closed_ack_scalar_types")

def _receipt_dtos(receipt):
    require(type(receipt) is dict and set(receipt)==RECEIPT_FIELDS, "held_receipt_fields")
    require(type(receipt["schema_version"]) is int and old._text(receipt["status"])
        and old._text(receipt["timings_origin"]) and old._text(receipt["scope"]), "held_receipt_text_types")
    require(all(type(receipt[key]) is bool for key in ("native_acceptance","protocol_complete","actor_actual_join",
        "mysql_actual_join","kill_attempted","kill_returned","expected_server_error_actual_source_retained",
        "expected_connection_joinerror_retained","raw_error_formatter_invoked")), "held_receipt_boolean_types")
    require(all(old._uint(receipt[key],20000000) for key in
        ("protocol_started_us","kill_started_us","kill_returned_us","settled_us")), "held_receipt_clock_types")
    require(old._uint(receipt["mysql_terminal_code"],65535), "held_receipt_terminal_type")
    for key in ("health_before_tasks_created","health_after_tasks_created"):
        require(type(receipt[key]) is list and len(receipt[key])==3
            and all(old._uint(n) for n in receipt[key]), "held_receipt_health_types")
    require(type(receipt["failed_source_slots"]) is list and len(receipt["failed_source_slots"])==8
        and all(type(flag) is bool for flag in receipt["failed_source_slots"]), "held_failed_source_types")
    old.parse_roles(receipt["original_roles"])
    source=receipt["source_pins"]
    require(type(source) is dict and set(source)=={"clean_revision","source_tree_sha256","server_binary_sha256",
        "runner_binary_sha256","base_config_sha256","execution_binding_sha256"}
        and old._hex(source["clean_revision"],40)
        and all(old._hex(source[key]) for key in source if key!="clean_revision"), "held_source_pin_types")
    _task(receipt["selected_root"])
    require(type(receipt["independent_target_source"]) is dict, "held_target_source_type")
    acks=receipt["closed_acks"]
    require(type(acks) is list and len(acks)==2, "two_original_late_acks")
    for ack in acks: _ack_dto(ack)
    _samples_dto(receipt["root_samples"])
    _wire_dto(receipt["original_wire"]); _wire_dto(receipt["same_socket_health"])
    _actor_dto(receipt["actor"])

def verify_held_final(expected,actual,*,kill=os.kill):
    require(type(expected) is HeldExpected and type(actual) is HeldIndependentMaterials and expected.case==CASE, "held_expected_type")
    p=expected.provenance
    require(type(p) is dict and set(p)==old.PROVENANCE_FIELDS and old._hex(p["clean_revision"],40), "held_provenance")
    require(all(old._hex(p[k]) for k in old.PROVENANCE_FIELDS-{"clean_revision","server_build_identity"}), "held_provenance_hash")
    require(p["server_build_identity"]==expected.server_build_commit==expected.runner_build_commit==expected.runner_build_identity==p["clean_revision"], "held_full_build")
    require(p["large_input_sha256"]==LARGE_SHA and p["tiny_input_sha256"]==TINY_SHA and expected.held_input_sha256==INPUT_SHA, "immutable_inputs")
    require(old._hex(expected.cargo_lock_sha256) and old._hex(expected.effective_launch_config_sha256)
        and old._hex(expected.original_execution_binding_sha256), "held_expected_hash")
    require(type(expected.roles) is tuple and old.parse_roles([item.json() for item in expected.roles])==expected.roles, "held_original_roles")
    require(all(old._path(v) for v in (expected.server_path,expected.runner_path,expected.base_config_path,expected.runtime_dir,expected.evidence_path)), "held_paths")
    require(type(actual.held_input_bytes) is bytes and old._digest(actual.held_input_bytes)==INPUT_SHA, "held_actual_input")
    require(type(actual.held_binding_bytes) is bytes and old._digest(actual.held_binding_bytes)==p["frozen_execution_binding_sha256"], "held_actual_binding")
    require(type(actual.original_binding_bytes) is bytes and old._digest(actual.original_binding_bytes)==expected.original_execution_binding_sha256, "original_actual_binding")
    binding=old.parse_finite_json(actual.held_binding_bytes)
    require(set(binding)=={"schema_version","kind","runnable","frozen_before_execution",
        "original_execution_binding_path","original_execution_binding_sha256","held_input_sha256",
        "cargo_lock_sha256","neutral_feature_policy","prelaunch_effective_config_policy","preparation","scene"}
        and type(binding["schema_version"]) is int and binding["schema_version"]==1
        and binding["kind"]=="mem-1-m07-held-native-admission-v1"
        and old._text(binding["original_execution_binding_path"]) and binding["original_execution_binding_path"],
        "held_binding_fields")
    require(old._json_equal(binding["preparation"],{
        "deadline_ms":30000,"command_ms":5000,"reap_ms":1000,"binding_bytes":131072,"input_bytes":65536,
        "config_bytes":4194304,"binary_bytes":4294967296,"git_stdout_bytes":8388608,
        "stderr_bytes":65536,"identity_stdout_bytes":4096,"scratch_bytes":65536}),"held_prepare_bounds")
    require(old._json_equal(binding["scene"],{
        "name":CASE,"whole_prelaunch_ms":20000,"metadata_wait_ms":5000,"whole_protocol_phase_ms":5000,
        "phase_sample_interval_ms":100,"phase_max_samples":51,"mysql_write_deadline_ms":30000,
        "closing_deadline_ms":5000,"request_frame_bytes":4096,"response_data_bytes":1052672,
        "held_capture_bytes":4096,"held_frame_bytes":16384,"held_capture_frames":16,
        "max_identity_markers_per_backend":8,"original_log_bytes_per_backend":2097152,
        "original_marker_line_bytes":384,"segment_bytes":1048576,"window_positions":2,
        "max_wait_millis":100,"case_count":1}),"held_scene_bounds")
    original=old.parse_finite_json(actual.original_binding_bytes)
    require(original.get("schema_version")==1 and type(original.get("schema_version")) is int
        and original.get("kind")=="mem-1-m07-exact-native-admission-v1"
        and original.get("runnable") is True and original.get("frozen_before_execution") is True,
        "original_binding_admission")
    require(all(original.get(k)==p[k] for k in old.PROVENANCE_FIELDS-{"frozen_execution_binding_sha256"}),
        "original_binding_provenance")
    require(binding.get("runnable") is True and binding.get("frozen_before_execution") is True
        and binding.get("held_input_sha256")==INPUT_SHA and binding.get("cargo_lock_sha256")==expected.cargo_lock_sha256
        and binding.get("original_execution_binding_sha256")==expected.original_execution_binding_sha256
        and binding.get("neutral_feature_policy")=="actual_exact_argument_neutral_feature_full_clean_build"
        and binding.get("prelaunch_effective_config_policy")=="freeze_original_prepared_before_role_spawn", "held_binding_admission")
    require(actual.neutral_build_actual_wait_zero is True, "neutral_diagnostic_wait")
    line=("NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_BUILD commit="+p["clean_revision"]+
        " build_identity="+p["clean_revision"]+" root_observation=true\n").encode("ascii")
    require(type(actual.neutral_build_stdout) is bytes and actual.neutral_build_stdout==line, "neutral_actual_build")
    effective=old._validate_actual(expected,actual.common)
    old._validate_runner_log(expected,actual.common.runner_log)
    old._validate_schema5(expected,actual.common,effective)
    receipt=old.parse_finite_json(actual.common.observation_bytes)
    _receipt_dtos(receipt)
    require(set(receipt)==RECEIPT_FIELDS and receipt["schema_version"]==1 and type(receipt["schema_version"]) is int
        and receipt["status"]=="COMPONENT_AND_SCENE_ASSERTIONS_PASSED_PENDING_ROLE_EXIT" and receipt["native_acceptance"] is False,
        "held_receipt_fields")
    require(receipt["source_pins"]=={k:p[v] for k,v in (("clean_revision","clean_revision"),("source_tree_sha256","source_tree_sha256"),
        ("server_binary_sha256","server_binary_sha256"),("runner_binary_sha256","runner_binary_sha256"),("base_config_sha256","base_config_sha256"),("execution_binding_sha256","frozen_execution_binding_sha256"))}, "held_receipt_provenance")
    require(old.parse_roles(receipt["original_roles"])==expected.roles, "held_receipt_roles")
    require(all(receipt[k] is True for k in ("protocol_complete","actor_actual_join","mysql_actual_join","kill_attempted","kill_returned","expected_server_error_actual_source_retained")), "held_original_join_control")
    require(receipt["raw_error_formatter_invoked"] is False and type(receipt["failed_source_slots"]) is list and len(receipt["failed_source_slots"])==8 and all(flag is False for flag in receipt["failed_source_slots"]), "held_failed_source_slots")
    root,slot=_target(expected,actual,receipt["independent_target_source"])
    _identity_copy(receipt["selected_root"],root,"selected_root_independent")
    times=[receipt[k] for k in ("protocol_started_us","kill_started_us","kill_returned_us","settled_us")]
    require(all(old._uint(t,20000000) for t in times) and times==sorted(times) and times[-1]-times[0]<=5000000
        and receipt["timings_origin"]=="the original launch_config prelaunch instant, not scene entry", "held_original_clock")
    acks=receipt["closed_acks"]
    require(type(acks) is list and len(acks)==2, "two_original_late_acks")
    for ack,label in zip(acks,("ACK1_first","ACK1_repeat")):
        require(type(ack) is dict and set(ack)=={"label","root","profile","kind","wanted","consumed","accepted_consumed","outcome","observed_from_original_prelaunch_us"}, "closed_ack_fields")
        _identity_copy(ack["root"],root,"closed_ack_root_independent")
        require(ack["label"]==label and type(ack["profile"]) is int and ack["profile"]==1
            and ack["kind"]=="ClientRows" and ack["wanted"] is None and type(ack["consumed"]) is int and ack["consumed"]==1
            and type(ack["accepted_consumed"]) is int and ack["accepted_consumed"]==0 and ack["outcome"]=="AwaitTerminalControl"
            and old._uint(ack["observed_from_original_prelaunch_us"],20000000)
            and times[2]<=ack["observed_from_original_prelaunch_us"]<=times[3], "closed_ack_actual_outcome")
    require(acks[0]["observed_from_original_prelaunch_us"]<=acks[1]["observed_from_original_prelaunch_us"],"ack_clock_order")
    samples=receipt["root_samples"]
    require(type(samples) is list and 0<len(samples)<=51, "root_sample_positions")
    require(all(type(sample) is dict and old._uint(sample.get("observed_from_original_prelaunch_us"),20000000)
        and times[0]<=sample["observed_from_original_prelaunch_us"]<=times[3] for sample in samples),"census_original_clock")
    require([sample["observed_from_original_prelaunch_us"] for sample in samples]==sorted(sample["observed_from_original_prelaunch_us"] for sample in samples),"census_clock_order")
    for label,sealed,holder in (("W2",False,False),("W2_repeat",False,False),("quiet_before_replay",False,False),("held_open",False,True),
        ("sealed_held",True,True),("ACK1_first",True,True),("ACK1_repeat",True,True)):
        qualifying=[s for s in samples if type(s) is dict and s.get("phase")==label and _root_sample(s,slot,sealed,holder)]
        require(bool(qualifying), "positive_original_context_holder")
    require(type(receipt["mysql_terminal_code"]) is int and receipt["mysql_terminal_code"]==1317,"mysql_actual_terminal_code")
    before=receipt["health_before_tasks_created"]; after=receipt["health_after_tasks_created"]
    require(type(before) is list and type(after) is list and len(before)==len(after)==3
        and all(old._uint(n) for n in before+after)
        and all(a>=b for a,b in zip(after,before)) and any(a>b for a,b in zip(after,before)),"same_socket_native_tasks")
    w2=[sample for sample in samples if sample.get("phase")=="W2" and _root_sample(sample,slot,False,False)]
    repeat=[sample for sample in samples if sample.get("phase")=="W2_repeat" and _root_sample(sample,slot,False,False)]
    require(repeat[0]["observed_from_original_prelaunch_us"]-w2[-1]["observed_from_original_prelaunch_us"]>=100000,"original_sample_interval")
    _wire(receipt["original_wire"],False); _wire(receipt["same_socket_health"],True)
    actor=receipt["actor"]
    _actor_dto(actor)
    require( actor["phase"]=="Settled" and actor["first_failure"] is None
        and actor["backend_index"]==slot and actor["proven_offered_sequence"]==1
        and actor["connection_exit"] in ("AbortedAndJoined","Closed") and actor["reply_fully_decoded"] is False
        and actor["released_stream_credit_bytes"]==0 and actor["withheld_stream_credit_bytes"]>0
        and actor["response_abandoned"] is True and actor["abort_requested"] is True, "original_held_actor")
    require(receipt["expected_connection_joinerror_retained"] is (actor["connection_exit"]=="AbortedAndJoined"),"original_connection_source")
    require(old._uint(actor["backend_index"],2) and type(actor["proven_offered_sequence"]) is int
        and type(actor["released_stream_credit_bytes"]) is int
        and old._uint(actor["withheld_stream_credit_bytes"]),"held_actor_scalar_types")
    require(type(actor["actual_grpc_port"]) is int and 0<actor["actual_grpc_port"]<=65535
        and old._uint(actor["received_frames"],16) and actor["received_frames"]>0
        and old._uint(actor["received_bytes"],1052672) and actor["received_bytes"]>=5
        and old._uint(actor["captured_prefix_bytes"],4096) and actor["captured_prefix_bytes"]==min(actor["received_bytes"],4096)
        and old._uint(actor["declared_message_bytes"],1052667) and actor["declared_message_bytes"]>=1048576
        and actor["received_bytes"]<actor["declared_message_bytes"]+5
        and actor["received_digest_complete"] is True
        and actor["withheld_stream_credit_bytes"]>=actor["received_bytes"],"incomplete_original_response")
    _hex_array(actor["received_sha256"]);_hex_array(actor["captured_prefix_sha256"])
    absent=[]
    for identity in expected.roles:
        try: kill(identity.pid,0)
        except OSError as error:
            require(type(error.errno) is int and error.errno==errno.ESRCH,"original_pid_absence_unknown")
            absent.append({**identity.json(),"result":"ESRCH"})
        except BaseException: raise old.VerificationFailure("original_pid_absence_unknown") from None
        else: raise old.VerificationFailure("original_pid_present") from None
    return {"schema_version":1,"case":CASE,"native_acceptance":True,"original_role_absence":absent,
        "scope":"context-owned unique root positive delivery/reservation holder and late ACK accepted0, original response connection job/MySQL joins and four PID ESRCH; excludes last Arc alias, allocator deallocation, released roots and fullClosing64"}
