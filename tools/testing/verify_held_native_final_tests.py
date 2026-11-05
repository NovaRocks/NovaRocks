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
"""Pure synthetic verifier negatives; no PID, RPC, service or Native execution."""
import copy
import errno
import json
from pathlib import Path
from unittest import mock
import unittest
from types import SimpleNamespace
import verify_exact_native_final as old
import verify_held_native_final as held

class HeldVerifierProjectionTests(unittest.TestCase):
    def fixture(self):
        fe="01900000-0000-7000-8000-000000000004"
        be=tuple("01900000-0000-7000-8000-00000000000"+str(i) for i in (1,2,3))
        roles=tuple(old.LaunchIdentity(role,i+1,"synthetic-component") for i,role in enumerate(old.ROLES))
        # A u64 attempt above u32 guards against guessing the production identity width.
        attempt=1<<33
        task=dict(query_hi=0,query_lo=-7,attempt=attempt,stage=1,task=1,backend=be[1])
        logs=(b"",(
            f"{held.CREATE} execution_id=0:-7:{attempt} stage=1 task=1 backend={be[1]}\n"
            f"{held.ROOT} execution_id=0:-7:{attempt} stage=1 task=1 backend={be[1]}\n"
            f"{held.CONTEXT} execution_id=0:-7:{attempt} frontend={fe} backend={be[1]}\n").encode(),b"")
        target=dict(schema_version=1,source="RootObservation",frontend=fe,root=task,root_backend_index=1,
            backend_process_from_descriptor=be[1],actual_backend_descriptors=list(be),original_roles=[r.json() for r in roles],
            contexts=[None,dict(query_hi=0,query_lo=-7,attempt=attempt,frontend=fe,backend=be[1]),None],
            fresh_tasks=[dict(backend_index=1,identity=task)],marker_counts=[0,3,0],baseline_log_bytes=[0,0,0],
            baseline_sha256=[list(bytes.fromhex(old._digest(b""))) for _ in range(3)],
            after_log_bytes=[len(raw) for raw in logs],after_sha256=[list(bytes.fromhex(old._digest(raw))) for raw in logs])
        actual=SimpleNamespace(frontend_marker_line=f"NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_FE frontend_process_id={fe}\n".encode(),
            actual_backend_descriptors=be,original_backend_log_prefixes=logs)
        return SimpleNamespace(roles=roles),actual,target
    def test_original_log_projection_accepts_unique_root_without_two_task_or_highest_stage_guess(self):
        expected,actual,target=self.fixture()
        root,slot=held._target(expected,actual,target)
        self.assertEqual(slot,1);self.assertEqual(root["attempt"],1<<33)
    def test_rpc_pid_or_exact_marker_cannot_refill_neutral_fe_source(self):
        expected,actual,target=self.fixture()
        actual.frontend_marker_line=b"NOVAROCKS_MEM_1_M07_EXACT_MYSQL_FE pid=12\n"
        with self.assertRaises(old.VerificationFailure):held._target(expected,actual,target)
    def test_replaced_log_prefix_wrong_descriptor_and_context_are_rejected(self):
        expected,actual,target=self.fixture()
        for mutate in (lambda t:t["after_sha256"][1].__setitem__(0,0),
            lambda t:t.__setitem__("backend_process_from_descriptor",t["actual_backend_descriptors"][0]),
            lambda t:t["contexts"][1].__setitem__("query_lo",-8)):
            changed=copy.deepcopy(target);mutate(changed)
            with self.assertRaises(old.VerificationFailure):held._target(expected,actual,changed)
    def test_missing_root_cannot_pass_using_context_or_census(self):
        expected,actual,target=self.fixture()
        target["fresh_tasks"]=[]
        with self.assertRaises(old.VerificationFailure):held._target(expected,actual,target)
    def test_query_zero_and_bool_stage_are_not_typed_identities(self):
        _,_,target=self.fixture()
        for key,value in (("stage",True),("task",0),("attempt",0)):
            changed=target["root"].copy();changed[key]=value
            with self.assertRaises(old.VerificationFailure):held._task(changed)
        changed=target["root"].copy();changed["query_lo"]=0
        with self.assertRaises(old.VerificationFailure):held._task(changed)
    def test_full_native_verifier_refuses_unadmitted_shape_before_any_pid_probe(self):
        calls=[]
        with self.assertRaises(old.VerificationFailure):
            held.verify_held_final(object(),object(),kill=lambda pid,sig:calls.append((pid,sig)))
        self.assertEqual(calls,[])


class HeldVerifierStrictDtoTests(unittest.TestCase):
    fixture=HeldVerifierProjectionTests.fixture
    # Component fixtures only. They mint neither role births nor Native admission.
    def replace_log(self, actual, target, raw):
        actual.original_backend_log_prefixes=(b"",raw,b"")
        target["after_log_bytes"][1]=len(raw)
        target["after_sha256"][1]=list(bytes.fromhex(old._digest(raw)))

    def sample(self,label,us,sealed=False,holder=False):
        roots=[{key:0 for key in held.ROOT_FIELDS} for _ in range(3)]
        root=roots[1]
        root.update(channels=1,terminal_task_records=1,producers_exited=1,ends_published=1,
            sealed=int(sealed),data_positions=0 if sealed else 2,
            payload_bytes=0 if sealed else 1048584,segments=0 if sealed else 2)
        if holder: root.update(deliveries=1,retained_reservations=1,metadata_holders=1,metadata_bytes=1)
        return dict(phase=label,roots=roots,tasks_created=[0,1,0],observed_from_original_prelaunch_us=us)

    def wire(self,health=False):
        return dict(rows=1,row_payload_bytes=5 if health else 1048580,wire_bytes=100 if health else 1048700,
            packets=5,columns=1,schema=[dict(name="total" if health else "payload",mysql_type=8 if health else 253)],
            metadata_sha256="1"*64,wire_prefix_sha256="2"*64,first_payload_chunk_micros=1,
            first_row_micros=2,elapsed_micros=3,row_sha256=held.HEALTH_SHA if health else held.ROW_SHA,
            error=None if health else "original result failure; actual source retained")

    def receipt(self):
        expected,actual,target=self.fixture(); root=copy.deepcopy(target["root"])
        actor=dict(phase="Settled",first_failure=None,backend_index=1,actual_grpc_port=1234,
            proven_offered_sequence=1,received_frames=1,received_bytes=40,received_sha256=[0]*32,
            received_digest_complete=True,captured_prefix_bytes=40,captured_prefix_sha256=[0]*32,
            declared_message_bytes=1048584,withheld_stream_credit_bytes=40,released_stream_credit_bytes=0,
            reply_fully_decoded=False,reset_requested=True,response_abandoned=True,abort_requested=True,
            connection_exit="AbortedAndJoined")
        acks=[dict(label=label,root=copy.deepcopy(root),profile=1,kind="ClientRows",wanted=None,
            consumed=1,accepted_consumed=0,outcome="AwaitTerminalControl",
            observed_from_original_prelaunch_us=us) for label,us in (("ACK1_first",350000),("ACK1_repeat",500000))]
        samples=[self.sample(*args) for args in (("W2",1000),("W2_repeat",101000),
            ("quiet_before_replay",110000),("held_open",120000,False,True),
            ("sealed_held",310000,True,True),("ACK1_first",410000,True,True),("ACK1_repeat",520000,True,True))]
        receipt=dict(schema_version=1,status="COMPONENT_AND_SCENE_ASSERTIONS_PASSED_PENDING_ROLE_EXIT",
            native_acceptance=False,source_pins={key:("a"*40 if key=="clean_revision" else "b"*64)
                for key in ("clean_revision","source_tree_sha256","server_binary_sha256","runner_binary_sha256",
                    "base_config_sha256","execution_binding_sha256")},original_roles=[role.json() for role in expected.roles],
            selected_root=root,independent_target_source=target,closed_acks=acks,protocol_complete=True,
            actor=actor,actor_actual_join=True,mysql_actual_join=True,kill_attempted=True,kill_returned=True,
            timings_origin="the original launch_config prelaunch instant, not scene entry",protocol_started_us=1000,
            kill_started_us=200000,kill_returned_us=300000,settled_us=1000000,
            expected_server_error_actual_source_retained=True,expected_connection_joinerror_retained=True,
            root_samples=samples,original_wire=self.wire(),same_socket_health=self.wire(True),
            failed_source_slots=[False]*8,raw_error_formatter_invoked=False,scope="synthetic component only",
            mysql_terminal_code=1317,health_before_tasks_created=[0,1,0],health_after_tasks_created=[0,2,0])
        self.assertEqual(set(receipt),held.RECEIPT_FIELDS)
        return expected,actual,receipt

    def verify_component(self, receipt):
        # Exercise the complete final verifier's new paths, while explicitly
        # replacing only old independent source/build admission with pure stubs.
        # No actual PID, role, process, binary or Native fact is asserted here.
        _,projection,reference=self.receipt()
        # Share the actual Rust test fixture; execution templates stay outside Git.
        repository=Path(old.__file__).resolve().parents[2]
        inputs=repository/"tests/system-test-runner/fixtures/mem-1-m07"
        held_input=(inputs/"held-late-ack-freeze-v3.json").read_bytes()
        binding=json.loads((repository/"tests/system-test-runner/src/held_native_admission_test_binding.json").read_bytes())
        commit="a"*40
        provenance={key:"b"*64 for key in old.PROVENANCE_FIELDS}
        provenance.update(clean_revision=commit,server_build_identity=commit,
            large_input_sha256=held.LARGE_SHA,tiny_input_sha256=held.TINY_SHA)
        original=dict(provenance,schema_version=1,kind="mem-1-m07-exact-native-admission-v1",
            runnable=True,frozen_before_execution=True)
        original_raw=json.dumps(original).encode()
        binding.update(runnable=True,frozen_before_execution=True,
            original_execution_binding_path="/component/original.json",held_input_sha256=held.INPUT_SHA,
            original_execution_binding_sha256=old._digest(original_raw),cargo_lock_sha256="c"*64)
        binding_raw=json.dumps(binding).encode()
        provenance["frozen_execution_binding_sha256"]=old._digest(binding_raw)
        receipt=copy.deepcopy(receipt)
        receipt["source_pins"]={key:provenance[source] for key,source in (
            ("clean_revision","clean_revision"),("source_tree_sha256","source_tree_sha256"),
            ("server_binary_sha256","server_binary_sha256"),("runner_binary_sha256","runner_binary_sha256"),
            ("base_config_sha256","base_config_sha256"),("execution_binding_sha256","frozen_execution_binding_sha256"))}
        roles=tuple(old.LaunchIdentity(role,i+1,"synthetic-component") for i,role in enumerate(old.ROLES))
        expected=held.HeldExpected(held.CASE,provenance,commit,commit,commit,"c"*64,"d"*64,roles,
            "/component/server","/component/runner","/component/config","/component/runtime","/component/evidence",
            old._digest(original_raw),held.INPUT_SHA)
        actual=held.HeldIndependentMaterials(SimpleNamespace(observation_bytes=json.dumps(receipt).encode(),runner_log=b""),
            f"NOVAROCKS_MEM_1_M07_ROOT_OBSERVATION_BUILD commit={commit} build_identity={commit} root_observation=true\n".encode(),
            True,held_input,binding_raw,original_raw,projection.frontend_marker_line,
            projection.actual_backend_descriptors,projection.original_backend_log_prefixes)
        calls=[]
        def no_real_pid(pid,sig):
            calls.append((pid,sig))
            raise OSError(errno.ESRCH,"synthetic absence only")
        with mock.patch.object(old,"_validate_actual",return_value={}), \
             mock.patch.object(old,"_validate_runner_log",return_value=None), \
             mock.patch.object(old,"_validate_schema5",return_value=None):
            try:
                result=held.verify_held_final(expected,actual,kill=no_real_pid)
            except old.VerificationFailure as error:
                self.assertEqual(calls,[],"invalid DTO must fail before any role absence query")
                self.assertNotIn(error.code,("held_binding_fields","held_binding_admission",
                    "original_binding_admission","original_binding_provenance","held_actual_input",
                    "held_actual_binding","original_actual_binding"),"negative fixture must pass admission setup")
                raise
        self.assertEqual(calls,[(r.pid,0) for r in roles])
        return result

    def test_complete_component_envelope_reaches_only_injected_pid_boundary(self):
        _,_,receipt=self.receipt()
        result=self.verify_component(receipt)
        self.assertEqual(result["case"],held.CASE)  # Not a Native receipt.

    def test_raw_context_and_matching_dto_foreign_execution_are_rejected(self):
        for hi,lo,attempt in ((0,-8,1<<33),(0,-7,(1<<33)+1),(1<<63,-7,1<<33),
            (-(1<<63)-1,-7,1<<33),(0,-7,1<<64),(0,0,1<<33),(0,-7,0)):
            with self.subTest(hi=hi,lo=lo,attempt=attempt):
                expected,actual,target=self.fixture()
                raw=actual.original_backend_log_prefixes[1]
                old_line=raw.split(b"\n")[2]
                new_line=f"{held.CONTEXT} execution_id={hi}:{lo}:{attempt} frontend={target['frontend']} backend={target['root']['backend']}".encode()
                self.replace_log(actual,target,raw.replace(old_line,new_line))
                target["contexts"][1].update(query_hi=hi,query_lo=lo,attempt=attempt)
                with self.assertRaises(old.VerificationFailure): held._target(expected,actual,target)

    def test_execution_signed_and_attempt_unsigned_boundaries_remain_accepted(self):
        for hi,lo,attempt in ((-(1<<63),(1<<63)-1,(1<<64)-1),((1<<63)-1,-(1<<63),1)):
            with self.subTest(hi=hi):
                expected,actual,target=self.fixture()
                raw=actual.original_backend_log_prefixes[1].replace(f"0:-7:{1<<33}".encode(),f"{hi}:{lo}:{attempt}".encode())
                target["root"].update(query_hi=hi,query_lo=lo,attempt=attempt)
                target["contexts"][1].update(query_hi=hi,query_lo=lo,attempt=attempt)
                self.replace_log(actual,target,raw)
                held._target(expected,actual,target)

    def test_lf_only_no_embedded_marker_or_long_line_can_invent_boundary(self):
        for prefix in (b"noise\v",b"noise\r",b"noise\x85","noise\u0085".encode(),
            "noise\u2028".encode(),"noise\u2029".encode(),b"noise ",b"x"*385):
            with self.subTest(prefix=prefix[:8]):
                expected,actual,target=self.fixture()
                self.replace_log(actual,target,prefix+actual.original_backend_log_prefixes[1])
                with self.assertRaises(old.VerificationFailure): held._target(expected,actual,target)

    def test_long_unrelated_lf_line_is_ignored_and_hash_anchored(self):
        expected,actual,target=self.fixture()
        self.replace_log(actual,target,b"x"*4096+b"\n"+actual.original_backend_log_prefixes[1])
        held._target(expected,actual,target)

    def test_partial_line_duplicate_embedded_marker_and_invalid_utf8_refuse(self):
        expected,actual,target=self.fixture()
        original=actual.original_backend_log_prefixes[1]
        for raw in (original[:-1],original.replace(b" stage=",b" "+held.CONTEXT.encode()+b" stage=",1),
            b"\xff\n"+original,original+original.split(b"\n")[0]+b"\n"):
            with self.subTest(raw_length=len(raw)):
                e,a,t=self.fixture();self.replace_log(a,t,raw)
                with self.assertRaises(old.VerificationFailure): held._target(e,a,t)

    def test_every_copied_identity_bool_unknown_missing_and_foreign_refuse(self):
        for which in ("selected_root",0,1):
            for key,value in (("query_hi",False),("stage",True),("task",True),("attempt",True),
                ("backend","01900000-0000-7000-8000-000000000001"),("query_lo",-8)):
                with self.subTest(which=which,key=key):
                    _,_,receipt=self.receipt()
                    root=receipt["selected_root"] if which=="selected_root" else receipt["closed_acks"][which]["root"]
                    root[key]=value
                    with self.assertRaises(old.VerificationFailure): self.verify_component(receipt)
            for missing in (False,True):
                _,_,receipt=self.receipt()
                root=receipt["selected_root"] if which=="selected_root" else receipt["closed_acks"][which]["root"]
                if missing: del root["stage"]
                else: root["unknown"]=1
                with self.assertRaises(old.VerificationFailure): self.verify_component(receipt)

    def test_valid_but_unqualified_known_sample_cannot_hide_invalid_sample(self):
        _,_,receipt=self.receipt()
        quiet=self.sample("W2",1100);quiet["roots"]=[{key:0 for key in held.ROOT_FIELDS} for _ in range(3)]
        receipt["root_samples"].insert(1,quiet)
        self.verify_component(receipt)  # Preserve bounded polling observations.
        for change in (lambda s:s.__setitem__("phase","unknown"),lambda s:s.__setitem__("extra",1),
            lambda s:s["roots"][0].__setitem__("channels",False),lambda s:s["roots"][0].__setitem__("unknown",1),
            lambda s:s["tasks_created"].__setitem__(0,True),lambda s:s.__setitem__("observed_from_original_prelaunch_us",True)):
            changed=copy.deepcopy(receipt);change(changed["root_samples"][1])
            with self.assertRaises(old.VerificationFailure): self.verify_component(changed)

    def test_every_census_field_bool_negative_overflow_and_unknown_are_rejected(self):
        for key in held.ROOT_FIELDS:
            for value in (False,-1,1<<64):
                sample=self.sample("W2",1000);sample["roots"][0][key]=value
                with self.subTest(key=key,value=value),self.assertRaises(old.VerificationFailure):held._samples_dto([sample])
        for count in (0,52):
            with self.assertRaises(old.VerificationFailure): held._samples_dto([self.sample("W2",1000)]*count)

    def test_native_copy_holder_and_original_segments_have_distinct_phase_contracts(self):
        self.verify_component(self.receipt()[2])
        for label,sealed in (("held_open",False),("sealed_held",True),("ACK1_first",True),("ACK1_repeat",True)):
            expected=0 if sealed else 2
            for segments in (0,1,2,3):
                sample=self.sample(label,1000,sealed,True)
                sample["roots"][1]["segments"]=segments
                self.assertEqual(held._root_sample(sample,1,sealed,True),segments==expected)
            for field in ("deliveries","retained_reservations","metadata_holders","metadata_bytes"):
                _,_,receipt=self.receipt()
                sample=next(s for s in receipt["root_samples"] if s["phase"]==label)
                sample["roots"][1][field]=0
                with self.subTest(phase=label,field=field),self.assertRaises(old.VerificationFailure):
                    self.verify_component(receipt)

    def test_nested_harness_role_tuple_is_rejected_by_independent_verifier(self):
        for key in ("receipt", "target"):
            _,_,receipt=self.receipt()
            owner=receipt if key=="receipt" else receipt["independent_target_source"]
            roles=owner["original_roles"]
            owner["original_roles"]=[roles[0],roles[1:]]
            with self.subTest(key=key),self.assertRaises(old.VerificationFailure) as failure:
                self.verify_component(receipt)
            self.assertEqual(failure.exception.code,"four_original_roles")

    def test_wire_numeric_hash_timestamp_and_schema_fields_strict(self):
        for health in (False,True):
            for key in ("rows","row_payload_bytes","wire_bytes","packets","columns"):
                for value in (True,-1,1<<64,"1",None):
                    row=self.wire(health);row[key]=value
                    with self.subTest(key=key,value=value),self.assertRaises(old.VerificationFailure): held._wire(row,health)
            for key in ("metadata_sha256","wire_prefix_sha256","row_sha256"):
                for value in ("A"*64,"0"*63,[0]*32,None,True):
                    row=self.wire(health);row[key]=value
                    with self.subTest(key=key),self.assertRaises(old.VerificationFailure): held._wire(row,health)
            for key in ("first_payload_chunk_micros","first_row_micros","elapsed_micros"):
                for value in (True,-1,1<<128,"0"):
                    row=self.wire(health);row[key]=value
                    with self.subTest(key=key),self.assertRaises(old.VerificationFailure):held._wire(row,health)
            row=self.wire(health);row["first_payload_chunk_micros"]=None;row["first_row_micros"]=None
            held._wire(row,health)  # Source Option<u128>, no guessed timing requirement.
            for mutation in (lambda r:r.__setitem__("extra",1),lambda r:r["schema"][0].__setitem__("extra",1),
                lambda r:r["schema"][0].__setitem__("mysql_type",True),lambda r:r["schema"][0].__setitem__("name",False)):
                row=self.wire(health);mutation(row)
                with self.assertRaises(old.VerificationFailure):held._wire(row,health)

    def test_all_actor_numeric_booleans_digests_and_reset_are_typed(self):
        _,_,receipt=self.receipt();actor=receipt["actor"]
        numeric=("backend_index","actual_grpc_port","proven_offered_sequence","received_frames","received_bytes",
            "captured_prefix_bytes","declared_message_bytes","withheld_stream_credit_bytes","released_stream_credit_bytes")
        for key in numeric:
            for value in (True,-1,"0",1<<64):
                changed=copy.deepcopy(actor);changed[key]=value
                with self.subTest(key=key,value=value),self.assertRaises(old.VerificationFailure):held._actor_dto(changed)
        for key in ("received_digest_complete","reply_fully_decoded","reset_requested","response_abandoned","abort_requested"):
            for value in (0,1,[],None,"true"):
                changed=copy.deepcopy(actor);changed[key]=value
                with self.subTest(key=key),self.assertRaises(old.VerificationFailure):held._actor_dto(changed)
        for key in ("received_sha256","captured_prefix_sha256"):
            for value in ([True]+[0]*31,[256]+[0]*31,[0]*31,"0"*64):
                changed=copy.deepcopy(actor);changed[key]=value
                with self.subTest(key=key),self.assertRaises(old.VerificationFailure):held._actor_dto(changed)
        for key in ("phase","first_failure","connection_exit"):
            changed=copy.deepcopy(actor);changed[key]="unknown"
            with self.assertRaises(old.VerificationFailure):held._actor_dto(changed)
        for value in (False,True):
            _,_,receipt=self.receipt();receipt["actor"]["reset_requested"]=value
            self.verify_component(receipt)  # Bool is a diagnostic, not proof RST arrived.

    def test_top_level_and_ack_dto_unknown_duplicate_oversize_bool_refuse(self):
        for mutation in (lambda r:r.__setitem__("unknown",1),lambda r:r.__setitem__("schema_version",True),
            lambda r:r.__setitem__("protocol_started_us",True),lambda r:r.__setitem__("mysql_terminal_code",True),
            lambda r:r.__setitem__("scope",[]),lambda r:r["health_after_tasks_created"].__setitem__(0,True),
            lambda r:r["closed_acks"][0].__setitem__("extra",1),lambda r:r["closed_acks"][1].__setitem__("consumed",True),
            lambda r:r["closed_acks"][0].__setitem__("accepted_consumed",False),lambda r:r["actor"].__setitem__("extra",1)):
            _,_,receipt=self.receipt();mutation(receipt)
            with self.assertRaises(old.VerificationFailure):self.verify_component(receipt)
        for raw in (b'{"schema_version":1,"schema_version":1}',b'{"n":NaN}',b" "*(old.JSON_BYTES+1)):
            with self.assertRaises(old.VerificationFailure):old.parse_finite_json(raw)


if __name__=="__main__": unittest.main()
