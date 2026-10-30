# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file for additional
# information regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Independent fake-service observations for scratch scope and lifetime rules."""
import copy
import json
from pathlib import Path
import shlex
import socket
import sys
import tempfile
import unittest
import weakref
from unittest.mock import patch
from urllib.parse import unquote

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import fixture_runtime as runtime
import scratch

NOW = 1800000000000
POLICY = dict(scratch_ttl_seconds=604800, max_run_seconds=86400, cleanup_grace_seconds=300)


class Clock:
    def __init__(self):
        self.wall_ms, self.tick = NOW, 100.0

    def wall(self):
        return self.wall_ms / 1000

    def mono(self):
        return self.tick

    def advance(self, seconds):
        self.tick += seconds
        self.wall_ms += int(seconds * 1000)


def identity(age_seconds=0, token="a" * 32, worktree="s3://novarocks/worktree-a"):
    return scratch.ScratchIdentity.create(worktree, now_ms=NOW - age_seconds * 1000, token=token)


def record():
    return dict(policy=copy.deepcopy(POLICY), id="machine-test", control_root="/machine-test", daemon_id="daemon-test", version="v1", generation="g1",
                config=dict(benchmark_root="s3://novarocks/shared/benchmarks", minio_volume="minio-test"),
                protected_benchmark_roots=["s3://novarocks/shared/benchmarks"],
                ports=dict(rest=28002, rest_mv=28003), server_warehouses={"rest": "s3://warehouse/service", "rest-mv": "s3://warehouse/service-mv"},
                offline_catalog_barrier=dict(schema=1, daemon_id="daemon-test", generation="g1",
                    volumes=["nr-fixture-machine_rest-catalog", "nr-fixture-machine_rest-mv-catalog"], complete=True),
                credentials=dict(access_key="SECRETACCESS", secret_key="SECRETSECRET"))


class Backend:
    timeout = 10

    def __init__(self, clock):
        self.clock, self.calls = clock, []
        self.objects, self.users, self.policies = set(), set(), set()
        self.disabled, self.fail, self.delay, self.raw = set(), {}, 0, None
        self.user_info, self.policy_info = {}, {}

    def provision(self, identity, purpose="init"):
        user, policy = identity.iam_name(purpose), identity.resource_name(purpose)
        bucket, key = identity.scope_root[5:].split("/", 1)
        document = {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::" + bucket],
             "Condition": {"StringLike": {"s3:prefix": [key + "/", key + "/*"]}}},
            {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
             "Resource": ["arn:aws:s3:::" + bucket + "/" + key + "/*"]}]}
        self.users.add(user)
        self.policies.add(policy)
        self.user_info[user] = dict(status="success", accessKey=user, policyName=policy, memberOf=[])
        # Native mc userPolicyMessage wraps madmin.PolicyInfo, whose exported
        # PolicyName and json.RawMessage Policy retain their exact capitals.
        self.policy_info[policy] = dict(status="success", policy=policy,
                                        policyInfo=dict(PolicyName=policy, Policy=document))
        return user, policy

    def run_mc(self, record, script, *, deadline):
        assert "SECRETACCESS" not in script and "SECRETSECRET" not in script
        assert "${MINIO_ROOT_USER}" in script and "${MINIO_ROOT_PASSWORD}" in script
        command = next(line for line in script.splitlines() if line.startswith("mc --json "))
        args = tuple(shlex.split(command)[2:-1])  # Last token is the stderr redirect.
        self.calls.append(args)
        self.clock.advance(self.delay)
        if self.raw is not None:
            return self.raw
        if args in self.fail:
            value = dict(status="error", error=dict(cause=dict(error=dict(Code=self.fail[args]))))
            return json.dumps(value, indent=2) + "\nNOVA_SCRATCH_MC_EXIT=1"
        values = []
        if args == ("ls", "store"):
            buckets = {x[5:].split("/", 1)[0] for x in self.objects}
            values = [dict(status="success", type="folder", key=b + "/") for b in sorted(buckets)]
        elif args[:2] == ("ls", "--recursive"):
            root = "s3://" + args[-1].removeprefix("store/")
            values = [dict(status="success", type="file", key=value[len(root):])
                      for value in sorted(self.objects) if value.startswith(root)]
        elif args[0] == "ls":
            root = "s3://" + args[1].removeprefix("store/")
            prefix = root if root.endswith("/") else root + "/"
            children = set()
            for value in self.objects:
                if value.startswith(prefix):
                    rest = value[len(prefix):]
                    name = rest.split("/", 1)[0] + ("/" if "/" in rest else "")
                    children.add(name)
            values = [dict(status="success", type="folder" if c.endswith("/") else "file", key=c)
                      for c in sorted(children)]
        elif args[0] == "rm":
            root = "s3://" + args[-1].removeprefix("store/")
            self.objects = {x for x in self.objects if not x.startswith(root)}
            values = [dict(status="success")]
        elif args[:3] == ("admin", "user", "list"):
            values = [dict(status="success", accessKey=n) for n in sorted(self.users)]
        elif args[:3] == ("admin", "policy", "list"):
            values = [dict(status="success", policy=n) for n in sorted(self.policies)]
        elif args[:3] == ("admin", "user", "info"):
            if args[-1] not in self.users:
                return '{"status":"error","error":{"code":"NoSuchUser"}}\nNOVA_SCRATCH_MC_EXIT=1'
            values = [copy.deepcopy(self.user_info.get(args[-1], dict(status="success", accessKey=args[-1])))]
        elif args[:3] == ("admin", "policy", "info"):
            if args[-1] not in self.policies:
                return '{"status":"error","error":{"code":"NoSuchPolicy"}}\nNOVA_SCRATCH_MC_EXIT=1'
            values = [copy.deepcopy(self.policy_info.get(args[-1], dict(status="success", policy=args[-1])))]
        elif args[:3] == ("admin", "user", "disable"):
            self.disabled.add(args[-1])
            values = [dict(status="success")]
        elif args[:3] == ("admin", "user", "rm"):
            self.users.discard(args[-1])
            values = [dict(status="success")]
        elif args[:3] == ("admin", "policy", "rm"):
            self.policies.discard(args[-1])
            values = [dict(status="success")]
        else:
            raise AssertionError(args)
        return "\n".join(json.dumps(x, indent=2) for x in values) + "\nNOVA_SCRATCH_MC_EXIT=0"


class Sweeper(scratch.ScratchSweeper):
    def __init__(self, record, backend, clock):
        super().__init__(record, backend, wall_clock=clock.wall, monotonic=clock.mono)
        self.rest, self.rest_calls, self.rest_fail = {}, [], {}
        self.rest_mv = {}

    def _rest_request(self, service, method, path, deadline, *, absent_ok=False):
        self._check(deadline)
        self.rest_calls.append((service, method, path))
        if (method, path) in self.rest_fail:
            raise runtime.RuntimeFailure(self.rest_fail[method, path], "private diagnostic SECRETSECRET")
        rest = self.rest if service == "rest" else self.rest_mv
        parts = path.split("?")[0].split("/")
        if path == "namespaces":
            return {"namespaces": [[n] for n in rest]}
        name = unquote(parts[1])
        if name not in rest:
            if absent_ok:
                return None
            raise AssertionError(path)
        if len(parts) == 2 and method == "DELETE":
            del rest[name]
            return {}
        collection = parts[2]
        if method == "GET" and len(parts) == 3:
            return {"identifiers": [dict(namespace=[name], name=child)
                                    for child in rest[name][collection]]}
        child = unquote(parts[3])
        if method == "GET":
            return dict(metadata=dict(location=rest[name][collection][child]))
        del rest[name][collection][child]
        return {}


class RuntimeAssertions:
    def assert_code(self, code, call):
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            call()
        self.assertEqual(caught.exception.code, code)


class IdentityTests(RuntimeAssertions, unittest.TestCase):
    def test_identity_is_exact_and_scope_is_a_run_not_a_worktree(self):
        value = identity()
        self.assertEqual(value.run_id, "nr_scratch_1800000000000_" + "a" * 32)
        self.assertEqual(value.scope_root, "s3://novarocks/worktree-a/scratch/" + value.run_id)
        self.assertEqual(value.as_dict()["created_unix_ms"], NOW)
        self.assert_code("InvalidScratchScope", lambda: scratch.ScratchIdentity(value.run_id, "s3://novarocks/worktree-a"))

    def test_distinct_tokens_and_purposes_get_distinct_bounded_iam_keys(self):
        value = identity()
        names = {value.iam_name("init"), value.iam_name("rot"), identity(token="b" * 32).iam_name("init")}
        self.assertEqual(len(names), 3)
        self.assertTrue(all(len(n) == 20 and scratch.parse_iam_name(n) == NOW for n in names))
        self.assertIsNone(scratch.parse_iam_name("minioadmin"))
        self.assertIsNone(scratch.parse_iam_name(value.run_id))

    def test_resource_length_is_rejected_never_truncated(self):
        self.assertEqual(scratch.resource_run(identity().resource_name("init")), identity().run_id)
        self.assert_code("InvalidScratchIdentity", lambda: identity().resource_name("toolong"))
        self.assert_code("InvalidScratchIdentity", lambda: identity().resource_name("../"))

    def test_json_identity_rejects_a_contradictory_timestamp(self):
        value = identity().as_dict()
        self.assertEqual(scratch.ScratchIdentity.from_dict(value), identity())
        value["created_unix_ms"] += 1
        self.assert_code("InvalidScratchIdentity", lambda: scratch.ScratchIdentity.from_dict(value))

    def test_unsafe_and_encoded_s3_paths_are_rejected(self):
        for uri in ("s3://novarocks/a/../b", "s3://novarocks/a//b", "s3://novarocks/a%2fb",
                    "s3://novarocks/a?x=1", "s3://novarocks/a b", "s3://novarocks/ /b",
                    "https://novarocks/a", "s3://key:secret@bucket/a"):
            self.assert_code("InvalidScratchScope", lambda uri=uri: scratch.canonical_s3(uri))


class BudgetTests(RuntimeAssertions, unittest.TestCase):
    def setUp(self):
        self.clock, self.record, self.identity = Clock(), record(), identity()

    def admit(self, **kwargs):
        return scratch.RunBudget.admit(self.record, self.identity, wall_clock=self.clock.wall,
                                       monotonic=self.clock.mono, **kwargs)

    def test_budget_reserves_teardown_and_expires_without_reset(self):
        budget = self.admit(seconds=600)
        self.assertEqual(budget.remaining_seconds(work=True), 300)
        self.clock.advance(301)
        self.assert_code("RunBudgetExpired", budget.check)
        self.assertEqual(budget.remaining_seconds(), 299)
        self.clock.advance(300)
        self.assertEqual(budget.remaining_seconds(), 0)

    def test_nested_context_preserves_original_deadline(self):
        original = self.admit(seconds=1200)
        self.clock.advance(100)
        inherited = self.admit(seconds=86400, inherited=original.as_dict())
        self.assertEqual(inherited.deadline, original.deadline)
        self.assertEqual(inherited.deadline_unix_ms, original.deadline_unix_ms)
        self.assertEqual(inherited.remaining_seconds(), 1100)

    def test_short_machine_maximum_does_not_revalidate_unused_default_budget(self):
        self.record["policy"]["max_run_seconds"] = 1800
        original = self.admit(seconds=1200)
        self.clock.advance(100)
        inherited = self.admit(inherited=original.as_dict())
        self.assertEqual(inherited.remaining_seconds(), 1100)
        self.assertEqual(inherited.deadline_unix_ms, original.deadline_unix_ms)

    def test_cleanup_can_enter_original_reserve_but_cannot_mint_or_extend_budget(self):
        original = self.admit(seconds=600)
        self.assert_code("RunBudgetRejected", lambda: self.admit(cleanup=True))
        self.clock.advance(450)
        self.assert_code("RunBudgetExpired", lambda: self.admit(inherited=original.as_dict()))
        cleanup = self.admit(inherited=original.as_dict(), cleanup=True)
        self.assertEqual(cleanup.remaining_seconds(), 150)
        self.assertEqual(cleanup.deadline_unix_ms, original.deadline_unix_ms)
        self.clock.advance(150)
        self.assert_code("RunBudgetExpired", lambda: self.admit(inherited=original.as_dict(), cleanup=True))

    def test_inherited_work_and_complete_expiry_remain_runtime_failures(self):
        source = self.admit(seconds=600).as_dict()
        self.clock.advance(300)
        self.assert_code("RunBudgetExpired", lambda: self.admit(inherited=source))
        self.assertEqual(self.admit(inherited=source, cleanup=True).remaining_seconds(), 300)
        self.clock.advance(300)
        self.assert_code("RunBudgetExpired", lambda: self.admit(inherited=source))
        self.assert_code("RunBudgetExpired", lambda: self.admit(inherited=source, cleanup=True))
        invalid = copy.deepcopy(source)
        invalid["deadline_unix_ms"] = invalid["started_unix_ms"] + 86401 * 1000
        self.assert_code("RunBudgetRejected", lambda: self.admit(inherited=invalid, cleanup=True))
        self.record["protected_benchmark_roots"] = [self.identity.scope_root]
        self.assert_code("RunBudgetRejected", lambda: self.admit(inherited=source, cleanup=True))

    def test_invalid_budgets_are_rejected_before_any_external_operation(self):
        for seconds in (0, -1, 300, 86401, True, 1.5):
            self.assert_code("RunBudgetRejected", lambda seconds=seconds: self.admit(seconds=seconds))

    def test_expired_tampered_or_different_run_context_is_rejected(self):
        source = self.admit(seconds=1200).as_dict()
        for key, replacement in (("schema", 2), ("cleanup_grace_seconds", 1), ("deadline_unix_ms", NOW + 999999999)):
            changed = copy.deepcopy(source)
            changed[key] = replacement
            self.assert_code("RunBudgetRejected", lambda: self.admit(inherited=changed))
        changed = copy.deepcopy(source)
        changed["identity"] = identity(token="b" * 32).as_dict()
        self.assert_code("RunBudgetRejected", lambda: self.admit(inherited=changed))
        self.clock.advance(1000)
        self.assert_code("RunBudgetExpired", lambda: self.admit(inherited=source))

    def test_admission_protects_custom_benchmark_ancestors_and_descendants(self):
        self.record["protected_benchmark_roots"] = [self.identity.scope_root + "/ready"]
        self.assert_code("RunBudgetRejected", self.admit)
        self.record["protected_benchmark_roots"] = ["s3://novarocks/worktree-a"]
        self.assert_code("RunBudgetRejected", self.admit)


class SweepTests(unittest.TestCase):
    def setUp(self):
        self.clock, self.record = Clock(), record()
        self.backend = Backend(self.clock)
        self.sweeper = Sweeper(self.record, self.backend, self.clock)
        self.old, self.young = identity(604801), identity(1, token="b" * 32)

    def sweep(self, expired_only=True):
        return self.sweeper.sweep(expired_only=expired_only, deadline=self.clock.mono() + 100)

    def test_only_expired_exact_run_prefix_is_removed(self):
        old, young = self.old.scope_root + "/data/file", self.young.scope_root + "/data/file"
        unrelated = {"s3://novarocks/shared/benchmarks/ready", "s3://novarocks/worktree-a/important",
                     "s3://novarocks/worktree-a/scratch/manual/data"}
        self.backend.objects = {old, young, *unrelated}
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertEqual(self.backend.objects, {young, *unrelated})
        removals = [c for c in self.backend.calls if c[0] == "rm"]
        self.assertEqual(removals, [("rm", "--recursive", "--force", self.old.scope_root.replace("s3://", "store/") + "/")])

    def test_observation_counts_unexpired_resources_before_cleanup_without_extra_enumeration(self):
        for value in (self.old, self.young):
            self.backend.provision(value)
            self.backend.objects.add(value.scope_root + "/file")
            self.sweeper.rest[value.resource_name("test")] = dict(tables={}, views={})
        result = self.sweep()
        self.assertEqual(result["observed_counts"], {"s3-prefix": 2, "rest-namespace": 2, "user": 2, "policy": 2})
        self.assertEqual(result["oldest_created_unix_ms"], scratch.created_ms(self.old.run_id))
        self.assertEqual(result["sampled_at_unix_ms"], NOW)
        self.assertTrue(result["counts_are_pre_cleanup"])
        self.assertTrue(result["enumeration_complete"])
        self.assertIn("nonexpired child metadata not inspected", result["classification_basis"]["rest-namespace"])
        self.assertEqual(sum(c == ("ls", "store") for c in self.backend.calls), 1)
        self.assertEqual(sum(c[:3] == ("admin", "user", "list") for c in self.backend.calls), 1)
        self.assertFalse(any(c[2] != "namespaces" and self.young.run_id in c[2] for c in self.sweeper.rest_calls))
        again = self.sweep()
        self.assertEqual(again["observed_counts"], {"s3-prefix": 1, "rest-namespace": 1, "user": 1, "policy": 1})
        self.assertEqual(again["removed"], [])

    def test_observation_excludes_protected_runs_and_separates_unknown_iam(self):
        self.record["protected_benchmark_roots"].append(self.old.scope_root + "/staging")
        self.sweeper = Sweeper(self.record, self.backend, self.clock)
        self.backend.objects = {self.old.scope_root + "/file"}
        self.sweeper.rest[self.old.resource_name("test")] = dict(tables={}, views={})
        self.backend.users.add(self.young.iam_name("init"))
        result = self.sweep()
        self.assertEqual(result["observed_counts"], {"s3-prefix": 0, "rest-namespace": 0, "user": 0, "policy": 0})
        self.assertIsNone(result["oldest_created_unix_ms"])
        self.assertEqual(result["unknown_counts"], dict(user=1, policy=0, **{"rest-namespace": 0}))
        self.assertFalse(result["enumeration_complete"])

    def test_upgrade_observation_explicitly_excludes_stopped_rest_scope(self):
        result = self.sweep(False)
        self.assertEqual(result["enumeration_scope"], ["s3-prefix", "user", "policy"])
        self.assertTrue(result["enumeration_complete"])
        self.assertEqual(result["observed_counts"]["rest-namespace"], 0)
        self.assertIsNone(result["oldest_created_unix_ms"])

    def test_upgrade_removes_all_scratch_without_calling_stopped_rest(self):
        self.backend.objects = {self.old.scope_root + "/data", self.young.scope_root + "/data"}
        self.assertTrue(self.sweep(False)["complete"])
        self.assertFalse(self.backend.objects)
        self.assertFalse(self.sweeper.rest_calls)

    def test_custom_benchmark_nested_in_scratch_blocks_entire_prefix_deletion(self):
        protected = self.old.scope_root + "/staging"
        self.record["protected_benchmark_roots"] = [protected]
        self.sweeper = Sweeper(self.record, self.backend, self.clock)
        objects = {protected + "/data", self.old.scope_root + "/other"}
        self.backend.objects = set(objects)
        self.assertTrue(self.sweep(False)["complete"])
        self.assertEqual(self.backend.objects, objects)

    def test_benchmark_descendants_are_not_even_enumerated(self):
        self.backend.objects = {"s3://novarocks/shared/benchmarks/staging/scratch/" + self.old.run_id + "/data"}
        self.assertTrue(self.sweep(False)["complete"])
        self.assertFalse(any("benchmarks/" in c[-1] for c in self.backend.calls))

    def test_users_and_policies_are_scoped_and_repeated_sweeps_are_idempotent(self):
        old_user, young_user = self.old.iam_name("init"), self.young.iam_name("init")
        old_policy, young_policy = self.old.resource_name("init"), self.young.resource_name("init")
        self.backend.provision(self.old)
        self.backend.provision(self.young)
        self.backend.users = {old_user, young_user, "ordinary-user"}
        self.backend.policies = {old_policy, young_policy, "readwrite"}
        self.assertTrue(self.sweep()["complete"])
        self.assertTrue(self.sweep()["complete"])
        self.assertEqual(self.backend.users, {young_user, "ordinary-user"})
        self.assertEqual(self.backend.policies, {young_policy, "readwrite"})

    def test_compact_iam_second_precision_never_collects_before_exact_ttl(self):
        boundary = identity(604800)
        self.backend.provision(boundary)
        self.assertTrue(self.sweep()["complete"])
        self.assertTrue(self.backend.users)
        self.clock.advance(1)
        self.assertTrue(self.sweep()["complete"])
        self.assertFalse(self.backend.users)

    def test_upgrade_revocation_disables_only_scratch_users_before_purge(self):
        self.backend.provision(self.old)
        self.backend.provision(self.young, "rot")
        self.backend.users = {self.old.iam_name("init"), self.young.iam_name("rot"), "ordinary-user"}
        result = self.sweeper.revoke_users()
        self.assertTrue(result["complete"])
        self.assertEqual(self.backend.disabled, self.backend.users - {"ordinary-user"})
        policy_reads = [i for i,c in enumerate(self.backend.calls) if c[:3] == ("admin", "policy", "info")]
        disables = [i for i,c in enumerate(self.backend.calls) if c[:3] == ("admin", "user", "disable")]
        self.assertTrue(policy_reads)
        self.assertLess(max(policy_reads), min(disables))
        self.assertFalse(any(c[:3] == ("admin", "policy", "rm") for c in self.backend.calls))

    def test_ttl_collects_unique_policy_then_user_add_crash_before_attach(self):
        old_user, old_policy = self.backend.provision(self.old)
        young_user, young_policy = self.backend.provision(self.young)
        for user in (old_user, young_user):
            self.backend.user_info[user]["policyName"] = ""
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertEqual(self.backend.users, {young_user})
        self.assertEqual(self.backend.policies, {young_policy})
        policy_reads = [i for i,c in enumerate(self.backend.calls) if c[:3] == ("admin", "policy", "info")]
        removals = [i for i,c in enumerate(self.backend.calls) if c[:3] in (("admin", "user", "rm"), ("admin", "policy", "rm"))]
        self.assertLess(max(policy_reads), min(removals))
        self.assertLess(self.backend.calls.index(("admin", "user", "rm", "store", old_user)),
                        self.backend.calls.index(("admin", "policy", "rm", "store", old_policy)))
        self.assertTrue(self.sweep()["complete"])
        self.assertTrue(self.sweeper.revoke_users()["complete"])
        self.assertEqual(self.backend.disabled, {old_user, young_user})

    def test_upgrade_revokes_unattached_unique_user_without_removing_mapping_policy(self):
        for value, purpose in ((self.old,"init"),(self.young,"rot")):
            user,_ = self.backend.provision(value,purpose)
            self.backend.user_info[user]["policyName"] = ""
        policies = set(self.backend.policies)
        for _ in range(2):
            self.assertTrue(self.sweeper.revoke_users()["complete"])
        self.assertEqual(self.backend.disabled,self.backend.users)
        self.assertEqual(self.backend.policies,policies)
        self.assertFalse(any(c[:3] in (("admin", "user", "rm"),("admin", "policy", "rm")) for c in self.backend.calls))
        self.assertTrue(self.sweep(False)["complete"])
        self.assertFalse(self.backend.users)
        self.assertFalse(self.backend.policies)

    def test_unknown_unattached_native_facts_preserve_candidate_policy_for_retry(self):
        for change in ({"policyName":None}, {"policyName":[]}, {"policyName":"", "memberOf":{}},
                       {"policyName":"", "memberOf":[{"name":"administrators"}]},
                       {"policyName":"readwrite"}, {"policyName":"candidate,readwrite"}):
            with self.subTest(change=change):
                self.backend=Backend(self.clock)
                self.sweeper=Sweeper(self.record,self.backend,self.clock)
                user,policy=self.backend.provision(self.old)
                self.backend.user_info[user].update(change)
                result=self.sweep()
                self.assertFalse(result["complete"])
                self.assertEqual(self.backend.users,{user})
                self.assertEqual(self.backend.policies,{policy})
                with self.assertRaises(runtime.RuntimeFailure): self.sweeper.revoke_users()
                self.assertFalse(self.backend.disabled)
                self.assertEqual(self.backend.policies,{policy})

    def test_unattached_hash_collision_never_selects_one_of_multiple_full_run_policies(self):
        user,first=self.backend.provision(self.old)
        _,second=self.backend.provision(identity(604801,token="c"*32))
        self.backend.users={user}
        self.backend.user_info[user]["policyName"]=""
        # A collision in the bounded hash is possible in principle. Simulate
        # only that hash result; both native policy documents remain exact.
        with patch.object(scratch.ScratchIdentity,"iam_name",return_value=user):
            result=self.sweep()
            self.assertFalse(result["complete"])
            self.assertEqual(self.backend.users,{user})
            self.assertEqual(self.backend.policies,{first,second})
            with self.assertRaises(runtime.RuntimeFailure): self.sweeper.revoke_users()
        self.assertFalse(self.backend.disabled)

    def test_unread_policy_prevents_unique_inference_but_explicit_binding_can_proceed(self):
        user,policy=self.backend.provision(self.old)
        self.backend.user_info[user]["policyName"]=""
        _,unread=self.backend.provision(identity(604801,token="c"*32))
        self.backend.users.remove(identity(604801,token="c"*32).iam_name("init"))
        bound,bound_policy=self.backend.provision(identity(604801,token="d"*32))
        self.backend.fail[("admin","policy","info","store",unread)]="AccessDenied"
        result=self.sweep()
        self.assertFalse(result["complete"])
        self.assertIn(user,self.backend.users)
        self.assertNotIn(bound,self.backend.users)
        self.assertEqual(self.backend.policies,{policy,unread,bound_policy})
        self.assertFalse(any(c[:3]==("admin","policy","rm") for c in self.backend.calls))

    def test_partial_native_policy_list_cannot_establish_unique_orphan_mapping(self):
        for disable in (False,True):
            with self.subTest(disable=disable):
                self.backend=Backend(self.clock)
                self.sweeper=Sweeper(self.record,self.backend,self.clock)
                user,policy=self.backend.provision(self.old)
                self.backend.user_info[user]["policyName"]=""
                bound,bound_policy=self.backend.provision(identity(604801,token="d"*32))
                original=self.backend.run_mc
                def partial(record,script,*,deadline):
                    if "mc --json admin policy list store " in script:
                        self.backend.calls.append(("admin","policy","list","store"))
                        # A valid first row is followed by the native failure.
                        # The first row alone must never prove uniqueness.
                        error = dict(status="error",error=dict(cause=dict(error=dict(Code="AccessDenied"))))
                        return json.dumps(dict(status="success",policy=policy))+"\n"+json.dumps(error)+"\nNOVA_SCRATCH_MC_EXIT=1"
                    return original(record,script,deadline=deadline)
                self.backend.run_mc=partial
                if disable:
                    with self.assertRaises(runtime.RuntimeFailure): self.sweeper.revoke_users()
                    self.assertEqual(self.backend.disabled,{bound})
                else:
                    self.assertFalse(self.sweep()["complete"])
                    self.assertIn(user,self.backend.users)
                    self.assertNotIn(bound,self.backend.users)
                self.assertEqual(self.backend.policies,{policy,bound_policy})
                self.assertFalse(any(c[:3]==("admin","policy","rm") for c in self.backend.calls))

    def test_s3_pending_prefix_bound_does_not_truncate_complete_iam_inventory(self):
        user,policy=self.backend.provision(self.old)
        self.backend.user_info[user]["policyName"]=""
        _,other=self.backend.provision(identity(604801,token="c"*32))
        self.backend.users={user}
        with patch.object(scratch,"MAX_PENDING_PREFIXES",1):
            result=self.sweep()
        self.assertTrue(result["complete"],result)
        self.assertTrue(result["enumeration_complete"])
        self.assertEqual(self.backend.users,set())
        self.assertEqual(self.backend.policies,set())
        inspected={call[-1] for call in self.backend.calls
                   if call[:3]==("admin","policy","info")}
        self.assertEqual(inspected,{policy,other})

    def test_scratch_shaped_unmapped_user_is_preserved_not_guessed(self):
        user = self.old.iam_name("init")
        self.backend.users.add(user)
        self.backend.user_info[user] = dict(status="success", accessKey=user, policyName="readwrite")
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][0]["code"], "ScratchIamUnmapped")
        self.assertEqual(self.backend.users, {user})
        self.assertFalse(self.backend.disabled)
        with self.assertRaises(runtime.RuntimeFailure):
            self.sweeper.revoke_users()
        self.assertFalse(self.backend.disabled)

    def test_compact_policy_is_unmapped_and_never_deleted(self):
        name = self.old.iam_name("init")
        self.backend.policies.add(name)
        result = self.sweep(False)
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][0]["code"], "ScratchIamUnmapped")
        self.assertEqual(self.backend.policies, {name})

    def test_native_user_policy_mapping_must_reproduce_exact_key_hash(self):
        user, policy = self.backend.provision(self.old)
        other = identity(604802, token="c" * 32)
        _, other_policy = self.backend.provision(other)
        self.backend.user_info[user]["policyName"] = other_policy
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertIn(user, self.backend.users)
        self.assertIn(other_policy, self.backend.policies)
        self.assertFalse(any(c[:3] == ("admin", "user", "rm") and c[-1] == user for c in self.backend.calls))

    def test_multiple_policies_or_group_grants_are_not_treated_as_exact_scope(self):
        for change in ({"policyName": self.old.resource_name("init") + ",readwrite"},
                       {"memberOf": [{"name": "administrators", "policies": ["readwrite"]}]}):
            self.backend = Backend(self.clock)
            self.sweeper = Sweeper(self.record, self.backend, self.clock)
            user, policy = self.backend.provision(self.old)
            self.backend.user_info[user].update(change)
            result = self.sweep()
            self.assertFalse(result["complete"])
            self.assertIn(user, self.backend.users)

    def test_native_policy_info_uses_exact_exported_field_names(self):
        user, policy = self.backend.provision(self.old)
        info = self.backend.policy_info[policy]["policyInfo"]
        info["policyName"] = info.pop("PolicyName")
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][0]["code"], "InvalidMcResponse")
        self.assertIn(user, self.backend.users)
        self.assertIn(policy, self.backend.policies)

    def test_policy_wide_object_or_bucket_grants_are_preserved(self):
        cases = ("object-wildcard", "bucket-list-all", "foreign-root", "wildcard-action")
        for change in cases:
            self.backend = Backend(self.clock)
            self.sweeper = Sweeper(self.record, self.backend, self.clock)
            user, policy = self.backend.provision(self.old)
            statements = self.backend.policy_info[policy]["policyInfo"]["Policy"]["Statement"]
            if change == "object-wildcard":
                statements[1]["Resource"] = ["arn:aws:s3:::novarocks/*"]
            elif change == "bucket-list-all":
                statements[0]["Condition"]["StringLike"]["s3:prefix"] = ["*"]
            elif change == "wildcard-action":
                statements[1]["Action"] = ["s3:*"]
            else:
                statements[1]["Resource"] = [self.young.scope_root.replace("s3://", "arn:aws:s3:::") + "/*"]
            result = self.sweep()
            self.assertFalse(result["complete"], change)
            self.assertIn(user, self.backend.users)
            self.assertIn(policy, self.backend.policies)

    def test_protected_benchmark_root_prevents_iam_changes(self):
        user, policy = self.backend.provision(self.old)
        self.record["protected_benchmark_roots"] = [self.old.scope_root + "/ready"]
        self.sweeper = Sweeper(self.record, self.backend, self.clock)
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertIn(user, self.backend.users)
        self.assertIn(policy, self.backend.policies)

    def test_bare_bucket_list_prefix_cannot_grant_same_named_siblings(self):
        user, policy = self.backend.provision(self.old)
        _, key = self.old.scope_root[5:].split("/", 1)
        document = self.backend.policy_info[policy]["policyInfo"]["Policy"]
        document["Statement"][0]["Condition"]["StringLike"]["s3:prefix"] = [key, key + "/*"]
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][0]["code"], "InvalidScratchScope")
        self.assertIn(user, self.backend.users)
        self.assertIn(policy, self.backend.policies)

    def test_valid_orphan_full_run_policy_can_be_collected_without_a_user(self):
        user, policy = self.backend.provision(self.old)
        self.backend.users.remove(user)
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertNotIn(policy, self.backend.policies)

    def test_cleanup_run_ignores_ttl_and_settles_only_exact_original_pairs(self):
        user, policy = self.backend.provision(self.young)
        peer_user, peer_policy = self.backend.provision(self.old)
        self.backend.objects = {self.young.scope_root + "/file", self.old.scope_root + "/file"}
        pair = [{"user": user, "policy": policy}]
        result = self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.young, self.record), pair, self.clock.mono() + 100)
        self.assertTrue(result["complete"], result)
        self.assertEqual(self.backend.users, {peer_user})
        self.assertEqual(self.backend.policies, {peer_policy})
        self.assertEqual(self.backend.objects, {self.old.scope_root + "/file"})
        self.assertFalse(any(c == ("ls", "store") or c[:3] == ("admin", "user", "list") for c in self.backend.calls))
        self.assertTrue(self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.young, self.record), pair, self.clock.mono() + 100)["complete"])

    def test_cleanup_run_settles_predeclared_user_before_policy_was_attached(self):
        user, policy = self.backend.provision(self.young)
        self.backend.user_info[user].pop("policyName")
        self.backend.policies.clear()
        self.backend.objects = {self.young.scope_root + "/file"}
        result = self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.young, self.record), [{"user": user, "policy": policy}], self.clock.mono() + 100)
        self.assertTrue(result["complete"], result)
        self.assertFalse(self.backend.users)
        self.assertFalse(self.backend.objects)

    def test_cleanup_run_can_remove_valid_orphan_policy_and_absent_user(self):
        user, policy = self.backend.provision(self.young)
        self.backend.users.clear()
        result = self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.young, self.record), [{"user": user, "policy": policy}], self.clock.mono() + 100)
        self.assertTrue(result["complete"], result)
        self.assertFalse(self.backend.policies)

    def test_cleanup_run_validates_all_predeclared_pairs_before_remote_calls(self):
        user, policy = self.backend.provision(self.young)
        peer_user, peer_policy = self.backend.provision(self.old)
        pairs = [{"user": user, "policy": policy}, {"user": peer_user, "policy": peer_policy}]
        result = self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.young, self.record), pairs, self.clock.mono() + 100)
        self.assertFalse(result["complete"])
        self.assertFalse(self.backend.calls)
        self.assertEqual(self.backend.users, {user, peer_user})

    def test_cleanup_run_rejects_foreign_group_policy_and_malformed_binding(self):
        for change in ({"policyName": "readwrite"}, {"memberOf": [{"name": "admin"}]}, {"policyName": []}):
            self.backend = Backend(self.clock)
            self.sweeper = Sweeper(self.record, self.backend, self.clock)
            user, policy = self.backend.provision(self.young)
            self.backend.user_info[user].update(change)
            self.backend.objects = {self.young.scope_root + "/file"}
            result = self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.young, self.record), [{"user": user, "policy": policy}], self.clock.mono() + 100)
            self.assertFalse(result["complete"])
            self.assertIn(user, self.backend.users)
            self.assertIn(policy, self.backend.policies)
            self.assertTrue(self.backend.objects)
            self.assertFalse(self.backend.disabled)

    def test_cleanup_run_preserves_prefix_after_iam_permission_failure(self):
        user, policy = self.backend.provision(self.young)
        self.backend.objects = {self.young.scope_root + "/file"}
        self.backend.fail[("admin", "user", "disable", "store", user)] = "AccessDenied"
        result = self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.young, self.record), [{"user": user, "policy": policy}], self.clock.mono() + 100)
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][0]["code"], "ScratchAccessDenied")
        self.assertTrue(self.backend.objects)
        self.assertIn(user, self.backend.users)

    def test_cleanup_run_cannot_touch_protected_scope_or_dispatch_after_deadline(self):
        original_scope = scratch.RunStorageScope.from_record(self.young, self.record)
        self.record["protected_benchmark_roots"] = [self.young.scope_root + "/ready"]
        self.sweeper = Sweeper(self.record, self.backend, self.clock)
        self.assertFalse(self.sweeper.cleanup_run(original_scope, [], self.clock.mono() + 100)["complete"])
        self.assertFalse(self.backend.calls)
        self.assertFalse(self.sweeper.cleanup_run(scratch.RunStorageScope.from_record(self.old, self.record), [], self.clock.mono())["complete"])
        self.assertFalse(self.backend.calls)

    def test_large_benchmark_is_never_recursively_listed_or_loaded(self):
        self.backend.objects = {"s3://novarocks/shared/benchmarks/objects/" + str(i) for i in range(2000)}
        self.backend.objects.add(self.old.scope_root + "/data/file")
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertEqual(len(self.backend.objects), 2000)
        self.assertFalse(any(c[:2] == ("ls", "--recursive") for c in self.backend.calls))
        self.assertFalse(any(c[0] == "ls" and "benchmarks" in c[-1] for c in self.backend.calls))
        self.assertFalse(any(c[0] == "ls" and self.old.run_id in c[-1] for c in self.backend.calls))

    def test_bounded_prefix_continuation_makes_progress_across_sweeps(self):
        with tempfile.TemporaryDirectory() as directory:
            self.record.update(compose_file=str(Path(directory) / "compose.yml"), version="v1", generation="g1")
            values = [identity(604801, token=str(i) * 32, worktree="s3://novarocks/worktree-" + str(i)) for i in range(3)]
            self.backend.objects = {v.scope_root + "/file" for v in values}
            outcomes = []
            with patch.object(scratch, "MAX_PREFIX_VISITS", 2):
                for _ in range(12):
                    self.sweeper = Sweeper(self.record, self.backend, self.clock)
                    result = self.sweep()
                    outcomes.append(result["complete"])
                    self.assertFalse(result["enumeration_complete"])
                    if result["complete"]:
                        break
            self.assertFalse(outcomes[0])
            self.assertTrue(outcomes[-1])
            self.assertFalse(self.backend.objects)
            cursor = json.loads((Path(directory) / "scratch-cursor.json").read_text())
            self.assertEqual(cursor["pending"], [])
            self.assertNotIn("lease", cursor)

    def continuation(self, directory, roots):
        self.record.update(compose_file=str(Path(directory) / "compose.yml"), version="v1", generation="g1")
        path = Path(directory) / "scratch-cursor.json"
        path.write_text(json.dumps(dict(schema=1, version="v1", generation="g1", expired_only=True,
            pending=["s3://novarocks/unvisited"], observed_run_roots=roots)))
        return path

    def test_continued_runs_share_one_native_parent_list_without_authorizing_missing_root(self):
        present = [identity(604801, token=token * 32) for token in ("a", "b", "c")]
        missing = identity(604801, token="d" * 32)
        files = {value.scope_root + "/file" for value in (*present, missing)}
        self.backend.objects = set(files)
        parent_call = ("ls", "store/novarocks/worktree-a/scratch/")
        native = self.backend.run_mc
        def listing(record, script, *, deadline):
            result = native(record, script, deadline=deadline)
            if self.backend.calls[-1] == parent_call:
                # Independent native listing omits one cursor hint. Its file
                # stays in the fake store to detect any cursor-only deletion.
                return "\n".join(json.dumps(dict(status="success", type="folder", key=value.run_id + "/"))
                                 for value in present) + "\nNOVA_SCRATCH_MC_EXIT=0"
            return result
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [value.scope_root for value in (*present, missing)])
            with patch.object(self.backend, "run_mc", side_effect=listing):
                result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertEqual(self.backend.calls.count(parent_call), 1)
        self.assertEqual(self.backend.objects, {missing.scope_root + "/file"})
        self.assertEqual({call[-1] for call in self.backend.calls if call[0] == "rm"},
                         {value.scope_root.replace("s3://", "store/") + "/" for value in present})

    def test_continued_parent_lists_keep_exact_bucket_and_parent_boundaries(self):
        values = [identity(604801, token="a" * 32, worktree="s3://novarocks/worktree-a"),
                  identity(604801, token="b" * 32, worktree="s3://novarocks/worktree-b"),
                  identity(604801, token="c" * 32, worktree="s3://other/worktree-a")]
        self.backend.objects = {value.scope_root + "/file" for value in values}
        parents = [("ls", "store/novarocks/worktree-a/scratch/"),
                   ("ls", "store/novarocks/worktree-b/scratch/"),
                   ("ls", "store/other/worktree-a/scratch/")]
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [value.scope_root for value in values])
            result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertFalse(self.backend.objects)
        for parent in parents:
            self.assertEqual(self.backend.calls.count(parent), 1)

    def test_complete_continued_parent_rows_are_released_before_reading_next_parent(self):
        values = [identity(604801, token="a" * 32, worktree="s3://novarocks/worktree-a"),
                  identity(604801, token="b" * 32, worktree="s3://novarocks/worktree-b")]
        self.backend.objects = {value.scope_root + "/file" for value in values}
        parent_calls = {("ls", "store/novarocks/worktree-a/scratch/"),
                        ("ls", "store/novarocks/worktree-b/scratch/")}
        references = []
        native = self.sweeper._mc
        class Rows(list):
            pass
        def observed(args, deadline, *, absent_ok=False):
            if tuple(args) in parent_calls:
                self.assertTrue(all(reference() is None for reference in references))
                rows = Rows(native(args, deadline, absent_ok=absent_ok))
                references.append(weakref.ref(rows))
                return rows
            return native(args, deadline, absent_ok=absent_ok)
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [value.scope_root for value in values])
            with patch.object(self.sweeper, "_mc", side_effect=observed):
                result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertEqual(len(references), 2)
        self.assertTrue(all(reference() is None for reference in references))
        self.assertFalse(self.backend.objects)

    def test_continued_parent_observations_are_fresh_on_next_same_sweeper_call(self):
        original_file = self.old.scope_root + "/file"
        self.backend.objects = {original_file}
        parent_call = ("ls", "store/novarocks/worktree-a/scratch/")
        native = self.backend.run_mc
        def listing(record, script, *, deadline):
            result = native(record, script, deadline=deadline)
            if self.backend.calls[-1] == parent_call and self.backend.calls.count(parent_call) == 2:
                return "\nNOVA_SCRATCH_MC_EXIT=0"
            return result
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [self.old.scope_root])
            with patch.object(self.backend, "run_mc", side_effect=listing):
                first = self.sweep()
                self.assertTrue(first["complete"], first)
                self.assertFalse(self.backend.objects)
                self.backend.objects = {original_file}
                # A later continuation on the SAME instance must establish
                # native presence again, not reuse the earlier invocation.
                self.continuation(directory, [self.old.scope_root])
                second = self.sweep()
        self.assertTrue(second["complete"], second)
        self.assertEqual(self.backend.calls.count(parent_call), 2)
        self.assertEqual(self.backend.objects, {original_file})

    def test_continued_parent_read_failure_preserves_all_files(self):
        values = [identity(604801, token=token * 32) for token in ("a", "b")]
        files = {value.scope_root + "/file" for value in values}
        self.backend.objects = set(files)
        parent_call = ("ls", "store/novarocks/worktree-a/scratch/")
        self.backend.fail[parent_call] = "AccessDenied"
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [value.scope_root for value in values])
            result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertFalse(result["enumeration_complete"])
        self.assertEqual(result["errors"][-1]["code"], "ScratchAccessDenied")
        self.assertEqual(self.backend.calls.count(parent_call), 1)
        self.assertEqual(self.backend.objects, files)
        self.assertFalse(any(call[0] == "rm" for call in self.backend.calls))

    def test_all_continued_roots_are_validated_before_reading_any_parent(self):
        other = identity(604801, token="b" * 32)
        files = {self.old.scope_root + "/file", other.scope_root + "/file"}
        self.backend.objects = set(files)
        self.record["protected_benchmark_roots"].append(other.scope_root)
        self.sweeper = Sweeper(self.record, self.backend, self.clock)
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [self.old.scope_root, other.scope_root])
            result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][-1]["code"], "RuntimeStateUnreadable")
        self.assertEqual(self.backend.calls.count(("ls", "store/novarocks/worktree-a/scratch/")), 0)
        self.assertEqual(self.backend.objects, files)
        self.assertFalse(any(call[0] == "rm" for call in self.backend.calls))

    def test_continued_root_validation_observes_original_deadline(self):
        values = [identity(604801, token=f"{index:032x}") for index in range(2000)]
        roots = {value.scope_root for value in values}
        files = {root + "/file" for root in roots}
        self.backend.objects = set(files)
        native = self.sweeper._protected
        checked = []
        def protected(uri, *, deletion=False):
            if uri in roots:
                checked.append(uri)
                self.clock.advance(0.01)
            return native(uri, deletion=deletion)
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [value.scope_root for value in values])
            with patch.object(self.sweeper, "_protected", side_effect=protected):
                result = self.sweeper.sweep(expired_only=True, deadline=self.clock.mono() + 0.05)
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][-1]["code"], "ScratchSweepTimeout")
        self.assertGreater(len(checked), 0)
        self.assertLess(len(checked), len(values))
        self.assertEqual(self.backend.calls.count(("ls", "store/novarocks/worktree-a/scratch/")), 0)
        self.assertEqual(self.backend.objects, files)
        self.assertFalse(any(call[0] == "rm" for call in self.backend.calls))

    def test_continued_native_presence_matching_observes_original_deadline(self):
        values = [identity(604801, token=f"{index:032x}") for index in range(2000)]
        files = {value.scope_root + "/file" for value in values}
        self.backend.objects = set(files)
        native = self.sweeper._folder_component
        checked = []
        def component(row):
            checked.append(row["key"])
            self.clock.advance(0.01)
            return native(row)
        with tempfile.TemporaryDirectory() as directory:
            self.continuation(directory, [value.scope_root for value in values])
            with patch.object(self.sweeper, "_folder_component", side_effect=component):
                result = self.sweeper.sweep(expired_only=True, deadline=self.clock.mono() + 0.05)
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][-1]["code"], "ScratchSweepTimeout")
        self.assertGreater(len(checked), 0)
        self.assertLess(len(checked), len(values))
        self.assertEqual(self.backend.calls.count(("ls", "store/novarocks/worktree-a/scratch/")), 1)
        self.assertEqual(self.backend.objects, files)
        self.assertFalse(any(call[0] == "rm" for call in self.backend.calls))

    def test_upgrade_does_not_reuse_partial_expired_only_scan(self):
        with tempfile.TemporaryDirectory() as directory:
            self.record.update(compose_file=str(Path(directory) / "compose.yml"), version="v1", generation="g1")
            self.backend.objects = {self.old.scope_root + "/file", self.young.scope_root + "/file"}
            with patch.object(scratch, "MAX_PREFIX_VISITS", 2):
                self.assertFalse(self.sweep()["complete"])
            self.assertTrue(self.sweep(False)["complete"])
            self.assertFalse(self.backend.objects)

    def test_folder_listing_must_contain_one_safe_relative_component(self):
        native = self.backend.run_mc
        self.backend.objects = {self.old.scope_root + "/file"}

        def malformed(record, script, *, deadline):
            if "store/novarocks/" in script and "rm " not in script:
                return '{"status":"success","type":"folder","key":"../outside/"}\nNOVA_SCRATCH_MC_EXIT=0'
            return native(record, script, deadline=deadline)

        with patch.object(self.backend, "run_mc", side_effect=malformed):
            result = self.sweep(False)
        self.assertFalse(result["complete"])
        self.assertTrue(self.backend.objects)
        self.assertEqual(result["errors"][0]["code"], "InvalidMcResponse")

    def test_partial_iam_failure_remains_failure_and_does_not_leak_diagnostics(self):
        user = self.old.iam_name("init")
        self.backend.provision(self.old)
        self.backend.fail[("admin", "user", "rm", "store", user)] = "AccessDenied"
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertIn(user, self.backend.users)
        self.assertEqual(result["errors"][0]["code"], "ScratchAccessDenied")
        self.assertNotIn("SECRET", json.dumps(result))
        self.assertEqual(result["observed_counts"]["user"], 1)
        self.assertEqual(result["observed_counts"]["policy"], 1)
        self.assertTrue(result["enumeration_complete"])

    def test_revocation_failure_is_not_ignored(self):
        user = self.old.iam_name("init")
        self.backend.provision(self.old)
        self.backend.fail[("admin", "user", "disable", "store", user)] = "AccessDenied"
        with self.assertRaises(runtime.RuntimeFailure) as caught:
            self.sweeper.revoke_users()
        self.assertEqual(caught.exception.code, "ScratchCredentialRevocationFailed")

    def test_deadline_bounds_enumeration_without_deleting_unvisited_objects(self):
        self.backend.objects = {self.old.scope_root + "/file"}
        self.backend.delay = 2
        result = self.sweeper.sweep(expired_only=False, deadline=self.clock.mono() + 1)
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][-1]["code"], "ScratchSweepTimeout")
        self.assertTrue(self.backend.objects)
        self.assertEqual(len(self.backend.calls), 1)
        self.assertFalse(result["enumeration_complete"])

    def test_only_confirmed_native_or_predispatch_timeout_becomes_sweep_timeout(self):
        for code in ("DockerOperationTimeout", "OperationDeadlineExceeded", "ContainerCleanupUnconfirmed",
                     "DockerOperationFailed", "RunBudgetExpired"):
            with self.subTest(code=code), patch.object(self.backend, "run_mc",
                    side_effect=runtime.RuntimeFailure(code, "private diagnostics")):
                result = self.sweep(False)
                expected = "ScratchSweepTimeout" if code in {"DockerOperationTimeout", "OperationDeadlineExceeded"} else code
                self.assertFalse(result["complete"])
                self.assertEqual(result["errors"][-1]["code"], expected)
                self.assertFalse(result["enumeration_complete"])

    def test_cleaned_timeout_never_hides_explicit_native_mc_failure(self):
        cases = [
            ("\nNOVA_SCRATCH_MC_EXIT=0\n", "ScratchSweepTimeout"),
            ('{"status":"success"}\n{"truncated":', "ScratchSweepTimeout"),
            ('{"truncated":\nNOVA_SCRATCH_MC_EXIT=0\n', "InvalidMcResponse"),
            ("\nNOVA_SCRATCH_MC_EXIT=17\n", "ScratchMcFailed"),
            ('{"status":"error","error":{"cause":{"error":{"Code":"AccessDenied","Message":"SECRET"}}}}\nNOVA_SCRATCH_MC_EXIT=17\n', "ScratchAccessDenied"),
            ('{"status":"error","error":{"cause":{"error":{"Code":"AccessDenied"}}}}', "ScratchAccessDenied"),
        ]
        for output, expected in cases:
            with self.subTest(expected=expected):
                error = runtime.RuntimeFailure("DockerOperationTimeout", "cleanup confirmed")
                error.stdout = output
                with patch.object(self.backend, "run_mc", side_effect=error):
                    result = self.sweep(False)
                self.assertFalse(result["complete"])
                self.assertEqual(result["errors"][-1]["code"], expected)
                self.assertNotIn("SECRET", json.dumps(result))

    @staticmethod
    def large_success_mc_body():
        # The observed cleanup returned 51,744 legitimate per-object receipts.
        # Distinct normal-size keys keep the fixture representative without
        # requiring a native object store or a timing/performance assertion.
        return "\n".join(json.dumps(dict(status="success", key=f"object-{index:05d}-" + "x" * 300))
                         for index in range(51744))

    def test_complete_native_mc_receipts_over_sixteen_mib_are_not_rejected(self):
        body=self.large_success_mc_body()
        self.assertGreater(len(body.encode()),16 * 1024 * 1024)
        self.backend.raw=body + "\nNOVA_SCRATCH_MC_EXIT=0\n"
        rows=self.sweeper._mc(["ls","store"],self.clock.mono()+1)
        self.assertEqual(len(rows),51744)
        self.assertEqual(rows[0],dict(status="success",key="object-00000-" + "x" * 300))
        self.assertEqual(rows[-1],dict(status="success",key="object-51743-" + "x" * 300))
        self.assertEqual(self.backend.calls,[("ls","store")])

    def test_large_captured_native_failure_keeps_priority_over_timeout(self):
        body=self.large_success_mc_body()
        self.assertGreater(len(body.encode()),16 * 1024 * 1024)
        native_error='{"status":"error","error":{"cause":{"error":{"Code":"AccessDenied","Message":"SECRET"}}}}'
        for timed_out in (False,True):
            with self.subTest(timed_out=timed_out):
                # A timed-out native command may retain a truncated tail, but
                # its preceding complete native error remains authoritative.
                output=body + "\n" + native_error + ("\n{\"truncated\":" if timed_out else "\nNOVA_SCRATCH_MC_EXIT=17\n")
                failure=runtime.RuntimeFailure("DockerOperationTimeout","cleanup confirmed")
                failure.stdout=output
                with patch.object(self.backend,"run_mc",side_effect=failure) if timed_out else patch.object(self.backend,"run_mc",return_value=output):
                    with self.assertRaises(runtime.RuntimeFailure) as caught:
                        self.sweeper._mc(["rm","--recursive","--force","store/exact/"],self.clock.mono()+1)
                self.assertEqual(caught.exception.code,"ScratchAccessDenied")
                self.assertNotIn("SECRET",str(caught.exception))

    def test_malformed_native_response_cannot_become_empty_success(self):
        for output in ("not json\nNOVA_SCRATCH_MC_EXIT=0", "{}", "\nNOVA_SCRATCH_MC_EXIT=2",
                       '{}\nNOVA_SCRATCH_MC_EXIT=0'):
            self.backend.raw = output
            result = self.sweep(False)
            self.assertFalse(result["complete"], output)

    def test_native_not_found_is_success_but_other_nonzero_is_not(self):
        self.backend.fail[("ls", "store/missing/")] = "NoSuchBucket"
        self.assertEqual(self.sweeper._mc(["ls", "store/missing/"], self.clock.mono() + 1, absent_ok=True), [])
        self.backend.fail[("ls", "store/missing/")] = "AccessDenied"
        with self.assertRaises(runtime.RuntimeFailure):
            self.sweeper._mc(["ls", "store/missing/"], self.clock.mono() + 1, absent_ok=True)

    def test_rest_children_are_validated_and_dropped_before_namespace_and_data(self):
        name = self.old.resource_name("test")
        self.sweeper.rest[name] = dict(views=dict(v=self.old.scope_root + "/view"),
                                       tables=dict(t=self.old.scope_root + "/table"))
        self.sweeper.rest["ordinary"] = dict(views={}, tables={})
        self.backend.objects = {self.old.scope_root + "/view/metadata", self.old.scope_root + "/table/data"}
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        deletions = [c[2] for c in self.sweeper.rest_calls if c[1] == "DELETE"]
        self.assertEqual(deletions, [f"namespaces/{name}/views/v", f"namespaces/{name}/tables/t?purgeRequested=false", f"namespaces/{name}"])
        self.assertEqual(list(self.sweeper.rest), ["ordinary"])
        self.assertFalse(self.backend.objects)

    def test_foreign_or_benchmark_child_location_blocks_catalog_and_s3_deletion(self):
        for location in (self.young.scope_root + "/table", "s3://novarocks/shared/benchmarks/table"):
            self.sweeper.rest[self.old.resource_name("test")] = dict(views={}, tables=dict(t=location))
            self.backend.objects = {self.old.scope_root + "/important"}
            result = self.sweep()
            self.assertFalse(result["complete"])
            self.assertIn(self.old.resource_name("test"), self.sweeper.rest)
            self.assertTrue(self.backend.objects)
            self.assertFalse(any(c[1] == "DELETE" for c in self.sweeper.rest_calls))

    def test_namespace_delete_failure_preserves_s3_data_for_retry(self):
        name = self.old.resource_name("test")
        self.sweeper.rest[name] = dict(views={}, tables={})
        self.sweeper.rest_fail["DELETE", "namespaces/" + name] = "ScratchRestUnavailable"
        self.backend.objects = {self.old.scope_root + "/file"}
        self.assertFalse(self.sweep()["complete"])
        self.assertTrue(self.backend.objects)
        self.sweeper.rest_fail.clear()
        self.assertTrue(self.sweep()["complete"])
        self.assertFalse(self.backend.objects)

    def test_rest_unavailability_does_not_bypass_catalog_cleanup(self):
        self.sweeper.rest_fail["GET", "namespaces"] = "ScratchRestUnavailable"
        self.backend.objects = {self.old.scope_root + "/file"}
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][0]["code"], "ScratchRestUnavailable")
        self.assertFalse(self.backend.calls)
        self.assertTrue(self.backend.objects)

    def test_cancellation_propagates_to_machine_owner_instead_of_success(self):
        with patch.object(self.backend, "run_mc", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                self.sweep(False)


class FixedStorageTests(RuntimeAssertions, unittest.TestCase):
    def setUp(self):
        self.identity, self.record = identity(), record()
        self.scope = scratch.RunStorageScope.from_record(self.identity, self.record)

    def test_shared_python_rust_storage_vectors(self):
        vectors = json.loads(Path(__file__).with_name("run_storage_scope_vectors.json").read_text())
        value = scratch.ScratchIdentity.from_dict(vectors["identity"])
        scope = scratch.RunStorageScope.from_record(value, vectors["record"])
        self.assertEqual(scope.as_dict(), vectors["projection"])
        self.assertEqual(scope.iam_policy(), vectors["policy"])
        for case in vectors["membership"]:
            with self.subTest(uri=case["uri"]):
                if "error" in case:
                    self.assert_code(case["error"], lambda: scope.belongs(case["uri"]))
                else:
                    self.assertEqual(scope.belongs(case["uri"]), case["belongs"])
        for case in vectors["namespaces"]:
            self.assertEqual(scope.owns_namespace(case["name"]), case["owned"])

    def test_fixed_projection_is_three_ranges_without_changing_budget_schema(self):
        value = self.scope.as_dict()
        self.assertEqual(set(value["ranges"]), {"run_root", "rest", "rest_mv"})
        self.assertEqual(value["ranges"]["run_root"], self.identity.scope_root + "/")
        self.assertEqual(value["ranges"]["rest"]["namespace_prefix"], self.identity.run_id + "_")
        self.assertFalse(value["ranges"]["rest"]["namespace_prefix"].endswith("/"))
        self.assertEqual(scratch.RunStorageScope.from_dict(value).validate_against(self.identity, self.record), self.scope)
        for forbidden in ("lease", "deadline", "secret", "enrolled"):
            self.assertNotIn(forbidden, json.dumps(value))

    def test_namespace_requires_complete_bounded_purpose_and_exact_run(self):
        run = self.identity.run_id
        for good in (run + "_a", run + "_a1234"):
            self.assertEqual(scratch.namespace_run(good), run)
            self.assertTrue(self.scope.owns_namespace(good))
        for bad in (run, run + "_", run + "_123", run + "_a12345", run + "_a_b", run + "0_a", run + "_A"):
            self.assertIsNone(scratch.namespace_run(bad), bad)
            self.assertFalse(self.scope.owns_namespace(bad), bad)
        self.assertEqual(scratch.resource_run(run), run)  # IAM/full-root grammar is separate.

    def test_membership_covers_ctas_explicit_rename_and_both_warehouses(self):
        old_namespace = self.identity.resource_name("from")
        for uri in (self.identity.scope_root, self.identity.scope_root + "/rest/staged/table",
                    self.identity.scope_root + "/explicit", "s3://warehouse/service/" + old_namespace + "/table",
                    "s3a://warehouse/service/" + old_namespace + "/table",
                    "s3://warehouse/service-mv/" + self.identity.resource_name("mv") + "/view"):
            self.assertTrue(self.scope.belongs(uri), uri)
        for uri in (identity(token="b" * 32).scope_root + "/table", self.identity.scope_root + "lookalike/table",
                    "s3://warehouse/service/" + self.identity.run_id + "/table",
                    "s3://warehouse/service/" + self.identity.run_id + "_a12345/table",
                    "s3://warehouse/service2/" + old_namespace + "/table",
                    "s3://warehouse/service/other/table"):
            self.assertFalse(self.scope.belongs(uri), uri)
        for uri in ("s3://warehouse/service//" + old_namespace, self.identity.scope_root + "/../table",
                    self.identity.scope_root + "/%2e/table", self.identity.scope_root + "/table?", self.identity.scope_root + "/table#",
                    self.identity.scope_root + "/table*", "https://warehouse/service/table"):
            self.assert_code("InvalidScratchScope", lambda uri=uri: self.scope.belongs(uri))

    def test_inherited_storage_owner_or_ranges_cannot_change(self):
        for change in ("machine", "owner", "warehouse", "prefix", "run_root", "schema"):
            value = self.scope.as_dict()
            if change == "machine": value["machine"]["generation"] = "next-generation"
            elif change == "owner": value["storage_owner"]["daemon_id"] = "other-daemon"
            elif change == "warehouse": value["ranges"]["rest"]["warehouse"] = "s3://warehouse/other/"
            elif change == "prefix": value["ranges"]["rest"]["namespace_prefix"] += "a"
            elif change == "run_root": value["ranges"]["run_root"] += "extra/"
            else: value["schema"] = 2
            self.assert_code("InvalidScratchScope", lambda value=value:
                             scratch.RunStorageScope.from_dict(value).validate_against(self.identity, self.record))
        value = self.scope.as_dict()
        value["ranges"]["rest_mv"]["warehouse"] = value["ranges"]["rest"]["warehouse"] + "nested/"
        self.assert_code("InvalidScratchScope", lambda: scratch.RunStorageScope.from_dict(value))

    def test_fixed_iam_policy_has_literal_prefixes_and_no_namespace_enrollment(self):
        before = self.scope.iam_policy()
        self.assertTrue(self.scope.owns_namespace(self.identity.resource_name("later")))
        self.assertEqual(before, self.scope.iam_policy())
        objects = before["Statement"][-1]["Resource"]
        self.assertEqual(objects, [self.identity.scope_root.replace("s3://", "arn:aws:s3:::") + "/*",
                                  "arn:aws:s3:::warehouse/service/" + self.identity.run_id + "_*",
                                  "arn:aws:s3:::warehouse/service-mv/" + self.identity.run_id + "_*"])
        self.assertFalse(any("_/*" in uri for uri in objects))


class FixedCollectionTests(unittest.TestCase):
    setUp = SweepTests.setUp
    sweep = SweepTests.sweep
    def provision_fixed(self, value, purpose="init"):
        user, policy = self.backend.provision(value, purpose)
        self.backend.policy_info[policy]["policyInfo"]["Policy"] = scratch.RunStorageScope.from_record(value, self.record).iam_policy()
        return user, policy

    @staticmethod
    def native_fixed_policy(value):
        # Model MinIO's native marshaled policy independently: statement/set
        # order differs, singleton sets are strings, and Sid is nonsemantic.
        root = value.scope_root.removeprefix("s3://novarocks/")
        namespace_prefix = value.run_id + "_"
        return {"Version": "2012-10-17", "Statement": [
            {"Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::warehouse",
             "Condition": {"StringLike": {"s3:prefix": ["service-mv/" + namespace_prefix + "*",
                                                         "service/" + namespace_prefix + "*"]}}},
            {"Sid": "nativeObjectScope", "Effect": "Allow",
             "Action": ["s3:PutObject", "s3:ListMultipartUploadParts", "s3:GetObjectVersion", "s3:GetObject",
                        "s3:DeleteObjectVersion", "s3:DeleteObject", "s3:AbortMultipartUpload"],
             "Resource": ["arn:aws:s3:::warehouse/service-mv/" + namespace_prefix + "*",
                          "arn:aws:s3:::novarocks/" + root + "/*",
                          "arn:aws:s3:::warehouse/service/" + namespace_prefix + "*"]},
            {"Effect": "Allow", "Action": "s3:GetBucketLocation", "Resource": "arn:aws:s3:::novarocks"},
            {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::novarocks",
             "Condition": {"StringLike": {"s3:prefix": root + "/*"}}},
            {"Sid": "", "Effect": "Allow", "Action": "s3:GetBucketLocation", "Resource": ["arn:aws:s3:::warehouse"]}]}

    def test_native_policy_set_representations_preserve_exact_fixed_authority(self):
        user, policy = self.backend.provision(self.old)
        native = self.native_fixed_policy(self.old)
        self.backend.policy_info[policy]["policyInfo"]["Policy"] = native
        self.assertEqual(self.sweeper._policy_identity(policy, self.clock.mono() + 100), (self.old, "init"))
        self.backend.objects = {self.old.scope_root + "/file"}
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertNotIn(user, self.backend.users)
        self.assertNotIn(policy, self.backend.policies)
        self.assertFalse(self.backend.objects)

    def test_native_policy_normalization_never_widens_or_ignores_unknown_authority(self):
        mutations = ("action", "resource", "prefix", "operator", "condition-key", "condition-shape", "statement-field", "top-field", "sid")
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                user, policy = self.backend.provision(self.old)
                native = self.native_fixed_policy(self.old)
                if mutation == "action": native["Statement"][1]["Action"].append("s3:PutBucketPolicy")
                elif mutation == "resource": native["Statement"][1]["Resource"].append("arn:aws:s3:::warehouse/*")
                elif mutation == "prefix": native["Statement"][0]["Condition"]["StringLike"]["s3:prefix"].append("*")
                elif mutation == "operator": native["Statement"][0]["Condition"]["StringEquals"] = native["Statement"][0]["Condition"].pop("StringLike")
                elif mutation == "condition-key": native["Statement"][0]["Condition"]["StringLike"]["s3:delimiter"] = "/"
                elif mutation == "condition-shape": native["Statement"][1]["Condition"] = []
                elif mutation == "statement-field": native["Statement"][1]["NotResource"] = "arn:aws:s3:::private/*"
                elif mutation == "top-field": native["Id"] = "unrecognized-policy-input"
                else: native["Statement"][1]["Sid"] = {"unexpected": "value"}
                self.backend.policy_info[policy]["policyInfo"]["Policy"] = native
                with self.assertRaises(runtime.RuntimeFailure) as caught:
                    self.sweeper._policy_identity(policy, self.clock.mono() + 100)
                self.assertEqual(caught.exception.code, "InvalidScratchScope")
                self.assertIn(user, self.backend.users)
                self.assertIn(policy, self.backend.policies)

    def test_catalog_barrier_reports_only_completed_child_enumeration(self):
        for failed_service, failed_suffix in (("rest", "/tables"), ("rest-mv", "/tables/t")):
            with self.subTest(service=failed_service, path=failed_suffix):
                self.setUp()
                name = self.old.resource_name("test")
                self.sweeper.rest[name] = dict(tables={"t": self.old.scope_root + "/table"}, views={})
                self.sweeper.rest_mv[name] = dict(tables={"t": self.old.scope_root + "/mv-table"}, views={})
                self.backend.objects = {self.old.scope_root + "/table/metadata", self.old.scope_root + "/mv-table/metadata"}
                original = self.sweeper._rest_request
                def unavailable(service, method, path, deadline, *, absent_ok=False):
                    if service == failed_service and method == "GET" and path == "namespaces/" + name + failed_suffix:
                        raise runtime.RuntimeFailure("ScratchRestUnavailable", "private response")
                    return original(service, method, path, deadline, absent_ok=absent_ok)
                with patch.object(self.sweeper, "_rest_request", side_effect=unavailable):
                    result = self.sweep()
                self.assertFalse(result["complete"])
                barrier = result["catalog_barrier"][self.old.run_id]
                self.assertFalse(barrier["complete"])
                self.assertFalse(barrier["services"][failed_service]["enumeration_complete"])
                self.assertEqual(barrier["services"]["rest"]["enumeration_complete"], failed_service != "rest")
                self.assertFalse(barrier["services"]["rest"]["entries_absent"])
                self.assertFalse(barrier["services"]["rest-mv"]["entries_absent"])
                self.assertEqual(len(self.backend.objects), 2)
                self.assertFalse(any(c[1] == "DELETE" for c in self.sweeper.rest_calls))

    def test_fixed_native_policy_reverse_verification_and_iam_failure_keeps_files(self):
        user, policy = self.provision_fixed(self.old)
        root = "s3://warehouse/service/" + self.old.resource_name("table")
        self.backend.objects = {root + "/file", self.old.scope_root + "/ctas/file"}
        self.backend.fail[("admin", "user", "rm", "store", user)] = "AccessDenied"
        result = self.sweep()
        self.assertFalse(result["complete"], result)
        self.assertEqual(len(self.backend.objects), 2)
        self.assertIn(policy, self.backend.policies)
        self.backend.fail.clear()
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertFalse(self.backend.objects)

    def test_broadened_fixed_native_policy_cannot_mint_an_extra_root(self):
        user, policy = self.provision_fixed(self.old)
        self.backend.policy_info[policy]["policyInfo"]["Policy"]["Statement"][-1]["Resource"].append("arn:aws:s3:::warehouse/*")
        self.backend.objects = {self.old.scope_root + "/file"}
        self.assertFalse(self.sweep()["complete"])
        self.assertIn(user, self.backend.users)
        self.assertTrue(self.backend.objects)

    def test_renamed_table_blocks_all_original_directories_until_both_catalogs_clear(self):
        old_name, new_name = self.old.resource_name("old"), self.old.resource_name("new")
        old_root = "s3://warehouse/service/" + old_name
        self.sweeper.rest[new_name] = dict(tables={"renamed": old_root + "/original"}, views={})
        self.sweeper.rest_mv[self.old.resource_name("mv")] = dict(tables={}, views={"v": self.old.scope_root + "/mv"})
        self.backend.objects = {old_root + "/original/metadata", self.old.scope_root + "/mv/metadata"}
        self.sweeper.rest_fail["DELETE", "namespaces/" + new_name] = "ScratchRestUnavailable"
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(len(self.backend.objects), 2)
        self.assertFalse(result["catalog_barrier"][self.old.run_id]["complete"])
        self.sweeper.rest_fail.clear()
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertTrue(result["catalog_barrier"][self.old.run_id]["complete"])
        self.assertFalse(self.backend.objects)
        self.assertFalse(self.sweeper.rest_mv)

    def test_other_run_completes_when_one_run_catalog_deletion_fails(self):
        peer = identity(604802, token="c" * 32)
        own, other = self.old.resource_name("bad"), peer.resource_name("good")
        self.sweeper.rest[own] = dict(tables={}, views={})
        self.sweeper.rest[other] = dict(tables={}, views={})
        self.sweeper.rest_fail["DELETE", "namespaces/" + own] = "ScratchRestUnavailable"
        self.backend.objects = {self.old.scope_root + "/file", peer.scope_root + "/file"}
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(self.backend.objects, {self.old.scope_root + "/file"})
        self.assertTrue(result["catalog_barrier"][peer.run_id]["complete"])

    def test_metadata_cannot_mint_an_undiscovered_original_run_root(self):
        name = self.old.resource_name("test")
        self.sweeper.rest[name] = dict(tables={"t": self.old.scope_root + "/table"}, views={})
        # No native S3 or IAM fact establishing that original root.
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertIn(name, self.sweeper.rest)
        self.assertFalse(any(call[1] == "DELETE" for call in self.sweeper.rest_calls))

    def test_rest_only_orphan_uses_exact_direct_child_grammar(self):
        base = "s3://warehouse/service/"
        valid = base + self.old.resource_name("gone") + "/table/file"
        unknown = {base + self.old.run_id + "/bare/file", base + self.old.run_id + "_toolong/file",
                   base + self.old.run_id + "0_test/file", "s3://warehouse/service-mv/ordinary/table/file"}
        self.backend.objects = {valid, *unknown}
        self.sweeper.rest["ordinary"] = dict(tables={}, views={})
        self.sweeper.rest[self.old.run_id] = dict(tables={}, views={})
        result = self.sweep()
        self.assertTrue(result["complete"], result)
        self.assertTrue(result["enumeration_complete"])
        self.assertEqual(result["unknown_counts"]["rest-namespace"], 2)
        self.assertEqual(self.backend.objects, unknown)
        self.assertFalse(any(c[0] == "ls" and self.old.run_id in c[-1] for c in self.backend.calls))

    def test_two_original_roots_with_same_identity_are_rejected(self):
        foreign_root = identity(604801, worktree="s3://novarocks/worktree-other").scope_root
        self.backend.objects = {self.old.scope_root + "/file", foreign_root + "/file"}
        result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertEqual(len(self.backend.objects), 2)
        self.assertFalse(any(c[0] == "rm" for c in self.backend.calls))

    def test_second_catalog_enumeration_failure_is_a_run_wide_object_barrier(self):
        root = "s3://warehouse/service/" + self.old.resource_name("gone")
        self.backend.objects = {root + "/file", self.old.scope_root + "/file"}
        original = self.sweeper._rest_request
        def unavailable(service, method, path, deadline, *, absent_ok=False):
            if service == "rest-mv":
                raise runtime.RuntimeFailure("ScratchRestUnavailable", "private response")
            return original(service, method, path, deadline, absent_ok=absent_ok)
        with patch.object(self.sweeper, "_rest_request", side_effect=unavailable):
            result = self.sweep()
        self.assertFalse(result["complete"])
        self.assertFalse(result["enumeration_complete"])
        self.assertEqual(len(self.backend.objects), 2)
        self.assertFalse(self.backend.calls)

    def test_offline_purge_requires_exact_original_generation_and_volume_absence_barrier(self):
        original = copy.deepcopy(self.record["offline_catalog_barrier"])
        for change in (None, {**original, "generation": "other"}, {**original, "complete": False},
                       {**original, "volumes": ["other-volume"]}, {**original, "daemon_id": "other"}):
            self.record["offline_catalog_barrier"] = change
            self.backend.calls.clear()
            self.backend.objects = {self.old.scope_root + "/file"}
            result = self.sweep(False)
            self.assertFalse(result["complete"])
            self.assertEqual(result["errors"][0]["code"], "ScratchCatalogBarrierRequired")
            self.assertFalse(self.backend.calls)
            self.assertTrue(self.backend.objects)
        self.record["offline_catalog_barrier"] = original
        result = self.sweep(False)
        self.assertTrue(result["complete"], result)
        self.assertFalse(self.sweeper.rest_calls)
        self.assertFalse(self.backend.objects)

    def test_normal_cleanup_uses_same_both_service_barrier_and_membership(self):
        user, policy = self.provision_fixed(self.young)
        scope = scratch.RunStorageScope.from_record(self.young, self.record)
        old_name, new_name = self.young.resource_name("old"), self.young.resource_name("new")
        root = "s3://warehouse/service/" + old_name
        self.sweeper.rest[new_name] = dict(tables={"renamed": root + "/table"}, views={})
        self.backend.objects = {root + "/table/metadata", self.young.scope_root + "/ctas/file"}
        result = self.sweeper.cleanup_run(scope, [{"user": user, "policy": policy}], self.clock.mono() + 100)
        self.assertTrue(result["complete"], result)
        self.assertTrue(result["catalog_barrier"][self.young.run_id]["complete"])
        self.assertFalse(self.backend.objects)
        self.assertFalse(self.backend.users)
        self.assertIn(("rest", "DELETE", "namespaces/" + new_name + "/tables/renamed?purgeRequested=false"), self.sweeper.rest_calls)


class ExactIamTests(unittest.TestCase):
    setUp = SweepTests.setUp
    provision_fixed = FixedCollectionTests.provision_fixed

    def scope(self):
        return scratch.RunStorageScope.from_record(self.young, self.record)

    def pair(self, purpose="init"):
        return {"user": self.young.iam_name(purpose), "policy": self.young.resource_name(purpose)}

    def test_absence_checks_only_exact_user_and_policy_info(self):
        result = self.sweeper.check_users_absent(self.scope(), [self.pair()], self.clock.mono() + 100)
        self.assertTrue(result["complete"], result)
        self.assertFalse(result["removed"])
        self.assertEqual(self.backend.calls, [
            ("admin", "user", "info", "store", self.pair()["user"]),
            ("admin", "policy", "info", "store", self.pair()["policy"]),
        ])
        self.assertFalse(self.sweeper.rest_calls)

    def test_preexisting_parent_user_or_policy_rejects_without_mutation(self):
        for present in ("both", "user-only", "policy-only"):
            with self.subTest(present=present):
                self.setUp()
                user, policy = self.provision_fixed(self.young)
                if present == "user-only":
                    self.backend.policies.clear()
                    self.backend.user_info[user]["policyName"] = "parent-readwrite"
                elif present == "policy-only":
                    self.backend.users.clear()
                users, policies = set(self.backend.users), set(self.backend.policies)
                self.backend.objects = {self.young.scope_root + "/live-data"}
                result = self.sweeper.check_users_absent(self.scope(), [self.pair()], self.clock.mono() + 100)
                self.assertFalse(result["complete"])
                self.assertEqual(result["errors"][0]["code"], "ScratchIamAlreadyExists")
                self.assertFalse(result["removed"])
                self.assertEqual(self.backend.users, users)
                self.assertEqual(self.backend.policies, policies)
                self.assertFalse(self.backend.disabled)
                self.assertTrue(self.backend.objects)
                self.assertFalse(self.sweeper.rest_calls)
                self.assertTrue(all(call[:3] in (("admin", "user", "info"), ("admin", "policy", "info"))
                                    for call in self.backend.calls))

    def test_absence_does_not_turn_unrelated_native_errors_or_empty_success_into_missing(self):
        for kind, code, expected in (
                ("user", "AccessDenied", "ScratchAccessDenied"),
                ("policy", "InternalError", "ScratchMcFailed"),
                ("user", "NoSuchBucket", "ScratchMcFailed"),
                ("user", "NoSuchPolicy", "ScratchMcFailed"),
                ("policy", "NoSuchUser", "ScratchMcFailed")):
            with self.subTest(kind=kind, code=code):
                self.setUp()
                name = self.pair()[kind]
                self.backend.fail[("admin", kind, "info", "store", name)] = code
                result = self.sweeper.check_users_absent(self.scope(), [self.pair()], self.clock.mono() + 100)
                self.assertFalse(result["complete"])
                self.assertEqual(result["errors"][0]["code"], expected)
                self.assertFalse(result["removed"])
        self.setUp()
        self.backend.raw = "\nNOVA_SCRATCH_MC_EXIT=0"
        result = self.sweeper.check_users_absent(self.scope(), [self.pair()], self.clock.mono() + 100)
        self.assertFalse(result["complete"])
        self.assertEqual(result["errors"][0]["code"], "InvalidMcResponse")

    def test_iam_only_cleanup_preserves_catalogs_and_objects_and_exact_peer_iam(self):
        pairs = []
        for purpose in ("init", "rot"):
            user, policy = self.provision_fixed(self.young, purpose)
            pairs.append(dict(user=user, policy=policy))
        peer_user, peer_policy = self.provision_fixed(self.old)
        objects = {self.young.scope_root + "/live-data", self.old.scope_root + "/live-data"}
        self.backend.objects = set(objects)
        catalogs = {self.young.resource_name("test"): dict(tables={"live": self.young.scope_root}, views={})}
        self.sweeper.rest = copy.deepcopy(catalogs)
        self.sweeper.rest_mv = copy.deepcopy(catalogs)
        result = self.sweeper.cleanup_users(self.scope(), pairs, self.clock.mono() + 100)
        self.assertTrue(result["complete"], result)
        self.assertEqual(self.backend.users, {peer_user})
        self.assertEqual(self.backend.policies, {peer_policy})
        self.assertEqual(self.backend.objects, objects)
        self.assertEqual(self.sweeper.rest, catalogs)
        self.assertEqual(self.sweeper.rest_mv, catalogs)
        self.assertFalse(self.sweeper.rest_calls)
        self.assertFalse(any(call[0] in {"ls", "rm"} or call[2] == "list" for call in self.backend.calls))
        self.assertTrue(all(item["kind"] in {"user", "policy"} for item in result["removed"]))
        self.assertTrue(self.sweeper.cleanup_users(self.scope(), pairs, self.clock.mono() + 100)["complete"])

    def test_exact_iam_pair_validation_precedes_all_remote_calls(self):
        peer = {"user": self.old.iam_name("init"), "policy": self.old.resource_name("init")}
        for operation in ("check_users_absent", "cleanup_users"):
            for pairs in (None, [self.pair(), peer], [self.pair(), self.pair()],
                          [dict(user=self.pair()["user"], policy=self.pair("rot")["policy"])],
                          [dict(self.pair(), extra="unadmitted")]):
                with self.subTest(operation=operation, pairs=pairs):
                    self.setUp()
                    self.provision_fixed(self.young)
                    result = getattr(self.sweeper, operation)(self.scope(), pairs, self.clock.mono() + 100)
                    self.assertFalse(result["complete"])
                    self.assertEqual(result["errors"][0]["code"], "ScratchIamUnmapped")
                    self.assertFalse(self.backend.calls)
                    self.assertFalse(self.sweeper.rest_calls)
                    self.assertFalse(self.backend.disabled)

    def test_iam_cleanup_rejects_foreign_bindings_or_native_errors_before_mutation(self):
        for change in ({"policyName": "parent-readwrite"}, {"memberOf": ["parent-group"]}):
            with self.subTest(change=change):
                self.setUp()
                user, policy = self.provision_fixed(self.young)
                self.backend.user_info[user].update(change)
                result = self.sweeper.cleanup_users(self.scope(), [self.pair()], self.clock.mono() + 100)
                self.assertFalse(result["complete"])
                self.assertEqual(result["errors"][0]["code"], "ScratchIamUnmapped")
                self.assertEqual(self.backend.users, {user})
                self.assertEqual(self.backend.policies, {policy})
                self.assertFalse(self.backend.disabled)
        for kind, code in (("user", "NoSuchPolicy"), ("policy", "NoSuchUser")):
            with self.subTest(kind=kind, code=code):
                self.setUp()
                self.provision_fixed(self.young)
                self.backend.fail[("admin", kind, "info", "store", self.pair()[kind])] = code
                result = self.sweeper.cleanup_users(self.scope(), [self.pair()], self.clock.mono() + 100)
                self.assertFalse(result["complete"])
                self.assertEqual(result["errors"][0]["code"], "ScratchMcFailed")
                self.assertFalse(self.backend.disabled)
                self.assertFalse(self.sweeper.rest_calls)

    def test_iam_ports_recheck_scope_and_deadline_without_remote_effects(self):
        for operation in ("check_users_absent", "cleanup_users"):
            for expired in (False, True):
                with self.subTest(operation=operation, expired=expired):
                    self.setUp()
                    scope = self.scope()
                    if not expired:
                        self.record["generation"] = "replacement-generation"
                    deadline = self.clock.mono() + (0 if expired else 100)
                    result = getattr(self.sweeper, operation)(scope, [self.pair()], deadline)
                    self.assertFalse(result["complete"])
                    self.assertFalse(self.backend.calls)
                    self.assertFalse(self.sweeper.rest_calls)


class RestTransportTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.sweeper = scratch.ScratchSweeper(record(), Backend(self.clock), wall_clock=self.clock.wall,
                                               monotonic=self.clock.mono)

    def test_http_not_found_is_idempotent_but_forbidden_remains_typed(self):
        for status, expected in ((404, None), (403, "ScratchAccessDenied"), (503, "ScratchRestFailed")):
            with patch.object(scratch, "_DeadlineConnection") as factory:
                factory.return_value.getresponse.return_value.status = status
                if expected:
                    with self.assertRaises(runtime.RuntimeFailure) as caught:
                        self.sweeper._rest_request("rest", "DELETE", "namespaces/test", 200, absent_ok=True)
                    self.assertEqual(caught.exception.code, expected)
                else:
                    self.assertIsNone(self.sweeper._rest_request("rest", "DELETE", "namespaces/test", 200, absent_ok=True))
                factory.return_value.close.assert_called_once()

    def test_socket_makefile_reads_cannot_reset_original_deadline(self):
        receiving, sending = socket.socketpair()
        bounded = scratch._DeadlineSocket(receiving, 102, self.clock.mono)
        self.addCleanup(bounded.close)
        self.addCleanup(sending.close)
        stream = bounded.makefile("rb", buffering=0)
        self.addCleanup(stream.close)
        sending.sendall(b"a")
        self.assertEqual(stream.read(1), b"a")
        self.clock.advance(3)
        sending.sendall(b"b")
        with self.assertRaises(TimeoutError):
            stream.read(1)

    def test_repeating_pagination_token_is_rejected(self):
        with patch.object(self.sweeper, "_rest_request", return_value={"namespaces": [], "next-page-token": "same"}):
            with self.assertRaises(runtime.RuntimeFailure) as caught:
                list(self.sweeper._pages("rest", "namespaces", "namespaces", 200))
        self.assertEqual(caught.exception.code, "InvalidRestResponse")


if __name__ == "__main__":
    unittest.main()
