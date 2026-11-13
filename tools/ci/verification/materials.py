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
"""Read producer-owned raw materials; never repair, rewrite, or infer history.

All paths refer to retained materials. Missing or unsupported facts remain
coverage gaps. A complete execution domain does not require a complete batch.
"""
import hashlib
import json
from pathlib import Path
import xml.etree.ElementTree as ET

from .identity import confirmed, read_scope_identity, store_only_tool_config
from .model import Attempt, Observation, Scope, UnitKey
from .scope import default_scope


class MaterialError(ValueError):
    pass


def read_json(path, schema=1, schema_field="schema"):
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError) as error:
        raise MaterialError("cannot read {}: {}".format(path, error)) from error
    if not isinstance(value, dict) or value.get(schema_field) != schema:
        raise MaterialError("unsupported schema in " + str(path))
    return value


def _path(root, ref):
    value = ref.get("path") if isinstance(ref, dict) else ref
    if not value:
        raise MaterialError("missing material reference")
    path = Path(value)
    return path if path.is_absolute() else Path(root) / path


def _verified_path(root, ref):
    path = _path(root, ref)
    if isinstance(ref, dict) and ref.get("exists") is False:
        raise MaterialError("material did not exist at checkpoint: " + str(path))
    if isinstance(ref, dict) and ref.get("sha256"):
        if hashlib.sha256(path.read_bytes()).hexdigest() != ref["sha256"]:
            raise MaterialError("material digest differs: " + str(path))
    return path


def _read_ref(root, ref, schema=1):
    path = _verified_path(root, ref)
    return read_json(path, schema)


def _scope_for(unit, declarations, declaration=None):
    return next((scope.key for scope in declarations if unit in scope.units),
                default_scope(unit, declaration))


def resource_cleanup(resources):
    if not isinstance(resources, list):
        return "unknown"
    states = [item.get("state") for item in resources]
    if "cleanup_failed" in states:
        return "violation"
    return "ok" if all(state == "released" for state in states) else "unknown"


def read_monitor_cleanup(path):
    """Read G6a summary or native monitor events, including pretty JSON."""
    path = Path(path)
    try:
        raw = path.read_bytes()
        if not raw.endswith(b"\n"):
            return "unknown"
        try:
            summary = json.loads(raw)
        except ValueError:
            summary = None
        if isinstance(summary, dict) and "ok" in summary:
            if summary.get("schema") != 1:
                return "unknown"
            if (summary.get("errors") or summary.get("late_registrations")
                    or any(item.get("violations") for item in summary.get("records", []))):
                return "violation"
            return "ok" if (summary.get("ok") is True and summary.get("complete") is True
                            and summary.get("registration_closed") is True) else "unknown"
        events = [json.loads(line) for line in raw.splitlines()]
    except (OSError, UnicodeError, ValueError, TypeError):
        return "unknown"
    if (not events or not all(isinstance(event, dict) and event.get("schema") == 1 for event in events)
            or events[0].get("event") != "Pending"):
        return "unknown"
    final = events[-1]
    required = {"status_raw", "startup_error", "owner_lost", "errors", "cleanup_complete"}
    if (final.get("event") != "Final" or not required.issubset(final)
            or not isinstance(final["errors"], list) or type(final["cleanup_complete"]) is not bool):
        return "unknown"
    if (final["errors"] or final["cleanup_complete"] is False
            or any(event.get("event") == "CleanupIncomplete" for event in events)
            or path.with_name("caller-drop-error.txt").exists()):
        return "violation"
    return "ok"


def system_cleanup(teardown, monitors=()):
    """Only real cleanup failures are violations; launch failure is execution."""
    if not teardown:
        return "unknown"
    if teardown.get("schema") != 1:
        return "unknown"
    fixture, cluster = teardown.get("fixture", {}), teardown.get("cluster", {})
    resources = resource_cleanup(fixture.get("resources"))
    if (resources == "violation" or fixture.get("teardown", {}).get("status") == "failed"
            or cluster.get("shutdown") == "failed" or "violation" in monitors):
        return "violation"
    if (resources == "unknown" or (cluster.get("partial_cleanup") == "monitor"
                                  and (not monitors or any(value != "ok" for value in monitors)))
            or fixture.get("teardown", {}).get("status") not in {"ok", "not_applicable"}
            or cluster.get("shutdown") not in {"ok", "not_applicable"}):
        return "unknown"
    return "ok"


def _sql_case_cleanup_failed(failure):
    """Read explicit producer cleanup facts only, including aggregate failures."""
    pending = [failure]
    while pending:
        current = pending.pop()
        if not isinstance(current, dict):
            continue
        if current.get("cleanup") is True:
            return True
        pending.append(current.get("primary_failure"))
        additional = current.get("additional_failures")
        if isinstance(additional, list):
            pending.extend(additional)
    return False


def sql_cleanup(records):
    fixtures = [value for value in records if value.get("kind") == "fixture"]
    servers = [value for value in records if value.get("kind") == "server"]
    selection = any(value.get("kind") == "selection" for value in records)
    if not fixtures:
        return "unknown"
    fixture = fixtures[-1]
    resources = resource_cleanup(fixture.get("resources"))
    if (resources == "violation" or fixture.get("shutdown", {}).get("status") == "failed"
            or any(value.get("cleanup", {}).get("status") == "failed" for value in servers)):
        return "violation"
    if resources == "unknown" or (selection and not servers):
        return "unknown"
    if fixture.get("shutdown", {}).get("status") not in {"ok", "not_called"}:
        return "unknown"
    if servers and servers[-1].get("cleanup", {}).get("status") not in {"ok", "external", "not_launched"}:
        return "unknown"
    return "ok"


def platform_recorded(value):
    return (isinstance(value, dict) and
            all(isinstance(value.get(key), str) and value[key].strip()
                for key in ('system', 'machine')))


def sql_scope_identity(run, selection, suite):
    # Absolute program, output, report and retention paths are material facts.
    # Only content and effective execution policy belong to execution identity.
    fixture = selection.get("fixture", {})
    return {"platform": run.get("platform"),
            "runner_sha256": run.get("runner", {}).get("sha256"),
            "server_sha256": run.get("server", {}).get("sha256"),
            "server_launched": run.get("server", {}).get("launched"),
            "config_sha256": run.get("config", {}).get("sha256"),
            "fixture_env_sha256": fixture.get("env_file_sha256"),
            "fixture_runtime": fixture.get("isolated_runtime"),
            "datasets": fixture.get("datasets", []), "topology": run.get("topology"),
            "mode": run.get("mode"), "verification": run.get("verification"),
            "suite_verify_enabled": suite.get("verify_enabled"),
            "suite_query_timeout_secs": suite.get("query_timeout_secs"),
            "suite_inputs_sha256": suite.get("inputs_sha256"), "env": suite.get("env"),
            "declaration": suite.get("declaration")}


SQL_ORACLES = {"result_set", "expected_error", "explain_contains", "explain_not_contains",
               "be_log", "runtime_lifecycle", "text_assertions", "imv_equivalence",
               "imv_stateless_rebuild", "mv_rest_document_graph", "iceberg_orphan_absent",
               "query_stats_contract"}


def sql_obligation(run, case, suite=None):
    verification = run.get("verification", {})
    if (run.get("mode") != "verify" or verification.get("verify_enabled") is not True
            or verification.get("update_expected")
            or verification.get("float_epsilon") is not None
            or verification.get("order_sensitive_default")
            or verification.get("rewrite_explain_contains_as_not_contains")
            or verification.get("target_session_sql") or verification.get("dry_run")):
        return False
    if suite is not None and suite.get("verify_enabled") is not True:
        return False
    oracle = case.get("oracle", {})
    if not isinstance(oracle.get("required"), list) or not isinstance(oracle.get("executed"), list):
        return False
    if any(value.get("kind") not in SQL_ORACLES for value in oracle["required"] + oracle["executed"]):
        return False
    if any(value.get("policy", {}).get("explain_rewritten") for value in oracle["executed"]):
        return False
    # On a product failure later oracles may be unreachable. This remains a
    # canonical counterexample, whereas a passing record must prove each oracle.
    if case.get("status") == "pass":
        for required in oracle["required"]:
            actual = [value for value in oracle["executed"]
                      if (value.get("step"), value.get("kind")) == (required.get("step"), required.get("kind"))]
            if len(actual) != 1 or actual[0].get("outcome") != "pass":
                return False
    return True


def read_sql(path, attempt_id, declarations=(), invocation=None):
    """Return (observations, errors), preserving selected but unexecuted cases."""
    path = Path(path)
    records, errors = [], []
    try:
        raw = path.read_bytes()
    except OSError as error:
        return [], [str(error)]
    lines = raw.splitlines(keepends=True)
    for number, line in enumerate(lines, 1):
        if not line.endswith(b"\n"):
            errors.append("unfinished SQL record {}:{}".format(path, number))
            break
        try:
            value = json.loads(line)
            if not isinstance(value, dict):
                raise ValueError("record is not an object")
            records.append(value)
        except (UnicodeError, ValueError) as error:
            errors.append("invalid SQL record {}:{}: {}".format(path, number, error))
            break
    run = records[0] if records and records[0].get("kind") == "run" else {}
    schema_ok = run.get("schema") == 1
    if not schema_ok:
        errors.append("unsupported or missing SQL run schema")
    selections = [value for value in records if value.get("kind") == "selection"]
    selection = selections[0] if len(selections) == 1 else {}
    for field in ("unresolved_only", "unresolved_skip"):
        if selection.get(field):
            errors.append("unresolved SQL selection {}: {}".format(field, selection[field]))
    cleanup = sql_cleanup(records)
    observations = []
    suites = selection.get("suites", [])
    for suite in suites:
        if (not isinstance(suite, dict) or not isinstance(suite.get("suite"), str)
                or not isinstance(suite.get("cases"), list)
                or any(not isinstance(case, dict) or not isinstance(case.get("case"), str)
                       for case in suite.get("cases", []))):
            errors.append("invalid SQL suite selection")
            continue
        name = suite["suite"]
        declaration = suite.get("declaration") or {}
        identity = sql_scope_identity(run, selection, suite)
        cases = [value for value in records if value.get("kind") == "case" and value.get("suite") == name]
        closure = {(value.get("suite"), value.get("lifecycle_id")): value for value in records if value.get("kind") == "suite"}
        by_case = {}
        for value in cases:
            by_case.setdefault(value.get("case"), []).append(value)
        for selected in suite.get("cases", []):
            unit = UnitKey("sql-case", name + "/" + selected["case"])
            values = by_case.get(selected["case"], []) or [{}]
            for value in values:
                lifecycle = value.get("lifecycle_id", "unexecuted:" + name)
                end = closure.get((name, lifecycle), {})
                status = value.get("status", "not_run")
                if status not in {"pass", "fail", "not_run"}:
                    status = "unknown"
                scope_cleanup = ("violation" if (
                    end.get("cleanup", {}).get("status") == "failed"
                    or _sql_case_cleanup_failed(value.get("failure"))) else cleanup)
                if end.get("cleanup", {}).get("status") not in {"ok", "failed"} and scope_cleanup != "violation":
                    scope_cleanup = "unknown"
                stable = (end.get("inputs_stable") is True
                          and end.get("inputs_end_sha256") == suite.get("inputs_sha256")
                          and suite.get("inputs_sha256") is not None)
                if invocation is not None:
                    stable = stable and confirmed(invocation)
                failure = value.get("failure") or {}
                actually_executed = (status in {"pass", "fail"}
                                     and value.get("reason") not in {"suite_init_failed", "load_failed"}
                                     and failure.get("class") != "load")
                complete = bool(end and selected["case"] in end.get("executed_cases", []) and actually_executed)
                observations.append(Observation(unit, _scope_for(unit, declarations, declaration), attempt_id,
                    lifecycle, identity, status, stable, complete and schema_ok and not errors,
                    actually_executed, scope_cleanup,
                    "canonical" if schema_ok and platform_recorded(run.get("platform"))
                    and sql_obligation(run, value, suite) else "diagnostic",
                    value.get("failure"), str(path) + "#" + name + "/" + selected["case"],
                    declaration.get("env_inputs") is not None,
                    {"oracle": value.get("oracle"), "reason": value.get("reason"), "raw": value}))
    if not selections and invocation is not None:
        for native in invocation.get("units", []):
            unit = UnitKey.parse(native)
            observations.append(Observation(unit, _scope_for(unit, declarations), attempt_id,
                str(invocation.get("seq", "unselected")), invocation.get("inputs_start"),
                "not_run", False, False, False, cleanup, "invalid", None, str(path), False,
                {"reason": "selection was not reached", "raw": records}))
    # Fixture startup may fail before selection. The caller's saved selection
    # still defines required units; no phantom execution is invented here.
    return observations, errors


def _rust_cleanup(document):
    if document is None:
        return "unknown"
    if (document.get("errors") or document.get("late_registrations")
            or any(item.get("violations") for item in document.get("records", []))):
        return "violation"
    return "ok" if document.get("complete") is True and document.get("ok") is True else "unknown"


def read_rust(root, attempt_id, declarations=()):
    root = Path(root)
    errors, observations = [], []
    try:
        index = read_json(root / "execution-index.json")
    except MaterialError as error:
        return [], [str(error)]
    materials = index.get("materials", {})
    try:
        inputs = _path(root, materials.get("inputs"))
        selected = json.loads(_verified_path(root, materials["selection"]).read_text())
        prepared = read_json(inputs / "prepared.json")
        bindings = _read_ref(root, materials["test_bindings"])
        tool = _verified_path(root, materials["tool_config"]).read_text()
        tool_ok = store_only_tool_config(tool)
        if not tool_ok:
            errors.append("nextest tool config contains undeclared execution settings")
    except (MaterialError, OSError, ValueError, KeyError) as error:
        return [], [str(error)]
    for domain in ("nextest", "doctest"):
        stage = index.get("stages", {}).get(domain, {})
        code = stage.get("returncode")
        terminated = code is not None and stage.get("started") is not False
        try:
            identity_doc = read_json(root / ("identity/" + domain + ".json"))
        except MaterialError as error:
            identity_doc = {}
            errors.append(str(error))
        try:
            cleanup_doc = _read_ref(root, materials["rust_cleanup" if domain == "nextest" else "doctest_cleanup"])
        except (MaterialError, OSError, KeyError) as error:
            cleanup_doc = None
            if terminated:
                errors.append(str(error))
        cleanup = _rust_cleanup(cleanup_doc)
        complete = bool(terminated and not stage.get("errors") and cleanup_doc
                        and cleanup_doc.get("complete") is True)
        if domain == "doctest":
            unit = UnitKey("rust-doctest", "workspace")
            try:
                identity, stable, declared = read_scope_identity(identity_doc, "workspace")
            except ValueError:
                identity, stable, declared = None, False, False
            result = "pass" if code == 0 else "fail" if terminated else "not_run"
            if code is not None and (code < 0 or 129 <= code <= 192):
                result = "cancelled"
            observations.append(Observation(unit, _scope_for(unit, declarations), attempt_id, "doctest",
                identity, result, stable, complete, terminated, cleanup, "canonical", None,
                str(root / "execution-index.json") + "#stages.doctest", declared,
                {"batch_root": str(root.resolve())}))
            continue
        junit_cases = {}
        report = materials.get("junit", {})
        try:
            tree = ET.parse(_verified_path(root, report))
            for suite in tree.iter("testsuite"):
                for case in suite.findall("testcase"):
                    binary = case.get("classname") or suite.get("name")
                    key = binary + "::" + case.get("name", "")
                    if key in junit_cases:
                        raise MaterialError("duplicate native testcase in JUnit")
                    junit_cases[key] = case
        except (OSError, ET.ParseError, MaterialError, TypeError) as error:
            errors.append(str(error))
            complete = False
        complete = complete and report.get("complete") is True and tool_ok
        for binary_id, suite in selected.get("rust-suites", {}).items():
            for name, case in suite.get("testcases", {}).items():
                if case.get("filter-match", {}).get("status") != "matches":
                    continue
                native = binary_id + "::" + name
                unit = UnitKey("rust-test", native)
                try:
                    identity, stable, declared = read_scope_identity(identity_doc, native)
                except ValueError:
                    identity, stable, declared = None, False, False
                bound = bindings.get("binaries", {}).get(binary_id, {})
                stable = (stable and bound.get("sha256") is not None and identity is not None
                          and identity.get("executable_sha256") == bound.get("sha256")
                          and identity.get("nextest_profile") == prepared.get("profile")
                          and identity.get("nextest_config_sha256") == prepared.get("config_sha256"))
                raw = junit_cases.get(native)
                result = "not_run"
                if raw is not None:
                    result = "fail" if raw.find("failure") is not None or raw.find("error") is not None else "not_run" if raw.find("skipped") is not None else "pass"
                raw_result = result
                if not terminated and result == "pass":
                    result = "not_run"
                if code is not None and (code < 0 or 129 <= code <= 192) and result == "not_run":
                    result = "cancelled"
                observations.append(Observation(unit, _scope_for(unit, declarations), attempt_id, "nextest",
                    identity, result, stable, complete and raw is not None, terminated and result in {"pass", "fail"},
                    cleanup, "canonical" if tool_ok else "diagnostic", None,
                    str(_path(root, report)) + "#" + native if report else str(root / "execution-index.json"), declared,
                    {"batch_root": str(root.resolve()), "batch_complete": index.get("complete"), "domain_returncode": code, "raw_result": raw_result}))
    return observations, errors


def read_invocation(path, attempt_id, declarations=()):
    path = Path(path)
    try:
        invocation = read_json(path)
    except MaterialError as error:
        return [], [str(error)]
    kind = invocation.get("kind")
    if kind not in {"sql-suite", "system-scenario", "stage"}:
        return [], ["unsupported invocation kind"]
    complete = isinstance(invocation.get("exit", {}).get("code"), int) and "ended_at" in invocation
    if kind == "sql-suite":
        ref = invocation.get("materials", {}).get("sql_results")
        if ref:
            return read_sql(_path(path.parent, ref), attempt_id, declarations, invocation)
    units = [UnitKey.parse(value) for value in invocation.get("units", [])]
    observations, errors = [], []
    code = invocation.get("exit", {}).get("code")
    cleanup = invocation.get("cleanup", {}).get("summary", "unknown")
    evidence = None
    if kind == "system-scenario":
        try:
            evidence = read_json(_path(path.parent, invocation.get("materials", {}).get("evidence")), 5, "schema_version")
            if any(unit.kind == "system-scenario" and unit.id != evidence.get("scenario") for unit in units):
                raise MaterialError("scenario evidence belongs to a different native unit")
        except (MaterialError, OSError) as error:
            errors.append(str(error))
            evidence = None
        # Recompute producer cleanup rather than trusting a synthetic exit code.
        teardown_ref = invocation.get("materials", {}).get("teardown")
        if teardown_ref:
            try:
                monitors = [read_monitor_cleanup(_path(path.parent, ref))
                            for ref in invocation.get("materials", {}).get("monitors", [])]
                cleanup = system_cleanup(read_json(_path(path.parent, teardown_ref)), monitors)
            except MaterialError as error:
                errors.append(str(error))
                cleanup = "unknown"
        elif invocation.get("cleanup", {}).get("source") != "producer":
            cleanup = "unknown"
    for unit in units:
        result = "not_run" if not complete else "pass" if code == 0 else "fail"
        if code is not None and (code < 0 or 129 <= code <= 192):
            result = "cancelled"
        if kind == "system-scenario":
            if evidence is None and result != "cancelled":
                result = "unknown"
            elif evidence and evidence.get("outcome") == "failed":
                result = "fail"
            elif result == "pass" and not (evidence and evidence.get("outcome") == "passed"):
                result = "unknown"
        observations.append(Observation(unit, _scope_for(unit, declarations), attempt_id,
            str(invocation.get("seq", path.stem)), invocation.get("inputs_start"), result,
            confirmed(invocation), complete and not errors, complete,
            cleanup if cleanup in {"ok", "violation", "unknown"} else "unknown",
            "canonical" if platform_recorded((invocation.get("inputs_start") or {}).get("platform")) else "diagnostic", None,
            str(path), invocation.get("inputs_declared", True), {"invocation": invocation, "evidence": evidence}))
    if kind == "stage" and any(unit.id == "workspace nextest and doctests" for unit in units):
        index = {}
        try:
            index_path = _path(path.parent, invocation.get("materials", {}).get("execution_index"))
            if path.parent.parent.resolve() not in index_path.resolve().parents:
                raise MaterialError("Rust batch index is outside its owning attempt")
            index = read_json(index_path)
        except (MaterialError, OSError) as error:
            errors.append(str(error))
        for observation in observations:
            observation.detail["batch_index"] = index
            observation.detail["batch_root"] = str(index_path.parent.resolve()) if index else None
        internal = {"prepare": "joint tests and binaries preparation",
                    "error_manifest": "SQL error manifest freshness",
                    "final_prepare": "main binary build information"}
        for native, name in internal.items():
            stage = index.get("stages", {}).get(native, {})
            native_code = stage.get("returncode")
            done = isinstance(native_code, int) and stage.get("started") is not False
            unit = UnitKey("stage", name)
            observations.append(Observation(unit, _scope_for(unit, declarations), attempt_id,
                str(invocation.get("seq", path.stem)) + ":" + native,
                invocation.get("inputs_start"), "not_run" if not done else "pass" if native_code == 0 and not stage.get("errors") else "fail",
                confirmed(invocation), done, done,
                cleanup if cleanup in {"ok", "violation", "unknown"} else "unknown",
                "canonical" if platform_recorded((invocation.get("inputs_start") or {}).get("platform")) else "diagnostic",
                None, str(index_path) + "#stages." + native if index else str(path), True,
                {"native_stage": native, "raw": stage, "aggregate_invocation": str(path)}))
    return observations, errors


def read_attempt(path, declarations=(), current=False, formal=False):
    root = Path(path)
    declarations = [Scope.parse(value) for value in declarations]
    attempt = Attempt(root.name, current=current, formal=formal, root=str(root))
    source_path = root / "identity/source-start.json"
    if source_path.exists():
        try:
            attempt.source = read_json(source_path).get("source", {})
        except MaterialError as error:
            attempt.errors.append(str(error))
    rust = root / "rust"
    if (rust / "execution-index.json").exists():
        obs, errors = read_rust(rust, attempt.id, declarations)
        attempt.observations.extend(obs)
        attempt.errors.extend(errors)
    final = sorted((root / "invocations").glob("*.json"))
    final_paths = set(final)
    for invocation in final:
        if invocation.name.endswith(".started.json") and invocation.with_name(invocation.name.replace(".started.json", ".json")) in final_paths:
            continue
        obs, errors = read_invocation(invocation, attempt.id, declarations)
        attempt.observations.extend(obs)
        attempt.errors.extend(errors)
    return attempt
