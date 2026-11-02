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
"""Admission membership and current-attempt composition; raw evidence is read-only."""
import copy
import json
from pathlib import Path
import re
import subprocess

from .model import Scope, UnitKey
from .declarations import native_stage_mode
from .record import write_record

KINDS = {"stage", "rust-test", "rust-doctest", "sql-suite", "system-scenario"}


def _names(value, kind):
    if value == "all" and kind in {"rust-test", "rust-doctest"}:
        return value
    if (not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value)
            or len(set(value)) != len(value)):
        raise ValueError("admission names must be a unique list; all is only valid for Rust kinds: " + kind)
    return value


def scope_name(scope):
    kind, name = scope.key.split(":", 1)
    if kind == "sql-case":
        return "sql-suite", name.split("/", 1)[0]
    # Stage mode suffixes belong to the unit identity, not native stage names.
    if kind == "stage" and scope.units[0].mode != "default":
        name = scope.units[0].id
    return kind, name


class Enablement:
    def __init__(self, document):
        if (not isinstance(document, dict) or type(document.get("schema")) is not int or document.get("schema") != 1
                or set(document) != {"schema", "enabled", "pending", "enumeration_gaps"}):
            raise ValueError("unsupported admission enablement schema or fields")
        enabled = document["enabled"]
        if not isinstance(enabled, dict) or set(enabled) != KINDS:
            raise ValueError("admission enabled must declare exactly the supported kinds")
        self.enabled = {kind: _names(value, kind) for kind, value in enabled.items()}
        self.pending = copy.deepcopy(document["pending"])
        self.enumeration_gaps = copy.deepcopy(document["enumeration_gaps"])
        if not isinstance(self.pending, list) or not isinstance(self.enumeration_gaps, list):
            raise ValueError("admission pending and enumeration_gaps must be lists")
        for item in self.pending:
            if not isinstance(item, dict) or set(item) != {"selector", "owner", "enable_when"}:
                raise ValueError("invalid admission pending entry")
            if any(not isinstance(item[key], str) or not item[key].strip() for key in ("owner", "enable_when")):
                raise ValueError("admission pending requires owner and enable_when")
            selector = item["selector"]
            if (not isinstance(selector, dict) or not isinstance(selector.get("kind"), str) or selector.get("kind") not in KINDS
                    or set(selector) - {"kind", "names"}):
                raise ValueError("invalid admission pending selector")
            kind = selector["kind"]
            names = _names(selector["names"], kind) if "names" in selector else None
            active = self.enabled[kind]
            if active and (active == "all" or names is None or names == "all" or set(active) & set(names)):
                raise ValueError("admission enabled overlaps pending: " + kind)
        for item in self.enumeration_gaps:
            if (not isinstance(item, dict) or set(item) != {"name", "kind", "reason", "closes_with"}
                    or not isinstance(item.get("kind"), str) or item.get("kind") not in KINDS
                    or any(not isinstance(item[key], str) or not item[key].strip()
                           for key in ("name", "reason", "closes_with"))):
                raise ValueError("invalid admission enumeration gap")

    def validate(self, declarations, unavailable=()):
        names = {kind: set() for kind in KINDS}
        for value in declarations:
            kind, name = scope_name(Scope.parse(value))
            if kind in names:
                names[kind].add(name)
        for kind, selected in self.enabled.items():
            if kind not in unavailable and selected != "all" and set(selected) - names[kind]:
                raise ValueError("unknown admission enabled names for " + kind + ": " + ", ".join(sorted(set(selected) - names[kind])))
        for entry in self.pending:
            selector = entry["selector"]
            kind = selector["kind"]
            selected = selector.get("names", "all")
            if kind not in unavailable and selected != "all" and set(selected) - names[kind]:
                raise ValueError("unknown admission pending names for " + kind)
        for entry in self.enumeration_gaps:
            if entry["name"] in names[entry["kind"]]:
                raise ValueError("enumeration gap is now natively declared: " + entry["name"])
        return self

    def covers(self, scope):
        kind, name = scope_name(Scope.parse(scope))
        selected = self.enabled.get(kind, [])
        return selected == "all" or name in selected

    def reason(self, scope):
        kind, name = scope_name(Scope.parse(scope))
        for item in self.pending:
            selector = item["selector"]
            if selector["kind"] == kind and ("names" not in selector or selector["names"] == "all" or name in selector["names"]):
                return "pending"
        return "no_plan"


def load_enablement(path, declarations=None):
    path = Path(path)
    if not path.exists():
        return None
    value = Enablement(json.loads(path.read_text(encoding="utf-8")))
    return value.validate(declarations) if declarations is not None else value


def compose_attempt(repo, attempt_dir, declaration_path, run_identity, output_dir,
                    evidence_class="local-rehearsal", *, declarations=None, native_exit=None):
    """Compose one native attempt, retaining a blocking report on enumeration failure.

    ``declarations`` is an injection point for owner-local synthetic tests. Normal
    admission obtains a fresh native preparation and listing unless a failed
    native invocation never reached the Rust batch. That case retains stage
    facts and explicit enumeration gaps without building merely to report failure.
    Rehearsal uses admission membership without granting formal authority.
    """
    from . import run
    from .compose import compose
    from .dispositions import Dispositions
    from .identity import same_identity
    from .materials import read_json
    if evidence_class not in {"formal-admission", "local-rehearsal", "diagnostic"}:
        raise ValueError("unknown admission evidence class")
    repo, output_dir = Path(repo).resolve(), Path(output_dir).resolve()
    enablement = load_enablement(declaration_path)
    unavailable, enumeration_errors = [], []
    if declarations is None:
        try:
            if (native_exit is not None and native_exit != 0
                    and not (Path(attempt_dir) / "rust/execution-index.json").is_file()):
                raise ValueError("native Rust batch was not reached")
            inputs = run.prepare(repo, output_dir)
            native = run.inventory(repo, inputs)
            chosen = {"rust_filterset": None, "doctest": True, "sql_suites": [], "system_scenarios": [], "stages": []}
            declarations = run.scopes(repo, native, chosen)
        except (OSError, ValueError, KeyError, subprocess.SubprocessError) as error:
            enumeration_errors.append(str(error))
            unavailable = ["rust-test", "rust-doctest", "sql-suite", "system-scenario"]
            declarations = []
            # Stage registration does not depend on a successful candidate build.
            try:
                native_stages = run.json_command(["bash", str(repo / "tools/ci/local-full-ci.sh"), "--list-stages"], repo)
                for item in native_stages["stages"]:
                    mode = native_stage_mode(item)
                    unit = UnitKey("stage", item["name"], mode)
                    declarations.append(Scope(str(unit), (unit,), tier="heavy" if item["tier"] == "full" else "regular"))
            except (OSError, ValueError, KeyError, subprocess.SubprocessError) as stage_error:
                enumeration_errors.append(str(stage_error))
                unavailable.append("stage")
    declarations = [Scope.parse(value) for value in declarations]
    if enablement:
        enablement.validate(declarations, unavailable)
    attempt = run.owned_attempt(attempt_dir, declarations, current=True)
    formal = evidence_class == "formal-admission"
    attempt.formal = formal
    if formal:
        source = attempt.source
        if (source.get("dirty") is not False or not isinstance(source.get("commit"), str) or not re.fullmatch(r"[0-9a-f]{40}", source.get("commit", ""))
                or source.get("commit") != run_identity.get("head_sha")):
            raise ValueError("formal admission requires clean source at the exact full head SHA")
        end_path = Path(attempt_dir) / "identity/source-end.json"
        if not end_path.is_file():
            raise ValueError("formal admission requires the final source record")
        if not same_identity(source, read_json(end_path).get("source")):
            raise ValueError("formal admission source changed during the attempt")
    identities = {}
    for observation in attempt.observations:
        if observation.identity_confirmed:
            key = str(observation.unit)
            if key in identities and not same_identity(identities[key], observation.identity):
                raise ValueError("conflicting confirmed identities for current admission unit: " + key)
            identities[key] = observation.identity
    target = {"identities": identities, "current_attempt": attempt.id}
    value = compose(target, "admission", [attempt], declarations,
                    Dispositions.read(repo / "tools/ci/baselines/dispositions.json"),
                    run.confirmations(attempt_dir), enablement, admission_current=not formal)
    value["evidence_class"] = evidence_class
    if formal:
        value["formal_run"] = run_identity
    value["run_identity"] = run_identity
    value.setdefault("admission_gaps", {"not_admitted": [], "enumeration": []})
    value["admission_gaps"]["unenumerated"] = [{"kind": kind, "reason": "; ".join(enumeration_errors)} for kind in unavailable]
    value["enumeration_errors"] = enumeration_errors
    if enumeration_errors:
        value["layer_ok"] = False
    write_record(output_dir / "composition.json", value)
    return value
