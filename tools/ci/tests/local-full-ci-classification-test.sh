#!/usr/bin/env bash
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

set -euo pipefail
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
source "$REPO_ROOT/tools/ci/local-full-ci.sh" --source-only
test_root="$(mktemp -d)"
trap 'rm -rf "$test_root"' EXIT
python3 - "$test_root" <<'FIXTURES'
import copy,json,sys
from pathlib import Path
root=Path(sys.argv[1])
def records(status='pass',code=0):
    return [
      {'kind':'run','schema':1,'mode':'verify','verification':{'verify_enabled':True,'record_from':'reference'},'platform':{'system':'fixture','machine':'fixture','release':'fixture'}},
      {'kind':'selection','suites':[{'suite':'s','inputs_sha256':'inputs','verify_enabled':True,'query_timeout_secs':60,'declaration':{'scope':'suite','initial_state':'isolated','env_inputs':[]},'cases':[{'case':'a'}]}]},
      {'kind':'case','suite':'s','case':'a','lifecycle_id':'L','status':status,'oracle':{'required':[{'step':1,'kind':'result_set'}],'executed':[]} if status=='fail' else {'required':[{'step':1,'kind':'expected_error'}],'executed':[{'step':1,'kind':'expected_error','outcome':'pass','policy':{}}]},'failure':{'class':'error_code','error_code':'NR-FAIL','statement_index':1,'actual_digest':None} if status=='fail' else None},
      {'kind':'suite','suite':'s','lifecycle_id':'L','inputs_end_sha256':'inputs','inputs_stable':True,'executed_cases':['a'],'cleanup':{'status':'ok'}},
      {'kind':'server','cleanup':{'status':'ok'}},{'kind':'fixture','shutdown':{'status':'ok'},'resources':[]},
      {'kind':'end','completed':True,'exit_code':code}]
def put(name,rows):
    (root/(name+'.results.jsonl')).write_text(''.join(json.dumps(row)+'\n' for row in rows))
put('pass',records());put('fail',records('fail',1));put('truncated',records()[:-1])
violation=records();violation[4]['cleanup']['status']='failed';put('cleanup',violation)
# Port upstream strict-refusal cases as independent native records. Code text
# in a diagnostic or expected PASS never supplies a failure feature.
negative=records('fail',1);negative[2]['oracle']={'required':[{'step':1,'kind':'expected_error'}],'executed':[{'step':1,'kind':'expected_error','outcome':'fail'}]};put('negative',negative)
verify=records('fail',1);verify[2]['failure'].update({'class':'mismatch','actual_digest':'actual'});put('verify',verify)
diagnostic=records('fail',1);diagnostic[2]['failure']['class']='error';diagnostic[2]['reason']='VERIFY FAILED: expected PASS NR-FAIL';put('diagnostic',diagnostic)
for name,additional in [('case-cleanup',[{'class':'error','phase':'case_cleanup','error':'database busy'}]), ('multiple-refusals',[{'class':'error_code','error_code':'NR-OTHER','statement_index':2}])]:
    aggregate=records('fail',1)
    primary=copy.deepcopy(aggregate[2]['failure'])
    aggregate[2]['failure']={'class':'error','primary_failure':primary,'additional_failures':additional}
    put(name,aggregate)
for code in ('NR-FAIL.extra','UnknownEngineCode'):
    unknown=records('fail',1);unknown[2]['failure']['error_code']=code;put(code,unknown)
expected=records();expected[2]['reason']='expected error matched: UnknownEngineCode NR-FAIL';put('expected',expected)
direct=records('fail',1);direct[2]['reason']='target execute failed; diagnostic quotes PASS and UnknownEngineCode';put('direct',direct)
secondary=records('fail',1)
secondary[2]['oracle']={'required':[{'step':1,'kind':'result_set'},{'step':2,'kind':'expected_error'}], 'executed':[{'step':2,'kind':'expected_error','outcome':'fail'}]}
put('secondary',secondary)
for phase in ('init','cleanup'):
    hook=records('fail',1);hook[3][phase]={'status':'failed','errors':['suite hook failed']};put('suite-'+phase,hook)
empty={'schema':1,'known_failures':[],'quarantine':[]}
(root/'empty.json').write_text(json.dumps(empty))
known=copy.deepcopy(empty);known['known_failures']=[{'unit':'sql-case:s/a','failure':{'class':'error_code','code':'NR-FAIL'},'reason':'independent failing fixture','owner':'test owner','expires':'9999-12-31'}]
(root/'known.json').write_text(json.dumps(known));known['known_failures'][0]['expires']='2000-01-01';(root/'expired.json').write_text(json.dumps(known))
FIXTURES
ci_init_summary_state
check() {
  local file="$1" dispositions="$2" expected="$3" expected_code="$4" code=0
  CI_KNOWN_FAILURE_ROWS=""
  DISPOSITIONS_FILE="$test_root/$dispositions.json"
  ci_classify_sql_results s "$test_root/$file.results.jsonl" || code=$?
  test "$CI_SQL_RESULT_STATUS" = "$expected"
  test "$code" -eq "$expected_code"
}
check pass empty PASS 0
ci_sql_native_exit_acceptable 0
if ci_sql_native_exit_acceptable 1; then echo 'unexplained invocation exit accepted' >&2; exit 1; fi
check fail known KNOWN_FAIL 0
ci_sql_native_exit_acceptable 1
if ci_sql_native_exit_acceptable 0; then echo 'contradictory invocation exit accepted' >&2; exit 1; fi
if ci_sql_native_exit_acceptable 143; then echo 'cancelled invocation accepted' >&2; exit 1; fi
check fail expired FAIL 1
[[ "$CI_KNOWN_FAILURE_ROWS" == *EXPIRED_KNOWN_FAILURE* ]]
check fail empty FAIL 1
[[ "$CI_KNOWN_FAILURE_ROWS" == *NEW_FAILURE* ]]
check pass known UNEXPECTED_PASS 1
[[ "$CI_KNOWN_FAILURE_ROWS" == *STALE_KNOWN_FAILURE* ]]
check cleanup empty FAIL 1
check truncated empty FAIL 1
check expected empty PASS 0
check direct known KNOWN_FAIL 0
test "$(printf '%s\n' "$CI_KNOWN_FAILURE_ROWS" | rg -c KNOWN_FAIL)" = 1
for fixture in negative verify diagnostic NR-FAIL.extra UnknownEngineCode case-cleanup multiple-refusals; do
  check "$fixture" known FAIL 1
  [[ "$CI_KNOWN_FAILURE_ROWS" != *KNOWN_FAIL* ]]
done
check secondary known FAIL 1
[[ "$CI_KNOWN_FAILURE_ROWS" == *NEW_FAILURE* ]]
for fixture in suite-init suite-cleanup; do
  check "$fixture" known FAIL 1
  [[ "$CI_KNOWN_FAILURE_ROWS" == *KNOWN_FAIL* ]]
done

# Terminal text is deliberately false. Reclassification must use native records.
mkdir -p "$test_root/reclassify/sql"
cp "$test_root/pass.results.jsonl" "$test_root/reclassify/sql/s.results.jsonl"
printf 'fail=99\n    [s] a FAIL 1s\n' >"$test_root/reclassify/sql/s.log"
DISPOSITIONS_FILE="$test_root/empty.json"
CI_FROM_RUN_DIR="$test_root/reclassify"
raw_before="$(shasum -a 256 "$test_root/reclassify/sql/s.results.jsonl")"
reclassify_existing_run >"$test_root/reclassify.log"
grep -F '| s | PASS |' "$test_root/reclassify/summary.md" >/dev/null
test "$raw_before" = "$(shasum -a 256 "$test_root/reclassify/sql/s.results.jsonl")"

# The default tier delegates to native default discovery.
cat >"$test_root/sql-runner" <<'RUNNER'
#!/usr/bin/env bash
[ "$1" = --list-suites ] && [ "$2" = --suite ] && [ "$3" = all ] || exit 2
printf '%s\n' analytic decimal iceberg-mv-scheduler mv-rewrite runtime-filter-distributed sql-reject subquery
RUNNER
chmod +x "$test_root/sql-runner"
ci_bound_binary() { printf '%s\n' "$test_root/sql-runner"; }
defaults="$(ci_tier_suites full "$REPO_ROOT")"
for name in decimal iceberg-mv-scheduler mv-rewrite runtime-filter-distributed sql-reject subquery; do
  grep -Fx "$name" <<<"$defaults" >/dev/null
done

# Collection uses the ordinary dev classification view; quarantine is not a
# dev/full PASS and a direct unregistered refusal remains a raw failure.
python3 - "$test_root" <<'QUARANTINE'
import json,sys
from pathlib import Path
root=Path(sys.argv[1]); value=json.loads((root/'empty.json').read_text())
value['quarantine']=[{'unit':'sql-case:s/a','reason':'fixture capability','owner':'fixture owner','exit_condition':'restore capability','review_by':'9999-12-31'}]
(root/'quarantine.json').write_text(json.dumps(value))
QUARANTINE
CI_COLLECT_ALL=true
check fail quarantine FAIL 1
python3 - "$test_root/fail.classification.json" <<'NO_EXCLUSION'
import json,sys
value=json.load(open(sys.argv[1]));assert value['membership']=={'layer':'dev','excluded':{}}
assert value['rows'][0]['status']=='NEW_FAILURE'
NO_EXCLUSION
CI_COLLECT_ALL=false

# Native batch fixtures exercise the shell continuation decision without Cargo.
python3 - "$SCRIPT_DIR" "$test_root/batch" <<'RUST_FIXTURE'
import importlib.util,shutil,sys
from pathlib import Path
spec=importlib.util.spec_from_file_location('collection_fixture',Path(sys.argv[1])/'tests/verification-collection-test.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
case=module.Collection();fixture=case.rust(True)
shutil.copytree(fixture.root,Path(sys.argv[2])/'rust')
case.doCleanups()
RUST_FIXTURE
CI_RUN_DIR="$test_root/batch"; CI_SUMMARY="$CI_RUN_DIR/summary.md"
ci_init_summary_state
ci_invocation_start() { printf '%s\n' "$CI_RUN_DIR/raw.started.json"; }
ci_invocation_finish() { return 0; }
python3() {
  if [ "${1:-}" = tools/ci/test-run.py ] && [ "${2:-}" = batch ]; then return 1; fi
  command python3 "$@"
}
code=0; run_rust_batch_stage >/dev/null || code=$?
[ "$code" = 1 ]
CI_COLLECT_ALL=true; CI_COLLECTION_RESULT=0
code=0; run_rust_batch_stage >/dev/null || code=$?
[ "$code" = 0 ] && [ "$CI_COLLECTION_RESULT" = 1 ]
grep -q '^wrapper_exit=1$' "$CI_RUN_DIR/rust-batch.log"
[[ "$CI_STAGE_ROWS" == *'| workspace nextest and doctests | FAIL |'* ]]
[[ "$CI_STAGE_ROWS" != *'| workspace nextest and doctests | PASS |'* ]]
# An all-failure batch is still unsafe if its cleanup record is missing.
rm "$CI_RUN_DIR/rust/doctest-run/cleanup-summary.json"
code=0; run_rust_batch_stage >/dev/null || code=$?
[ "$code" = 1 ]
unset -f python3

# Collection refuses partial-membership and skip modes before execution.
for option in '--skip-cargo-test' '--skip-system-scenarios' '--suite s' '--tier smoke' '--cargo-only'; do
  code=0
  (parse_args --collect-all $option) >/dev/null 2>&1 || code=$?
  [ "$code" = 2 ]
done
# All original stages pass, but only actual EXIT finalization may publish PASS.
# Each failure uses the real cleanup function with bounded local owner fakes.
cleanup_root="$test_root/cleanup-runtime"
mkdir -p "$cleanup_root/docker/iceberg-rest"
cat >"$cleanup_root/docker/iceberg-rest/down.sh" <<'RUNTIME'
#!/usr/bin/env bash
[ "$*" = '--runtime-only --purge' ] || exit 91
exit "${FAKE_RUNTIME_EXIT:-0}"
RUNTIME
chmod +x "$cleanup_root/docker/iceberg-rest/down.sh"
ci_stop_all_in_one_server() { return "${FAKE_STOP_EXIT:-0}"; }
python3() {
  if [[ "${1:-}" == */identity_snapshot.py ]] && [ "${2:-}" = source ]; then
    return "${FAKE_SOURCE_EXIT:-0}"
  fi
  if [ "${FAKE_COLLECTION_WRITE_FAILURE:-false}" = true ] && [ "${1:-}" = - ] &&
     [[ "${2:-}" == */collection.json ]]; then return 13; fi
  command python3 "$@"
}
for mode in default collect-all; do
  for failure in success stop runtime source incomplete early-failure; do
    CI_RUN_DIR="$test_root/cleanup-$mode-$failure"; mkdir -p "$CI_RUN_DIR"
    CI_SUMMARY="$CI_RUN_DIR/summary.md"
    ci_init_summary_state
    CI_COLLECT_ALL=false; [ "$mode" != collect-all ] || CI_COLLECT_ALL=true
    CI_COLLECTION_REACHED_END=true; CI_RUN_REACHED_END=true; CI_COLLECTION_RESULT=0
    CI_RUNTIME_PREPARED=true; KEEP_RUNTIME=false
    ci_record_stage "all original stages" "PASS" "0" ""
    ci_render_summary "PASS"
    export FAKE_STOP_EXIT=0 FAKE_RUNTIME_EXIT=0 FAKE_SOURCE_EXIT=0 FAKE_COLLECTION_WRITE_FAILURE=false
    expected=0; entry=0
    case "$failure" in
      stop) FAKE_STOP_EXIT=7; expected=1 ;;
      runtime) FAKE_RUNTIME_EXIT=9; expected=1 ;;
      source) FAKE_SOURCE_EXIT=11; expected=1 ;;
      incomplete) CI_RUN_REACHED_END=false; expected=1 ;;
      early-failure) CI_RUN_REACHED_END=false; entry=23; expected=23 ;;
    esac
    ci_fake_entry_status() { return "$entry"; }
    code=0
    (cd "$cleanup_root" && ci_fake_entry_status; cleanup) >"$CI_RUN_DIR/finalization.log" 2>&1 || code=$?
    [ "$code" = "$expected" ]
    status=PASS; [ "$expected" = 0 ] || status=FAIL
    grep -Fx -- "- Status: $status" "$CI_SUMMARY" >/dev/null
    grep -Fx "$status: $CI_SUMMARY" "$CI_RUN_DIR/finalization.log" >/dev/null
    if [ "$status" = FAIL ]; then ! grep -Fx "PASS: $CI_SUMMARY" "$CI_RUN_DIR/finalization.log" >/dev/null; fi
    if [ "$mode" = default ]; then
      [ ! -f "$CI_RUN_DIR/collection.json" ]
    else
      command python3 - "$CI_RUN_DIR/collection.json" "$failure" "$expected" <<'CLEANUP'
import json,sys
value=json.load(open(sys.argv[1]));failure=sys.argv[2];code=int(sys.argv[3])
assert value['collection_complete']==(code==0)
assert value['exit_handler_status']==code
assert value['run_reached_end']==(failure not in ('incomplete','early-failure'))
assert value['cleanup']['server_stop_exit']==(7 if failure=='stop' else 0)
assert value['cleanup']['runtime_cleanup_exit']==(9 if failure=='runtime' else 0)
assert value['cleanup']['source_end_exit']==(11 if failure=='source' else 0)
assert value['cleanup']['runtime_cleanup_attempted'] is True
assert value['summary_write_exit']==0
assert value['invocation_exit']=='caller-must-observe'
CLEANUP
    fi
    grep -F "| run terminal and cleanup facts (source=$FAKE_SOURCE_EXIT stop=$FAKE_STOP_EXIT runtime=$FAKE_RUNTIME_EXIT) | $status |" "$CI_SUMMARY" >/dev/null
  done
done
# Failing the extra collector record is not a successful invocation either.
CI_RUN_DIR="$test_root/cleanup-record-failure"; mkdir -p "$CI_RUN_DIR"
CI_SUMMARY="$CI_RUN_DIR/summary.md"; ci_init_summary_state
CI_COLLECT_ALL=true; CI_COLLECTION_REACHED_END=true; CI_RUN_REACHED_END=true
export FAKE_SOURCE_EXIT=0 FAKE_STOP_EXIT=0 FAKE_RUNTIME_EXIT=0 FAKE_COLLECTION_WRITE_FAILURE=true
code=0; (cd "$cleanup_root" && cleanup) >"$CI_RUN_DIR/finalization.log" 2>&1 || code=$?
[ "$code" = 1 ] && [ ! -f "$CI_RUN_DIR/collection.json" ]
grep -Fx -- '- Status: FAIL' "$CI_SUMMARY" >/dev/null
grep -F '| collection record persistence | FAIL |' "$CI_SUMMARY" >/dev/null
# A failed source-start/init returns its original code before any fixture or
# Cargo stage. The installed EXIT trap can render the initialized state as FAIL.
init_failure_marker="$test_root/unexpected-init-fixture"
code=0
(
  CI_COLLECT_ALL=false; CI_FROM_RUN_DIR=""; CARGO_ONLY=false
  CI_RUN_REACHED_END=false; CI_RUNTIME_PREPARED=false
  export FAKE_SOURCE_EXIT=0 FAKE_STOP_EXIT=0 FAKE_RUNTIME_EXIT=0 FAKE_COLLECTION_WRITE_FAILURE=false
  init_run_dir() {
    CI_RUN_DIR="$test_root/init-failure"; mkdir -p "$CI_RUN_DIR"
    CI_SUMMARY="$CI_RUN_DIR/summary.md"; ci_init_summary_state
    ci_render_summary "RUNNING"
    return 17
  }
  prepare_runtime() { printf 'unexpected fixture\n' >"$init_failure_marker"; exit 72; }
  main
) >"$test_root/init-failure.log" 2>&1 || code=$?
[ "$code" = 17 ] && [ ! -f "$init_failure_marker" ]
grep -Fx -- '- Status: FAIL' "$test_root/init-failure/summary.md" >/dev/null
! grep -Fx "PASS: $test_root/init-failure/summary.md" "$test_root/init-failure.log" >/dev/null
# Read-only / stage-description dispatch never installs the normal-run cleanup
# trap or starts a fixture, including --from's existing SQL-only classifier.
readonly_marker="$test_root/unexpected-readonly-cleanup"
for operation in from list; do
  code=0
  (
    CI_COLLECT_ALL=false; CI_FROM_RUN_DIR=""; CARGO_ONLY=false
    DISPOSITIONS_FILE="$test_root/empty.json"
    cleanup() { printf 'unexpected cleanup\n' >"$readonly_marker"; exit 71; }
    prepare_runtime() { printf 'unexpected fixture\n' >"$readonly_marker"; exit 72; }
    if [ "$operation" = from ]; then main --from "$test_root/reclassify"; else main --list-stages; fi
  ) >"$test_root/readonly-$operation.log" 2>&1 || code=$?
  [ "$code" = 0 ] && [ ! -f "$readonly_marker" ]
done

unset -f python3
unset FAKE_SOURCE_EXIT FAKE_STOP_EXIT FAKE_RUNTIME_EXIT FAKE_COLLECTION_WRITE_FAILURE

# Runtime preparation delegates directly to the fixture owner. Missing Java,
# Docker or unavailable services surface from their actual commands, including
# exit 75, rather than becoming an input-BOM prerequisite result.
prepare_root="$test_root/prepare-owner"; mkdir -p "$prepare_root/docker/iceberg-rest"
cat >"$prepare_root/docker/iceberg-rest/up.sh" <<'UP'
#!/usr/bin/env bash
printf 'up\n' >>"$PREPARE_CALLS"
exit "$PREPARE_EXIT"
UP
chmod +x "$prepare_root/docker/iceberg-rest/up.sh"
for expected in 0 75 23; do
  code=0
  (
    CI_RUN_DIR="$test_root/prepare-$expected"; mkdir -p "$CI_RUN_DIR"
    CI_SUMMARY="$CI_RUN_DIR/summary.md"; ci_init_summary_state
    export PREPARE_CALLS="$CI_RUN_DIR/calls" PREPARE_EXIT="$expected"
    verify_fixture_inputs() { echo 'unexpected verifier' >>"$PREPARE_CALLS"; return 92; }
    python3() { echo 'unexpected probe' >>"$PREPARE_CALLS"; return 93; }
    load_fixture_publication() {
      NOVAROCKS_FE_CONFIG=fe; NOVAROCKS_BE_CONFIG=be; NOVAROCKS_SQL_TEST_CONFIG=sql
      NOVA_ENV_RUNTIME_DIR=runtime; NOVA_ENV_TRINO_PORT=8080
      NOVAROCKS_ICEBERG_REST_URI=rest; NOVAROCKS_ICEBERG_REST_MV_URI=mv
      NOVA_ENV_REST_ENV_FILE=env; NOVA_ENV_MANIFEST=manifest
      NOVA_ENV_OBJECT_STORE_RUNTIME=store; NOVA_ENV_CATALOG_RUNTIME=catalog; AWS_S3_ENDPOINT=s3
    }
    record_fixture_runtime() { echo 'publication' >>"$PREPARE_CALLS"; }
    cd "$prepare_root"
    prepare_runtime
    [ "$CI_RUNTIME_PREPARED" = true ]
  ) >"$test_root/prepare-$expected.log" 2>&1 || code=$?
  [ "$code" = "$expected" ]
  calls="$(cat "$test_root/prepare-$expected/calls")"
  if [ "$expected" = 0 ]; then
    [ "$calls" = $'up\npublication' ]
    grep -F '| prepare runtime | PASS |' "$test_root/prepare-$expected/summary.md" >/dev/null
  else
    [ "$calls" = up ]
    grep -F '| prepare runtime | FAIL |' "$test_root/prepare-$expected/summary.md" >/dev/null
    ! grep -F 'BLOCKED' "$test_root/prepare-$expected/summary.md" >/dev/null
  fi
done
# Cargo-only reaches its original gates without creating or diagnosing a fixture.
(
  CI_FROM_RUN_DIR=""; CI_COLLECT_ALL=false; CARGO_ONLY=false
  cleanup() { :; }
  init_run_dir() { :; }
  validate_explicit_suites_early() { :; }
  prepare_runtime() { echo 'unexpected fixture' >&2; exit 94; }
  run_cargo_gates() { printf 'cargo\n' >"$test_root/cargo-only"; }
  ci_render_summary() { :; }
  main --cargo-only
  [ "$CI_RUN_REACHED_END" = true ]
)
[ "$(cat "$test_root/cargo-only")" = cargo ]

echo 'local-full-ci-classification-test: PASS'
