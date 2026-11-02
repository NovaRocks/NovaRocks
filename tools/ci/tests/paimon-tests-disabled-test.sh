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

ci_bound_binary() { printf '%s\n' "$test_root/sql-runner"; }
cat >"$test_root/sql-runner" <<'RUNNER'
#!/usr/bin/env bash
printf '%s\n' filter optimizer
RUNNER
chmod +x "$test_root/sql-runner"
if ci_discover_sql_suites "$REPO_ROOT" default | grep -Fx paimon; then
  echo "disabled Paimon suite must not join default CI" >&2
  exit 1
fi

code=0
(
  parse_args --suite paimon
  validate_explicit_suites_early
) >"$test_root/selection.log" 2>&1 || code=$?
test "$code" -eq 2
grep -F 'SQL suite paimon is disabled' "$test_root/selection.log" >/dev/null

# A successful Iceberg preparation must not invoke the retained Paimon writer.
(
  CI_RUN_DIR="$test_root/runtime"
  mkdir -p "$CI_RUN_DIR"
  verify_fixture_inputs() { return 0; }
  function docker/iceberg-rest/up.sh { return 0; }
  load_fixture_publication() { return 0; }
  require_runtime_var() { return 0; }
  record_fixture_runtime() { return 0; }
  python3() { return 0; }
  function docker/paimon-read/prepare.sh {
    touch "$test_root/paimon-called"
    return 99
  }
  ci_record_stage() { return 0; }
  ci_render_summary() { return 0; }
  NOVAROCKS_FE_CONFIG=retained-fe
  NOVAROCKS_BE_CONFIG=retained-be
  NOVAROCKS_SQL_TEST_CONFIG=retained-sql
  NOVA_ENV_REST_ENV_FILE=retained-env
  NOVA_ENV_RUNTIME_DIR=retained-runtime
  NOVA_ENV_TRINO_PORT=1
  NOVAROCKS_ICEBERG_REST_URI=retained-rest
  NOVAROCKS_ICEBERG_REST_MV_URI=retained-mv
  NOVA_ENV_MANIFEST=retained-manifest
  prepare_runtime
  test "$NOVAROCKS_SQL_TEST_CONFIG" = retained-sql
  test "$CI_TRINO_PORT" = 1
) >"$test_root/runtime.log" 2>&1
test ! -e "$test_root/paimon-called"
grep -F 'PAIMON_TESTS=DISABLED' "$test_root/runtime/env.log" >/dev/null
grep -F 'NOVA_ENV_TRINO_PORT=1' "$test_root/runtime/env.log" >/dev/null

# Summary reports the actual client endpoint used by SQL, rather than MySQL.
ci_init_summary_state
CI_RUN_DIR="$test_root/summary"
mkdir -p "$CI_RUN_DIR"
CI_SUMMARY="$CI_RUN_DIR/summary.md"
ci_set_runtime_context retained-fe retained-sql 1
ci_render_summary PASS
grep -Fx -- '- Trino port: 1' "$CI_SUMMARY" >/dev/null

code=0
bash "$REPO_ROOT/docker/paimon-read/tests/run.sh" >"$test_root/fixture.log" 2>&1 || code=$?
test "$code" -eq 2
grep -F 'DISABLED: Paimon fixture tests' "$test_root/fixture.log" >/dev/null

echo "paimon-tests-disabled-test: PASS (selection and orchestration only)"
