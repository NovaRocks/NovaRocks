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

# Native 1FE+NBE System Scenario stage.
#
# This file selects, invokes and reports. It deliberately owns no cluster
# behaviour: configuration rendering, process spawn, readiness, topology
# barriers, restart, fault injection, artifact retention and cleanup all belong
# to `novarocks-cluster-harness`, reached through the
# `novarocks-system-test-runner` frontend. Do not reimplement any of that here.

# Discover the currently registered scenarios. The registry is the source of
# truth; the stage never hardcodes scenario names or an expected count.
ci_system_scenario_list() {
  local runner="$1"

  "$runner" --list-default
}

# Build the two explicit native-compatibility fixtures without leaving the
# workspace binary on a test-only identity or compatibility epoch. The caller
# owns the fixture root and may retain it with the rest of the CI artifacts for
# exact scenario reruns.
ci_prepare_system_scenario_binaries() {
  local binary="$1"
  local profile="$2"
  local fixture_root="$3"
  local build_suffix="$4"
  local log_path="$5"

  SYSTEM_SCENARIO_PRIMARY_BINARY=""
  SYSTEM_SCENARIO_COMPATIBLE_BINARY=""
  SYSTEM_SCENARIO_OTHER_ISLAND_BINARY=""

  if [ ! -x "$binary" ]; then
    echo "error: system scenario primary binary is not executable: $binary" >"$log_path"
    return 1
  fi

  mkdir -p "$fixture_root"
  local primary="$fixture_root/novarocks-primary"
  local compatible="$fixture_root/novarocks-compatible"
  local other_island="$fixture_root/novarocks-other-island"
  cp "$binary" "$primary" || return 1

  local code=0
  (
    set -x
    NOVAROCKS_NATIVE_BUILD_IDENTITY="ci-compatible-$build_suffix" \
      cargo build -p novarocks-server --bin novarocks --profile "$profile" &&
    cp "$binary" "$compatible" &&
    NOVAROCKS_NATIVE_BUILD_IDENTITY="ci-other-island-$build_suffix" \
      cargo build -p novarocks-server --bin novarocks --profile "$profile" \
      --features native-compatibility-test-fixture &&
    cp "$binary" "$other_island"
  ) >"$log_path" 2>&1 || code=$?

  # The second build deliberately produces the test-alternate-epoch binary at
  # the canonical target path. Restore the workspace binary even when either fixture build
  # fails so later diagnostics and cleanup never observe a test-only server.
  if ! cp "$primary" "$binary" >>"$log_path" 2>&1; then
    echo "error: failed to restore system scenario primary binary $binary" >>"$log_path"
    return 1
  fi
  if [ "$code" -ne 0 ]; then
    return "$code"
  fi

  SYSTEM_SCENARIO_PRIMARY_BINARY="$primary"
  SYSTEM_SCENARIO_COMPATIBLE_BINARY="$compatible"
  SYSTEM_SCENARIO_OTHER_ISLAND_BINARY="$other_island"
}

# Run every registered scenario serially, one `--only` invocation each, and
# record an independent summary row per scenario.
#
# Returns non-zero on the first failing scenario. The caller decides how to fail
# the run; this function never exits the shell itself.
ci_run_system_scenarios() {
  local runner="$1"
  local binary="$2"
  local compatible_binary="$3"
  local other_island_binary="$4"
  local base_config="$5"
  local artifact_root="$6"
  local cluster_size="$7"
  local timeout_secs="$8"

  local scenarios=()
  local scenario
  local list_log="$CI_RUN_DIR/system/list.log"

  mkdir -p "$CI_RUN_DIR/system" "$artifact_root"

  if ! ci_system_scenario_list "$runner" >"$list_log" 2>&1; then
    ci_record_system_scenario "<registry>" "FAIL" "0" "$list_log" "-"
    ci_mark_failure_tail "system scenario discovery failed" "$list_log"
    return 1
  fi

  while IFS= read -r scenario; do
    [ -n "$scenario" ] || continue
    scenarios+=("$scenario")
  done <"$list_log"

  if [ "${#scenarios[@]}" -eq 0 ]; then
    ci_record_system_scenario "<registry>" "FAIL" "0" "$list_log" "-"
    ci_mark_failure_tail "system scenario registry is empty" "$list_log"
    return 1
  fi

  local slug
  local log_path
  local artifact_dir
  local start
  local code
  local duration

  for scenario in "${scenarios[@]}"; do
    slug="${scenario//\//-}"
    log_path="$CI_RUN_DIR/system/$slug.log"
    artifact_dir="$artifact_root/$slug"
    mkdir -p "$artifact_dir"

    start="$(ci_epoch)"
    ci_run_logged "$log_path" \
      "$runner" \
      --binary "$binary" \
      --compatible-binary "$compatible_binary" \
      --other-island-binary "$other_island_binary" \
      --config "$base_config" \
      --artifact-root "$artifact_dir" \
      --cluster-size "$cluster_size" \
      --timeout-secs "$timeout_secs" \
      --only "$scenario"
    code=$?
    duration=$(($(ci_epoch) - start))

    if [ "$code" -ne 0 ]; then
      ci_record_system_scenario "$scenario" "FAIL" "$duration" "$log_path" "$artifact_dir"
      # The runner already printed the action history, process diagnostics and
      # an exact rerun command; preserve that tail verbatim rather than
      # rebuilding a second, weaker failure report here.
      ci_mark_failure_tail "system scenario $scenario failed" "$log_path"
      return "$code"
    fi

    ci_record_system_scenario "$scenario" "PASS" "$duration" "$log_path" "$artifact_dir"
    ci_render_summary "RUNNING"
  done

  return 0
}
