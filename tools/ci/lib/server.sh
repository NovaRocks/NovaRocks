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

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/build_capabilities.sh"


CI_SERVER_PID=""

ci_novarocks_binary_path() {
  local cargo_profile="${1:-dev}"

  ci_bound_binary novarocks
}

ci_start_all_in_one_server() {
  local fe_config_path="$1"
  local be_config_path="$2"
  local log_path="$3"
  local timeout_seconds="$4"
  local cargo_profile="${5:-dev}"
  local binary_path
  local i

  binary_path="$(ci_novarocks_binary_path "$cargo_profile")"

  {
    printf "+ NO_PROXY=127.0.0.1,localhost %q standalone --role all-in-one --fe-config %q --be-config %q\n" \
      "$binary_path" \
      "$fe_config_path" \
      "$be_config_path"
    NO_PROXY=127.0.0.1,localhost \
      "$binary_path" standalone \
        --role all-in-one \
        --fe-config "$fe_config_path" \
        --be-config "$be_config_path"
  } >"$log_path" 2>&1 &
  CI_SERVER_PID=$!

  i=0
  while [ "$i" -lt "$timeout_seconds" ]; do
    if grep -Eq '^NOVAROCKS_READY role=fe trino_port=[0-9]+ pid=[0-9]+$' "$log_path" 2>/dev/null; then
      return 0
    fi

    if ! kill -0 "$CI_SERVER_PID" 2>/dev/null; then
      wait "$CI_SERVER_PID" 2>/dev/null || true
      CI_SERVER_PID=""
      return 1
    fi

    sleep 1
    i=$((i + 1))
  done

  if kill -0 "$CI_SERVER_PID" 2>/dev/null; then
    kill "$CI_SERVER_PID" 2>/dev/null || true
    wait "$CI_SERVER_PID" 2>/dev/null || true
  fi
  CI_SERVER_PID=""
  return 2
}

ci_stop_all_in_one_server() {
  if [ -n "$CI_SERVER_PID" ] && kill -0 "$CI_SERVER_PID" 2>/dev/null; then
    kill "$CI_SERVER_PID" 2>/dev/null || true
    wait "$CI_SERVER_PID" 2>/dev/null || true
  fi
  CI_SERVER_PID=""
}
