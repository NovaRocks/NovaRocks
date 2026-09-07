#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

repo_root=$(cd "$(dirname "$0")/../../../.." && pwd)
env_file="${NOVA_ENV_REST_ENV_FILE:-$repo_root/docker/iceberg-rest/runtime/current/env.sh}"
if [[ ! -f "$env_file" ]]; then
  echo "Iceberg REST environment is not initialized: $env_file" >&2
  echo "run docker/iceberg-rest/up.sh first" >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$env_file"

compose=(docker compose --env-file "$NOVA_ENV_COMPOSE_ENV" \
  -p "$NOVA_ENV_COMPOSE_PROJECT" -f "$NOVA_ENV_COMPOSE_FILE")
"${compose[@]}" ps --status running spark | grep -q spark || {
  echo "Spark fixture is not running; run docker/iceberg-rest/up.sh first" >&2
  exit 1
}

runtime_jar=/opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar
runtime_digest=$("${compose[@]}" exec -T spark sha256sum "$runtime_jar" | awk '{print $1}')
test "$runtime_digest" = "94b8e36fc329f0293d44ba9e01b784a56e9501affec1842d898144c51f6e486a"

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-spark-theta-interop.XXXXXX")
container_dir="/tmp/novarocks-spark-theta-interop-$$"
cleanup() {
  "${compose[@]}" exec -T spark rm -rf "$container_dir" >/dev/null 2>&1 || true
  rm -rf "$work_dir"
}
trap cleanup EXIT

"${compose[@]}" exec -T spark mkdir -p "$container_dir/fixtures"
"${compose[@]}" cp \
  "$repo_root/tests/datasketches-tck/fixtures/." "spark:$container_dir/fixtures"
"${compose[@]}" cp \
  "$repo_root/tests/datasketches-tck/interop/spark/VerifyThetaInterop.scala" \
  "spark:$container_dir/VerifyThetaInterop.scala"
"${compose[@]}" cp "$NOVAROCKS_SPARK_DEFAULTS" "spark:$container_dir/spark-defaults.conf"

"${compose[@]}" exec -T -e NOVA_THETA_INTEROP_ROOT="$container_dir" spark \
  /bin/bash -lc "spark_shell_bin=\$(command -v spark-shell || true); \
    if [[ -z \"\$spark_shell_bin\" && -x /opt/spark/bin/spark-shell ]]; then \
      spark_shell_bin=/opt/spark/bin/spark-shell; \
    fi; \
    test -n \"\$spark_shell_bin\"; \
    printf ':quit\\n' | \"\$spark_shell_bin\" \
    --properties-file '$container_dir/spark-defaults.conf' \
    -i '$container_dir/VerifyThetaInterop.scala'"

"${compose[@]}" cp "spark:$container_dir/spark_disjoint_n1000.sk" "$work_dir/"
cargo run --quiet --locked -p novarocks-datasketches-tck \
  --bin verify_theta_interop -- \
  "$work_dir/spark_disjoint_n1000.sk" 1000 \
  "$repo_root/tests/datasketches-tck/fixtures/theta/rust_quickselect_n1000_ordered_v3.sk" 2000
