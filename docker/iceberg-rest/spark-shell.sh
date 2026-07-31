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

# Run a Scala script through the fixture's Spark shell with the same catalog
# configuration as spark-sql.sh.  This is intentionally a test-fixture helper:
# Iceberg's Table API exposes metadata (including StatisticsFile/Puffin) that
# is not available as a stable Spark SQL metadata table.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
CURRENT_ENV="$SCRIPT_DIR/runtime/current/env.sh"

if [[ ! -f "$CURRENT_ENV" ]]; then
  echo "environment is not initialized: $CURRENT_ENV" >&2
  echo "run docker/iceberg-rest/up.sh first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$CURRENT_ENV"

scala_file="${1:-}"
if [[ -z "$scala_file" ]]; then
  echo "usage: $0 <scala-file>" >&2
  exit 2
fi
if [[ ! -f "$scala_file" ]]; then
  echo "Scala file not found: $scala_file" >&2
  exit 1
fi
if [[ ! -f "$NOVAROCKS_SPARK_DEFAULTS" ]]; then
  echo "Spark defaults file not found: $NOVAROCKS_SPARK_DEFAULTS" >&2
  exit 1
fi

compose_args=(
  docker compose
  --env-file "$NOVA_ENV_COMPOSE_ENV"
  -p "$NOVA_ENV_COMPOSE_PROJECT"
  -f "$NOVA_ENV_COMPOSE_FILE"
)

tmp_dir="/tmp/novarocks-spark-shell-${NOVA_ENV_ID:-env}-$$"
tmp_scala="$tmp_dir/query.scala"
tmp_defaults="$tmp_dir/spark-defaults.conf"

cd "$WORKSPACE_ROOT"
"${compose_args[@]}" exec -T spark /bin/bash -lc "mkdir -p '$tmp_dir'"
{
  cat "$NOVAROCKS_SPARK_DEFAULTS"
  if [[ -n "${NOVAROCKS_SPARK_EXTRA_DEFAULTS:-}" ]]; then
    IFS=':' read -r -a extra_defaults_files <<< "$NOVAROCKS_SPARK_EXTRA_DEFAULTS"
    for defaults_file in "${extra_defaults_files[@]}"; do
      if [[ ! -f "$defaults_file" ]]; then
        echo "Spark extra defaults file not found: $defaults_file" >&2
        exit 1
      fi
      printf '\n'
      cat "$defaults_file"
    done
  fi
} | "${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_defaults'"
"${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_scala'" < "$scala_file"
"${compose_args[@]}" exec -T spark /bin/bash -lc "
  set -euo pipefail
  trap 'rm -rf $tmp_dir' EXIT
  spark_shell_bin=\"\${SPARK_SHELL_BIN:-}\"
  if [[ -z \"\$spark_shell_bin\" ]]; then
    spark_shell_bin=\"\$(command -v spark-shell || true)\"
  fi
  if [[ -z \"\$spark_shell_bin\" && -x /opt/spark/bin/spark-shell ]]; then
    spark_shell_bin=/opt/spark/bin/spark-shell
  fi
  if [[ -z \"\$spark_shell_bin\" ]]; then
    echo 'spark-shell binary not found' >&2
    exit 127
  fi
  printf ':quit\\n' | \"\$spark_shell_bin\" --properties-file '$tmp_defaults' -i '$tmp_scala'
"
