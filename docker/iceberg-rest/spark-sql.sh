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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
# `current` is a convenience link for interactive use. CI passes the exact
# generated entry it prepared so isolated system fixtures cannot redirect or
# remove the environment required by subsequent SQL helpers.
CURRENT_ENV="${NOVA_ENV_REST_ENV_FILE:-$WORKSPACE_ROOT/docker/iceberg-rest/runtime/current/env.sh}"
CURRENT_ENV="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$CURRENT_ENV")"

if [[ ! -f "$CURRENT_ENV" ]]; then
  echo "environment is not initialized: $CURRENT_ENV" >&2
  echo "run docker/iceberg-rest/up.sh first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$CURRENT_ENV"
[[ "${NOVA_ENV_READY:-false}" == "true" ]] || { echo "fixture is not ready; run docker/iceberg-rest/up.sh" >&2; exit 1; }

sql_file="${1:-${NOVAROCKS_SPARK_V3_SMOKE_SQL:-}}"
if [[ -z "$sql_file" ]]; then
  echo "usage: $0 [sql-file]" >&2
  exit 2
fi

if [[ ! -f "$sql_file" ]]; then
  echo "SQL file not found: $sql_file" >&2
  exit 1
fi

if [[ ! -f "$NOVAROCKS_SPARK_DEFAULTS" ]]; then
  echo "Spark defaults file not found: $NOVAROCKS_SPARK_DEFAULTS" >&2
  exit 1
fi

extra_defaults_files=()
if [[ -n "${NOVAROCKS_SPARK_EXTRA_DEFAULTS:-}" ]]; then
  IFS=':' read -r -a extra_defaults_files <<< "$NOVAROCKS_SPARK_EXTRA_DEFAULTS"
  for defaults_file in "${extra_defaults_files[@]}"; do
    if [[ ! -f "$defaults_file" ]]; then
      echo "Spark extra defaults file not found: $defaults_file" >&2
      exit 1
    fi
  done
fi

compose_args=(
  python3 "$SCRIPT_DIR/runtime_entry.py" compose
  --env-file "$NOVA_ENV_COMPOSE_ENV"
  -p "$NOVA_ENV_COMPOSE_PROJECT"
  -f "$NOVA_ENV_COMPOSE_FILE"
)

tmp_dir="/tmp/novarocks-spark-sql-${NOVA_ENV_ID:-env}-$$"
tmp_sql="$tmp_dir/query.sql"
tmp_defaults="$tmp_dir/spark-defaults.conf"

cd "$WORKSPACE_ROOT"
"${compose_args[@]}" exec -T spark /bin/bash -lc "mkdir -p '$tmp_dir'"
{
  cat "$NOVAROCKS_SPARK_DEFAULTS"
  if [[ ${#extra_defaults_files[@]} -gt 0 ]]; then
    for defaults_file in "${extra_defaults_files[@]}"; do
      printf '\n'
      cat "$defaults_file"
    done
  fi
} | "${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_defaults'"
"${compose_args[@]}" exec -T spark /bin/bash -lc "cat > '$tmp_sql'" < "$sql_file"
"${compose_args[@]}" exec -T spark /bin/bash -lc "
  set -euo pipefail
  trap 'rm -rf $tmp_dir' EXIT
  spark_sql_bin=\"\${SPARK_SQL_BIN:-}\"
  if [[ -z \"\$spark_sql_bin\" ]]; then
    spark_sql_bin=\"\$(command -v spark-sql || true)\"
  fi
  if [[ -z \"\$spark_sql_bin\" && -x /opt/spark/bin/spark-sql ]]; then
    spark_sql_bin=/opt/spark/bin/spark-sql
  fi
  if [[ -z \"\$spark_sql_bin\" ]]; then
    echo 'spark-sql binary not found' >&2
    exit 127
  fi
  \"\$spark_sql_bin\" --properties-file '$tmp_defaults' -f '$tmp_sql'
"
