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

slug="$(basename "$WORKSPACE_ROOT" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/^-*//;s/-*$//;s/--*/-/g')"
if [[ -z "$slug" ]]; then
  slug="novarocks"
fi
slug="$(printf '%s' "$slug" | cut -c1-24)"
hash="$(printf '%s' "$WORKSPACE_ROOT" | shasum -a 1 | awk '{print substr($1, 1, 8)}')"
env_id="${slug}-${hash}"
runtime_base="$SCRIPT_DIR/runtime"
runtime_dir="$runtime_base/$env_id"
current_link="$runtime_base/current"
compose_file="$SCRIPT_DIR/compose.yml"
compose_env="$runtime_dir/compose.env"
exports_file="$runtime_dir/env.sh"
config_file="${NOVA_ENV_CONFIG_FILE:-$SCRIPT_DIR/shared.env}"

if [[ -f "$config_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$config_file"
  set +a
fi

shared_docker="${NOVA_ENV_SHARED_DOCKER:-true}"
configured_compose_project="${NOVA_ENV_SHARED_COMPOSE_PROJECT:-nr-iceberg-rest}"
if [[ "$shared_docker" == "true" ]]; then
  compose_project="$configured_compose_project"
else
  compose_project="${NOVA_ENV_COMPOSE_PROJECT:-nr-${env_id}}"
fi

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  shared_docker="${NOVA_ENV_SHARED_DOCKER:-$shared_docker}"
  if [[ "$shared_docker" == "true" ]]; then
    compose_project="$configured_compose_project"
  else
    compose_project="${NOVA_ENV_COMPOSE_PROJECT:-$compose_project}"
  fi
fi

if [[ ! -f "$compose_env" ]]; then
  echo "environment is not initialized: $runtime_dir"
  exit 0
fi

docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  ps

echo
echo "Fixed discovery entry:"
echo "  current: $current_link"
echo "  manifest: ${NOVA_ENV_MANIFEST:-$runtime_dir/manifest.json}"
echo "  readme: ${NOVA_ENV_README:-$runtime_dir/README.md}"
echo "  shared docker: ${NOVA_ENV_SHARED_DOCKER:-$shared_docker}"
echo "  shared config: ${NOVA_ENV_CONFIG_FILE:-$config_file}"
echo
echo "Generated environment:"
echo "  env: $exports_file"
echo "  standalone config: ${NOVAROCKS_STANDALONE_CONFIG:-$runtime_dir/standalone.toml}"
echo "  scheduler standalone config: ${NOVAROCKS_STANDALONE_SCHEDULER_CONFIG:-$runtime_dir/standalone-scheduler.toml}"
echo "  sql-test config: ${NOVAROCKS_SQL_TEST_CONFIG:-$runtime_dir/sql-test.conf}"
echo "  REST catalog SQL: ${NOVAROCKS_ICE_REST_CATALOG_SQL:-$runtime_dir/ice-rest-catalog.sql}"
echo "  REST warehouse: ${NOVAROCKS_ICEBERG_REST_WAREHOUSE:-unknown}"
echo "  REST URI: ${NOVAROCKS_ICEBERG_REST_URI:-unknown}"
echo "  NovaRocks MySQL port: ${NOVA_ENV_MYSQL_PORT:-unknown}"
echo "  NovaRocks gRPC port: ${NOVA_ENV_GRPC_PORT:-unknown}"
echo "  Spark defaults: ${NOVAROCKS_SPARK_DEFAULTS:-$runtime_dir/spark-defaults.conf}"
echo "  Spark v3 smoke SQL: ${NOVAROCKS_SPARK_V3_SMOKE_SQL:-$runtime_dir/spark-iceberg-v3-smoke.sql}"
echo "  Spark SQL helper: ${NOVAROCKS_SPARK_SQL:-$SCRIPT_DIR/spark-sql.sh}"
