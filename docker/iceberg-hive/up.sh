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
REST_DIR="$WORKSPACE_ROOT/docker/iceberg-rest"

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
manifest_file="$runtime_dir/manifest.json"
readme_file="$runtime_dir/README.md"
hms_catalog_sql="$runtime_dir/ice-hms-catalog.sql"
spark_hms_defaults_file="$runtime_dir/spark-hms-defaults.conf"
config_file="${NOVA_ENV_HIVE_CONFIG_FILE:-$SCRIPT_DIR/shared.env}"
rest_exports_file="${NOVA_ENV_REST_ENV_FILE:-$REST_DIR/runtime/current/env.sh}"
prepare_only=false

for arg in "$@"; do
  case "$arg" in
    --prepare-only|--no-docker)
      prepare_only=true
      ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: $0 [--prepare-only|--no-docker]" >&2
      exit 2
      ;;
  esac
done

if [[ -f "$config_file" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$config_file"
  set +a
fi

shared_docker="${NOVA_ENV_SHARED_DOCKER:-true}"
configured_hive_compose_project="${NOVA_ENV_SHARED_HIVE_COMPOSE_PROJECT:-nr-iceberg-hive}"
configured_rest_compose_project="${NOVA_ENV_REST_COMPOSE_PROJECT:-nr-iceberg-rest}"
configured_hms_port="${NOVA_ENV_HMS_PORT:-9083}"
configured_hms_warehouse="${NOVA_ENV_SHARED_HMS_WAREHOUSE_URI:-s3://warehouse/shared/hms}"
hms_image="${HMS_IMAGE:-novarocks/hive-metastore:4.0.0}"
minio_user="${MINIO_ROOT_USER:-admin}"
minio_password="${MINIO_ROOT_PASSWORD:-admin123}"
minio_endpoint="${AWS_S3_ENDPOINT:-http://127.0.0.1:9000}"
rest_compose_project="$configured_rest_compose_project"

if [[ -f "$rest_exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$rest_exports_file"
  rest_compose_project="${NOVA_ENV_COMPOSE_PROJECT:-$rest_compose_project}"
  minio_endpoint="${AWS_S3_ENDPOINT:-$minio_endpoint}"
  minio_user="${MINIO_ROOT_USER:-$minio_user}"
  minio_password="${MINIO_ROOT_PASSWORD:-$minio_password}"
fi

rest_network="${NOVA_ENV_REST_NETWORK:-${rest_compose_project}_iceberg_net}"
if [[ "$shared_docker" == "true" ]]; then
  hive_compose_project="$configured_hive_compose_project"
else
  hive_compose_project="${NOVA_ENV_HIVE_COMPOSE_PROJECT:-nr-iceberg-hive-${env_id}}"
fi

mkdir -p "$runtime_dir"

port_in_use() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

choose_port_in_range() {
  local base="$1"
  local range="$2"
  local offset="$3"
  local slots=$((range + 1))
  local i
  local port
  for ((i = 0; i < slots; i += 1)); do
    port=$((base + ((offset + i) % slots)))
    if ! port_in_use "$port"; then
      printf '%s\n' "$port"
      return 0
    fi
  done
  echo "no free port in range $base-$((base + range))" >&2
  return 1
}

hash4="${hash:0:4}"
offset=$((16#$hash4 % 1000))
hms_port_start="${NOVA_ENV_HMS_PORT_START:-9083}"
hms_port_range="${NOVA_ENV_HMS_PORT_RANGE:-200}"

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  if [[ "$shared_docker" == "true" ]]; then
    hms_port="$configured_hms_port"
    hive_compose_project="$configured_hive_compose_project"
  else
    hms_port="${NOVA_ENV_HMS_PORT}"
    hive_compose_project="${NOVA_ENV_HIVE_COMPOSE_PROJECT:-$hive_compose_project}"
  fi
else
  if [[ "$shared_docker" == "true" ]]; then
    hms_port="$configured_hms_port"
  else
    hms_port="$(choose_port_in_range "$hms_port_start" "$hms_port_range" "$offset")"
  fi
fi

hms_uri="thrift://127.0.0.1:$hms_port"
hms_warehouse="$configured_hms_warehouse"
spark_hms_uri="thrift://hms:9083"
spark_minio_endpoint="http://minio:9000"

cat > "$compose_env" <<EOF
NOVA_ENV_SHARED_DOCKER=$shared_docker
NOVA_ENV_HIVE_COMPOSE_PROJECT=$hive_compose_project
NOVA_ENV_HIVE_RUNTIME_DIR=$runtime_dir
NOVA_ENV_REST_COMPOSE_PROJECT=$rest_compose_project
NOVA_ENV_REST_NETWORK=$rest_network
NOVA_ENV_HMS_PORT=$hms_port
NOVA_ENV_SHARED_HMS_WAREHOUSE_URI=$hms_warehouse
MINIO_ROOT_USER=$minio_user
MINIO_ROOT_PASSWORD=$minio_password
HMS_IMAGE=$hms_image
EOF

cat > "$hms_catalog_sql" <<EOF
CREATE EXTERNAL CATALOG ice_hms
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "iceberg.catalog.hive.metastore.uris" = "$hms_uri",
  "iceberg.catalog.warehouse" = "$hms_warehouse",
  "aws.s3.endpoint" = "$minio_endpoint",
  "aws.s3.access_key" = "$minio_user",
  "aws.s3.secret_key" = "$minio_password",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
EOF

cat > "$spark_hms_defaults_file" <<EOF
spark.sql.catalog.hms_catalog org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hms_catalog.type hive
spark.sql.catalog.hms_catalog.uri $spark_hms_uri
spark.sql.catalog.hms_catalog.warehouse $hms_warehouse
spark.sql.catalog.hms_catalog.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.hms_catalog.s3.endpoint $spark_minio_endpoint
spark.sql.catalog.hms_catalog.s3.path-style-access true
spark.sql.catalog.hms_catalog.s3.access-key-id $minio_user
spark.sql.catalog.hms_catalog.s3.secret-access-key $minio_password
spark.sql.catalog.hms_catalog.s3.region us-east-1
EOF

cat > "$exports_file" <<EOF
export NOVAROCKS_WORKSPACE_ROOT="$WORKSPACE_ROOT"
export NOVA_ENV_HIVE_CONFIG_FILE="$config_file"
export NOVA_ENV_REST_ENV_FILE="$rest_exports_file"
export NOVA_ENV_ID="$env_id"
export NOVA_ENV_SHARED_DOCKER="$shared_docker"
export NOVA_ENV_HIVE_RUNTIME_DIR="$runtime_dir"
export NOVA_ENV_HIVE_CURRENT_DIR="$current_link"
export NOVA_ENV_HIVE_MANIFEST="$manifest_file"
export NOVA_ENV_HIVE_README="$readme_file"
export NOVA_ENV_HIVE_COMPOSE_FILE="$compose_file"
export NOVA_ENV_HIVE_COMPOSE_ENV="$compose_env"
export NOVA_ENV_HIVE_COMPOSE_PROJECT="$hive_compose_project"
export NOVA_ENV_REST_COMPOSE_PROJECT="$rest_compose_project"
export NOVA_ENV_REST_NETWORK="$rest_network"
export NOVA_ENV_HMS_PORT="$hms_port"
export NOVAROCKS_ICEBERG_HMS_URI="$hms_uri"
export NOVA_ENV_SHARED_HMS_WAREHOUSE_URI="$hms_warehouse"
export NOVAROCKS_ICEBERG_HMS_WAREHOUSE="$hms_warehouse"
export NOVAROCKS_ICE_HMS_CATALOG_SQL="$hms_catalog_sql"
export NOVAROCKS_SPARK_HMS_DEFAULTS="$spark_hms_defaults_file"
export NOVAROCKS_SPARK_EXTRA_DEFAULTS="$spark_hms_defaults_file"
export HMS_IMAGE="$hms_image"
EOF

cat > "$manifest_file" <<EOF
{
  "workspace_root": "$WORKSPACE_ROOT",
  "config_file": "$config_file",
  "env_id": "$env_id",
  "shared_docker": $shared_docker,
  "hive_compose_project": "$hive_compose_project",
  "rest_compose_project": "$rest_compose_project",
  "rest_network": "$rest_network",
  "runtime_dir": "$runtime_dir",
  "current_dir": "$current_link",
  "compose_file": "$compose_file",
  "compose_env": "$compose_env",
  "hms": {
    "uri": "$hms_uri",
    "port": $hms_port,
    "warehouse": "$hms_warehouse",
    "catalog_sql": "$hms_catalog_sql",
    "spark_defaults": "$spark_hms_defaults_file",
    "image": "$hms_image"
  },
  "minio": {
    "endpoint": "$minio_endpoint",
    "access_key_id": "$minio_user",
    "secret_access_key": "$minio_password"
  }
}
EOF

cat > "$readme_file" <<EOF
# NovaRocks Iceberg Hive Metastore Environment

This file is generated by \`docker/iceberg-hive/up.sh\`.

The Hive Metastore fixture is separate from \`docker/iceberg-rest\`. It joins
the REST fixture's Docker network so HMS can reach MinIO at \`http://minio:9000\`,
but it is managed by its own Compose project.

- Workspace: \`$WORKSPACE_ROOT\`
- Environment id: \`$env_id\`
- Runtime dir: \`$runtime_dir\`
- Hive Compose project: \`$hive_compose_project\`
- REST Compose project: \`$rest_compose_project\`
- REST Docker network: \`$rest_network\`
- HMS URI: \`$hms_uri\`
- HMS warehouse: \`$hms_warehouse\`
- HMS catalog SQL: \`$hms_catalog_sql\`
- Spark HMS defaults: \`$spark_hms_defaults_file\`
- Manifest: \`$manifest_file\`
- Env exports: \`$exports_file\`

Use:

\`\`\`bash
docker/iceberg-rest/up.sh
docker/iceberg-hive/up.sh
source docker/iceberg-rest/runtime/current/env.sh
source docker/iceberg-hive/runtime/current/env.sh
cargo run --manifest-path tests/sql/runner/Cargo.toml -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms --mode verify
docker/iceberg-rest/spark-sql.sh /path/to/hms-catalog-query.sql
\`\`\`
EOF

rm -rf "$current_link"
ln -s "$env_id" "$current_link"

if [[ "$prepare_only" != true ]]; then
  if ! docker network inspect "$rest_network" >/dev/null 2>&1; then
    cat >&2 <<EOF
Missing Docker network: $rest_network

Start the REST/MinIO fixture first:
  docker/iceberg-rest/up.sh

Then start the standalone Hive fixture:
  docker/iceberg-hive/up.sh
EOF
    exit 1
  fi

  # The hms image is built here, and its Dockerfile FROM is a tag that
  # BuildKit pulls when it is not already local. Fixtures never pull during a
  # run, so require the base up front; `pull_policy: never` covers the rest.
  hive_base_image="apache/hive:4.0.0"
  if ! docker image inspect "$hive_base_image" >/dev/null 2>&1; then
    cat >&2 <<EOF
Missing local image (Hive Metastore base): $hive_base_image

This fixture never pulls during a run. Import it once, then re-run:
  docker pull $hive_base_image
EOF
    exit 1
  fi

  docker compose \
    --env-file "$compose_env" \
    -p "$hive_compose_project" \
    -f "$compose_file" \
    up -d --build --remove-orphans

  wait_tcp() {
    local host="$1" port="$2" name="$3"
    for _ in $(seq 1 60); do
      if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
        exec 3>&- 3<&-
        return 0
      fi
      sleep 1
    done
    echo "timed out waiting for $name on $host:$port" >&2
    docker compose --env-file "$compose_env" -p "$hive_compose_project" -f "$compose_file" logs --tail=120 hms >&2
    return 1
  }

  wait_tcp 127.0.0.1 "$hms_port" "Hive Metastore"
fi

environment_state="ready"
docker_state="Docker services are running or were reused."
docker_start_hint=""
if [[ "$prepare_only" == true ]]; then
  environment_state="prepared"
  docker_state="Docker services were not started by --prepare-only."
  docker_start_hint="  docker/iceberg-rest/up.sh
  docker/iceberg-hive/up.sh"
fi

cat <<EOF
NovaRocks Iceberg Hive environment is $environment_state.

Workspace: $WORKSPACE_ROOT
Environment id: $env_id
Runtime dir: $runtime_dir
Current entry: $current_link
Hive Compose project: $hive_compose_project
REST Compose project: $rest_compose_project
REST Docker network: $rest_network
Shared Docker: $shared_docker
Shared config: $config_file

HMS URI: $hms_uri
HMS warehouse: $hms_warehouse
$docker_state

Use:
  source "$current_link/env.sh"
$docker_start_hint
  cargo run --manifest-path tests/sql/runner/Cargo.toml -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms --mode verify
  cargo run --manifest-path tests/sql/runner/Cargo.toml -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-hms-compatibility --mode verify
EOF
