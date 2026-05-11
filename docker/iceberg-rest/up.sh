#!/usr/bin/env bash
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
manifest_file="$runtime_dir/manifest.json"
readme_file="$runtime_dir/README.md"
spark_defaults_file="$runtime_dir/spark-defaults.conf"
spark_v3_smoke_sql="$runtime_dir/spark-iceberg-v3-smoke.sql"
spark_sql_script="$SCRIPT_DIR/spark-sql.sh"
config_file="${NOVA_ENV_CONFIG_FILE:-$SCRIPT_DIR/shared.env}"
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
if [[ "$shared_docker" == "true" ]]; then
  compose_project="${NOVA_ENV_SHARED_COMPOSE_PROJECT:-nr-iceberg-rest}"
else
  compose_project="${NOVA_ENV_COMPOSE_PROJECT:-nr-${env_id}}"
fi

mkdir -p "$runtime_dir"

port_in_use() {
  local port="$1"
  lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1
}

choose_port() {
  local start="$1"
  local range="${2:-200}"
  local port="$start"
  local limit=$((start + range))
  while port_in_use "$port"; do
    port=$((port + 1))
    if (( port > limit )); then
      echo "no free port near $start" >&2
      return 1
    fi
  done
  printf '%s\n' "$port"
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

docker_image_exists() {
  local image="$1"
  local timeout_seconds="${NOVA_ENV_DOCKER_INSPECT_TIMEOUT_SECONDS:-15}"

  docker image inspect "$image" >/dev/null 2>&1 &
  local pid="$!"
  local elapsed=0
  while kill -0 "$pid" >/dev/null 2>&1; do
    if (( elapsed >= timeout_seconds )); then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$pid"
}

hash4="${hash:0:4}"
offset=$((16#$hash4 % 1000))
mysql_port_start="${NOVA_ENV_MYSQL_PORT_START:-9030}"
mysql_port_range="${NOVA_ENV_MYSQL_PORT_RANGE:-200}"
configured_compose_project="${NOVA_ENV_SHARED_COMPOSE_PROJECT:-nr-iceberg-rest}"
configured_minio_port="${NOVA_ENV_MINIO_PORT:-9000}"
configured_minio_console_port="${NOVA_ENV_MINIO_CONSOLE_PORT:-9001}"
configured_rest_port="${NOVA_ENV_REST_PORT:-8181}"
configured_spark_ui_port="${NOVA_ENV_SPARK_UI_PORT:-4040}"

if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  if [[ "$shared_docker" == "true" ]]; then
    minio_port="$configured_minio_port"
    minio_console_port="$configured_minio_console_port"
    rest_port="$configured_rest_port"
    spark_ui_port="$configured_spark_ui_port"
    compose_project="$configured_compose_project"
  else
    minio_port="${NOVA_ENV_MINIO_PORT}"
    minio_console_port="${NOVA_ENV_MINIO_CONSOLE_PORT}"
    rest_port="${NOVA_ENV_REST_PORT}"
    spark_ui_port="${NOVA_ENV_SPARK_UI_PORT:-4040}"
  fi
  mysql_port="${NOVA_ENV_MYSQL_PORT}"
else
  if [[ "$shared_docker" == "true" ]]; then
    minio_port="$configured_minio_port"
    minio_console_port="$configured_minio_console_port"
    rest_port="$configured_rest_port"
    spark_ui_port="$configured_spark_ui_port"
    mysql_port="$(choose_port_in_range "$mysql_port_start" "$mysql_port_range" "$offset")"
  else
    minio_port="$(choose_port $((19000 + offset)))"
    minio_console_port="$(choose_port $((20000 + offset)))"
    rest_port="$(choose_port $((21000 + offset)))"
    spark_ui_port="$(choose_port $((22000 + offset)))"
    mysql_port="$(choose_port $((23000 + offset)))"
  fi
fi
shared_docker="${NOVA_ENV_SHARED_DOCKER:-$shared_docker}"
if [[ "$shared_docker" == "true" ]]; then
  compose_project="$configured_compose_project"
else
  compose_project="${NOVA_ENV_COMPOSE_PROJECT:-$compose_project}"
fi

minio_user="${MINIO_ROOT_USER:-admin}"
minio_password="${MINIO_ROOT_PASSWORD:-admin123}"
rest_image="${ICEBERG_REST_IMAGE:-apache/iceberg-rest-fixture:1.8.1}"
rest_mirror_image="dockerproxy.net/apache/iceberg-rest-fixture:1.8.1"
spark_image="${SPARK_ICEBERG_IMAGE:-tabulario/spark-iceberg:3.5.5_1.8.1}"
spark_mirror_image="docker.1panel.live/tabulario/spark-iceberg:3.5.5_1.8.1"

if [[ "$prepare_only" != true ]]; then
  if docker_image_exists "$rest_image"; then
    rest_status=0
  else
    rest_status="$?"
  fi
  if [[ "$rest_status" -ne 0 ]]; then
    if [[ "$rest_status" -eq 124 ]]; then
      cat >&2 <<EOF
Docker image inspect timed out for: $rest_image

Docker Desktop may be unhealthy. Check it manually before running setup again:
  docker image inspect $rest_image
EOF
      exit 1
    fi
    if [[ "$rest_image" == "apache/iceberg-rest-fixture:1.8.1" ]] \
      && docker_image_exists "$rest_mirror_image"; then
      docker tag "$rest_mirror_image" "$rest_image"
    else
      cat >&2 <<EOF
Missing Iceberg REST image: $rest_image

Pull it first, for example:
  docker pull --platform linux/arm64 dockerproxy.net/apache/iceberg-rest-fixture:1.8.1
  docker tag dockerproxy.net/apache/iceberg-rest-fixture:1.8.1 apache/iceberg-rest-fixture:1.8.1

Or set ICEBERG_REST_IMAGE to an already available image.
EOF
      exit 1
    fi
  fi

  if docker_image_exists "$spark_image"; then
    spark_status=0
  else
    spark_status="$?"
  fi
  if [[ "$spark_status" -ne 0 ]]; then
    if [[ "$spark_status" -eq 124 ]]; then
      cat >&2 <<EOF
Docker image inspect timed out for: $spark_image

Docker Desktop may be unhealthy. Check it manually before running setup again:
  docker image inspect $spark_image
EOF
      exit 1
    fi
    if [[ "$spark_image" == "tabulario/spark-iceberg:3.5.5_1.8.1" ]] \
      && docker_image_exists "$spark_mirror_image"; then
      docker tag "$spark_mirror_image" "$spark_image"
    else
      cat >&2 <<EOF
Missing Spark Iceberg image: $spark_image

Pull it first, for example:
  docker pull docker.1panel.live/tabulario/spark-iceberg:3.5.5_1.8.1
  docker tag docker.1panel.live/tabulario/spark-iceberg:3.5.5_1.8.1 tabulario/spark-iceberg:3.5.5_1.8.1

Or set SPARK_ICEBERG_IMAGE to an already available image that contains spark-sql
and the Iceberg Spark runtime.
EOF
      exit 1
    fi
  fi
fi

managed_warehouse="s3://novarocks/$env_id/sql-tests-managed-lake"
iceberg_warehouse="s3://novarocks/$env_id/iceberg-catalog"
rest_warehouse="s3://warehouse/$env_id/rest"
shared_rest_warehouse="${NOVA_ENV_SHARED_REST_WAREHOUSE_URI:-s3://warehouse/shared/rest}"
compose_rest_warehouse="$rest_warehouse"
if [[ "$shared_docker" == "true" ]]; then
  compose_rest_warehouse="$shared_rest_warehouse"
fi
minio_endpoint="http://127.0.0.1:$minio_port"
rest_uri="http://127.0.0.1:$rest_port"
spark_minio_endpoint="http://minio:9000"
spark_rest_uri="http://rest:8181"

cat > "$compose_env" <<EOF
NOVA_ENV_SHARED_DOCKER=$shared_docker
NOVA_ENV_COMPOSE_PROJECT=$compose_project
NOVA_ENV_RUNTIME_DIR=$runtime_dir
NOVA_ENV_MINIO_PORT=$minio_port
NOVA_ENV_MINIO_CONSOLE_PORT=$minio_console_port
NOVA_ENV_REST_PORT=$rest_port
NOVA_ENV_SPARK_UI_PORT=$spark_ui_port
NOVA_ENV_REST_WAREHOUSE_URI=$compose_rest_warehouse
MINIO_ROOT_USER=$minio_user
MINIO_ROOT_PASSWORD=$minio_password
ICEBERG_REST_IMAGE=$rest_image
SPARK_ICEBERG_IMAGE=$spark_image
EOF

cat > "$runtime_dir/standalone-managed-lake.toml" <<EOF
[server]
host = "127.0.0.1"

[metadata]
provider = "sqlite"
path = "$runtime_dir/standalone-managed-lake.sqlite"

[standalone_server]
mysql_port = $mysql_port
user = "root"
warehouse_uri = "$managed_warehouse"

[standalone_server.object_store]
endpoint = "$minio_endpoint"
access_key_id = "$minio_user"
access_key_secret = "$minio_password"
enable_path_style_access = true
EOF

cat > "$runtime_dir/sql-test.conf" <<EOF
[cluster]
host = 127.0.0.1
port = $mysql_port
user = root
password =

[env]
url = http://127.0.0.1:8030
oss_ak = $minio_user
oss_sk = $minio_password
oss_endpoint = $minio_endpoint
managed_lake_warehouse = $managed_warehouse
iceberg_catalog_type = hadoop
iceberg_catalog_warehouse = $iceberg_warehouse
iceberg_rest_uri = $rest_uri
iceberg_rest_warehouse = $rest_warehouse
EOF

cat > "$runtime_dir/ice-rest-catalog.sql" <<EOF
CREATE EXTERNAL CATALOG ice_rest
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "$rest_uri",
  "warehouse" = "$rest_warehouse",
  "aws.s3.endpoint" = "$minio_endpoint",
  "aws.s3.access_key" = "$minio_user",
  "aws.s3.secret_key" = "$minio_password",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
EOF

cat > "$spark_defaults_file" <<EOF
spark.master local[*]
spark.app.name NovaRocksIcebergSpark
spark.ui.bindAddress 0.0.0.0
spark.driver.bindAddress 0.0.0.0
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.ice_rest org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.ice_rest.type rest
spark.sql.catalog.ice_rest.uri $spark_rest_uri
spark.sql.catalog.ice_rest.warehouse $rest_warehouse
spark.sql.catalog.ice_rest.io-impl org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.ice_rest.s3.endpoint $spark_minio_endpoint
spark.sql.catalog.ice_rest.s3.path-style-access true
spark.sql.catalog.ice_rest.s3.access-key-id $minio_user
spark.sql.catalog.ice_rest.s3.secret-access-key $minio_password
spark.sql.catalog.ice_rest.s3.region us-east-1
spark.sql.defaultCatalog ice_rest
spark.hadoop.fs.s3a.endpoint $spark_minio_endpoint
spark.hadoop.fs.s3a.access.key $minio_user
spark.hadoop.fs.s3a.secret.key $minio_password
spark.hadoop.fs.s3a.path.style.access true
spark.hadoop.fs.s3a.connection.ssl.enabled false
spark.hadoop.fs.s3a.aws.credentials.provider org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
EOF

cat > "$spark_v3_smoke_sql" <<EOF
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_v3;

DROP TABLE IF EXISTS ice_rest.nr_v3.spark_v3_smoke;

CREATE TABLE ice_rest.nr_v3.spark_v3_smoke (
  id BIGINT,
  data STRING,
  category STRING,
  ts TIMESTAMP
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_v3.spark_v3_smoke VALUES
  (1, 'spark-v3-a', 'alpha', TIMESTAMP '2026-05-07 00:00:00'),
  (2, 'spark-v3-b', 'beta', TIMESTAMP '2026-05-07 00:01:00'),
  (3, 'spark-v3-c', 'alpha', TIMESTAMP '2026-05-07 00:02:00');

SELECT * FROM ice_rest.nr_v3.spark_v3_smoke ORDER BY id;
EOF

cat > "$exports_file" <<EOF
export NOVAROCKS_WORKSPACE_ROOT="$WORKSPACE_ROOT"
export NOVA_ENV_CONFIG_FILE="$config_file"
export NOVA_ENV_ID="$env_id"
export NOVA_ENV_SHARED_DOCKER="$shared_docker"
export NOVA_ENV_COMPOSE_PROJECT="$compose_project"
export NOVA_ENV_RUNTIME_DIR="$runtime_dir"
export NOVA_ENV_CURRENT_DIR="$current_link"
export NOVA_ENV_MANIFEST="$manifest_file"
export NOVA_ENV_README="$readme_file"
export NOVA_ENV_COMPOSE_FILE="$compose_file"
export NOVA_ENV_COMPOSE_ENV="$compose_env"
export NOVA_ENV_MINIO_PORT="$minio_port"
export NOVA_ENV_MINIO_CONSOLE_PORT="$minio_console_port"
export NOVA_ENV_REST_PORT="$rest_port"
export NOVA_ENV_SPARK_UI_PORT="$spark_ui_port"
export NOVA_ENV_MYSQL_PORT="$mysql_port"
export AWS_S3_ENDPOINT="$minio_endpoint"
export AWS_S3_ACCESS_KEY_ID="$minio_user"
export AWS_S3_SECRET_ACCESS_KEY="$minio_password"
export MINIO_ROOT_USER="$minio_user"
export MINIO_ROOT_PASSWORD="$minio_password"
export CATALOG_WAREHOUSE_URI="$iceberg_warehouse"
export NOVAROCKS_MANAGED_LAKE_WAREHOUSE="$managed_warehouse"
export NOVAROCKS_ICEBERG_REST_URI="$rest_uri"
export NOVA_ENV_SHARED_REST_WAREHOUSE_URI="$shared_rest_warehouse"
export NOVA_ENV_REST_SERVER_WAREHOUSE_URI="$compose_rest_warehouse"
export NOVA_ENV_REST_WAREHOUSE_URI="$rest_warehouse"
export NOVAROCKS_ICEBERG_REST_WAREHOUSE="$rest_warehouse"
export NOVAROCKS_STANDALONE_CONFIG="$runtime_dir/standalone-managed-lake.toml"
export NOVAROCKS_SQL_TEST_CONFIG="$runtime_dir/sql-test.conf"
export NOVAROCKS_ICE_REST_CATALOG_SQL="$runtime_dir/ice-rest-catalog.sql"
export NOVAROCKS_SPARK_IMAGE="$spark_image"
export NOVAROCKS_SPARK_UI="http://127.0.0.1:$spark_ui_port"
export NOVAROCKS_SPARK_REST_URI="$spark_rest_uri"
export NOVAROCKS_SPARK_S3_ENDPOINT="$spark_minio_endpoint"
export NOVAROCKS_SPARK_DEFAULTS="$spark_defaults_file"
export NOVAROCKS_SPARK_V3_SMOKE_SQL="$spark_v3_smoke_sql"
export NOVAROCKS_SPARK_SQL="$spark_sql_script"
EOF

cat > "$manifest_file" <<EOF
{
  "workspace_root": "$WORKSPACE_ROOT",
  "config_file": "$config_file",
  "env_id": "$env_id",
  "shared_docker": $shared_docker,
  "compose_project": "$compose_project",
  "runtime_dir": "$runtime_dir",
  "current_dir": "$current_link",
  "compose_file": "$compose_file",
  "compose_env": "$compose_env",
  "minio": {
    "endpoint": "$minio_endpoint",
    "console": "http://127.0.0.1:$minio_console_port",
    "access_key_id": "$minio_user",
    "secret_access_key": "$minio_password",
    "volume": "${compose_project}_minio-data"
  },
  "iceberg_rest": {
    "uri": "$rest_uri",
    "warehouse": "$rest_warehouse",
    "server_default_warehouse": "$compose_rest_warehouse"
  },
  "spark": {
    "image": "$spark_image",
    "ui": "http://127.0.0.1:$spark_ui_port",
    "container_rest_uri": "$spark_rest_uri",
    "container_minio_endpoint": "$spark_minio_endpoint",
    "defaults_file": "$spark_defaults_file",
    "v3_smoke_sql": "$spark_v3_smoke_sql",
    "helper": "$spark_sql_script"
  },
  "novarocks": {
    "mysql_port": $mysql_port,
    "standalone_config": "$runtime_dir/standalone-managed-lake.toml",
    "sql_test_config": "$runtime_dir/sql-test.conf",
    "ice_rest_catalog_sql": "$runtime_dir/ice-rest-catalog.sql",
    "managed_lake_warehouse": "$managed_warehouse",
    "iceberg_catalog_warehouse": "$iceberg_warehouse"
  }
}
EOF

cat > "$readme_file" <<EOF
# NovaRocks Local Test Environment

This file is generated by \`docker/iceberg-rest/up.sh\`.

Read this directory first when you need the active workspace test environment.
Do not guess ports.

- Workspace: \`$WORKSPACE_ROOT\`
- Environment id: \`$env_id\`
- Runtime dir: \`$runtime_dir\`
- Compose project: \`$compose_project\`
- Shared Docker: \`$shared_docker\`
- Shared config: \`$config_file\`
- MinIO endpoint: \`$minio_endpoint\`
- MinIO console: \`http://127.0.0.1:$minio_console_port\`
- Iceberg REST: \`$rest_uri\`
- Iceberg REST warehouse: \`$rest_warehouse\`
- REST server default warehouse: \`$compose_rest_warehouse\`
- Spark UI: \`http://127.0.0.1:$spark_ui_port\`
- NovaRocks MySQL port: \`$mysql_port\`
- Manifest: \`$manifest_file\`
- Env exports: \`$exports_file\`
- Standalone config: \`$runtime_dir/standalone-managed-lake.toml\`
- SQL test config: \`$runtime_dir/sql-test.conf\`
- REST catalog SQL: \`$runtime_dir/ice-rest-catalog.sql\`
- Spark defaults: \`$spark_defaults_file\`
- Spark v3 smoke SQL: \`$spark_v3_smoke_sql\`

Use:

\`\`\`bash
source "$current_link/env.sh"
docker/iceberg-rest/up.sh  # start or reuse shared Docker services when needed
cargo run -- standalone-server --config "\$NOVAROCKS_STANDALONE_CONFIG"
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify
cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-compatibility --mode verify
docker/iceberg-rest/spark-sql.sh "\$NOVAROCKS_SPARK_V3_SMOKE_SQL"
\`\`\`
EOF

rm -rf "$current_link"
ln -s "$env_id" "$current_link"

if [[ "$prepare_only" != true ]]; then
  docker compose \
    --env-file "$compose_env" \
    -p "$compose_project" \
    -f "$compose_file" \
    up -d --remove-orphans

  wait_http() {
    local url="$1"
    local name="$2"
    for _ in $(seq 1 60); do
      if curl -fsS "$url" >/dev/null 2>&1; then
        return 0
      fi
      sleep 1
    done
    echo "$name did not become ready at $url" >&2
    docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" logs --tail=120 >&2
    return 1
  }

  wait_http "$minio_endpoint/minio/health/live" "MinIO"
  wait_http "$rest_uri/v1/config" "Iceberg REST"
fi

environment_state="ready"
docker_state="Docker services are running or were reused."
docker_start_hint=""
if [[ "$prepare_only" == true ]]; then
  environment_state="prepared"
  docker_state="Docker services were not started by --prepare-only."
  docker_start_hint="  docker/iceberg-rest/up.sh"
fi

cat <<EOF
NovaRocks workspace environment is $environment_state.

Workspace: $WORKSPACE_ROOT
Environment id: $env_id
Runtime dir: $runtime_dir
Current entry: $current_link
Compose project: $compose_project
Shared Docker: $shared_docker
Shared config: $config_file

MinIO endpoint: $minio_endpoint
MinIO console: http://127.0.0.1:$minio_console_port
Iceberg REST: $rest_uri
Iceberg REST warehouse: $rest_warehouse
REST server default warehouse: $compose_rest_warehouse
Spark UI: http://127.0.0.1:$spark_ui_port
$docker_state

Use:
  source "$current_link/env.sh"
$docker_start_hint
  cargo run -- standalone-server --config "\$NOVAROCKS_STANDALONE_CONFIG"
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg --mode verify
  cargo run --manifest-path tests/sql-test-runner/Cargo.toml --bin sql-tests -- --config "\$NOVAROCKS_SQL_TEST_CONFIG" --suite iceberg-compatibility --mode verify
  docker/iceberg-rest/spark-sql.sh "\$NOVAROCKS_SPARK_V3_SMOKE_SQL"
EOF
