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
tmpdir="$(mktemp -d)"

cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

workspace="$REPO_ROOT"
runtime_dir="$tmpdir/prepared-runtime"
env_file="$runtime_dir/env.sh"
compose_env="$runtime_dir/compose.env"
compose_file="$tmpdir/compose.yml"
spark_defaults="$runtime_dir/spark-defaults.conf"
mkdir -p "$workspace" "$runtime_dir"
touch "$compose_env" "$compose_file" "$spark_defaults"

cat >"$env_file" <<EOF
export NOVA_ENV_COMPOSE_ENV="$compose_env"
export NOVA_ENV_COMPOSE_PROJECT="ci-prepared-rest"
export NOVA_ENV_COMPOSE_FILE="$compose_file"
export NOVA_ENV_SHARED_DOCKER=false
export CATALOG_WAREHOUSE_URI="s3://novarocks/ci-prepared"
export NOVA_ENV_REST_WAREHOUSE_URI="s3://novarocks/ci-prepared"
export AWS_S3_ENDPOINT="http://127.0.0.1:9000"
export AWS_S3_ACCESS_KEY_ID="admin"
export AWS_S3_SECRET_ACCESS_KEY="admin123"
export iceberg_object_store_credential_name="iceberg-test-data"
export iceberg_object_store_credential_generation="v1"
export NOVAROCKS_SPARK_DEFAULTS="$spark_defaults"
export NOVA_ENV_ID="ci-prepared"
export NOVA_ENV_SHARED_BENCHMARK_ROOT="s3://novarocks/shared/benchmarks"
export NOVA_ENV_BENCHMARK_BUILD_TIMEOUT_SECONDS="3600"
EOF

# There deliberately is no workspace/docker/iceberg-rest/runtime/current/env.sh.
# These helpers must use the explicit CI-resolved entry instead.
bootstrap_output="$tmpdir/bootstrap.out"
resolved_dataset="$tmpdir/resolved-dataset.json"
python3 "$REPO_ROOT/tests/sql/fixtures/benchmarks/resolve_benchmark_fixture.py" \
  --workspace-root "$workspace" \
  --suite ssb \
  --scale 1 \
  --shared-root s3://novarocks/shared/benchmarks >"$resolved_dataset"
NOVA_ENV_REST_ENV_FILE="$env_file" \
NOVAROCKS_WORKSPACE_ROOT="$workspace" \
  "$REPO_ROOT/tests/sql/fixtures/benchmarks/bootstrap_benchmark_data.sh" \
  --suite ssb --scale 1 --resolved-dataset "$resolved_dataset" --dry-run \
  >/dev/null 2>"$bootstrap_output"

if ! grep -Fx "env_file=$env_file" "$bootstrap_output" >/dev/null; then
  echo "benchmark bootstrap did not use the explicit REST environment file" >&2
  cat "$bootstrap_output" >&2
  exit 1
fi

fakebin="$tmpdir/bin"
mkdir -p "$fakebin"
cat >"$fakebin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_CALLS"
EOF
chmod +x "$fakebin/docker"
export DOCKER_CALLS="$tmpdir/docker.calls"
touch "$DOCKER_CALLS"
query_file="$tmpdir/query.sql"
printf 'SELECT 1;\n' >"$query_file"

PATH="$fakebin:$PATH" \
NOVA_ENV_REST_ENV_FILE="$env_file" \
NOVAROCKS_WORKSPACE_ROOT="$workspace" \
  "$REPO_ROOT/docker/iceberg-rest/spark-sql.sh" "$query_file"

if ! grep -F -- "--env-file $compose_env" "$DOCKER_CALLS" >/dev/null; then
  echo "Spark helper did not use the explicit REST environment file" >&2
  cat "$DOCKER_CALLS" >&2
  exit 1
fi

scala_file="$tmpdir/query.scala"
printf 'println(1)\n' >"$scala_file"
PATH="$fakebin:$PATH" \
NOVA_ENV_REST_ENV_FILE="$env_file" \
NOVAROCKS_WORKSPACE_ROOT="$workspace" \
  "$REPO_ROOT/docker/iceberg-rest/spark-shell.sh" "$scala_file"

if [[ "$(grep -F -- "--env-file $compose_env" "$DOCKER_CALLS" | wc -l | tr -d ' ')" -lt 2 ]]; then
  echo "Spark shell helper did not use the explicit REST environment file" >&2
  cat "$DOCKER_CALLS" >&2
  exit 1
fi

trino_helper="$REPO_ROOT/tests/datasketches-tck/interop/trino/verify_rest_catalog.sh"
trino_output="$tmpdir/trino.out"
if NOVA_ENV_REST_ENV_FILE= \
  "$trino_helper" create-parent test_namespace test_table >"$trino_output" 2>&1; then
  echo "Trino helper must reject an absent explicit REST environment file" >&2
  exit 1
fi
grep -F 'NOVA_ENV_REST_ENV_FILE must name the generated Iceberg REST environment' \
  "$trino_output" >/dev/null

if PATH="$fakebin:$PATH" \
  NOVA_ENV_SHARED_DOCKER=true \
  NOVA_ENV_REST_ENV_FILE="$env_file" \
  "$trino_helper" create-parent test_namespace test_table >"$trino_output" 2>&1; then
  echo "Trino helper unexpectedly accepted a non-shared explicit REST environment" >&2
  exit 1
fi
if ! grep -F 'the canonical shared Iceberg REST fixture is not active' "$trino_output" >/dev/null; then
  echo "Trino helper did not read the explicit REST environment file" >&2
  cat "$trino_output" >&2
  exit 1
fi

missing_secret_env="$runtime_dir/missing-secret-env.sh"
sed \
  -e 's/^export NOVA_ENV_SHARED_DOCKER=false$/export NOVA_ENV_SHARED_DOCKER=true/' \
  -e '/^export AWS_S3_SECRET_ACCESS_KEY=/d' \
  "$env_file" >"$missing_secret_env"
if PATH="$fakebin:$PATH" \
  AWS_S3_SECRET_ACCESS_KEY=ambient-must-not-fill \
  NOVA_ENV_REST_ENV_FILE="$missing_secret_env" \
  "$trino_helper" create-parent test_namespace test_table >"$trino_output" 2>&1; then
  echo "Trino helper must reject a runtime entry with a missing S3 secret" >&2
  exit 1
fi
if ! grep -F 'generated Iceberg REST environment has no S3 secret key' "$trino_output" >/dev/null; then
  echo "Trino helper allowed ambient state to fill a missing runtime fact" >&2
  cat "$trino_output" >&2
  exit 1
fi

echo "sql-helper-env-file-test: PASS"
