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

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
runtime_env="${NOVA_ENV_REST_ENV_FILE:-}"

usage() {
  cat >&2 <<'EOF'
Usage:
  verify_rest_catalog.sh create-parent NAMESPACE TABLE
  verify_rest_catalog.sh verify-current NAMESPACE TABLE COLUMN EXPECTED_ROWS EXPECTED_NDV EXPECTED_MIN EXPECTED_MAX

The script starts one transient Trino reference container on the existing
Iceberg REST fixture network. NOVA_TRINO_REFERENCE_IMAGE must contain the
image ID resolved by the run entry from the fixture input declaration.
EOF
}

fail() {
  echo "Trino REST interop failed: $*" >&2
  exit 1
}

require_identifier() {
  local value=$1
  local label=$2
  [[ "$value" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]] || fail "$label is not a safe SQL identifier: $value"
}

if [[ $# -lt 3 ]]; then
  usage
  exit 2
fi

action=$1
namespace=$2
table=$3
require_identifier "$namespace" namespace
require_identifier "$table" table

case "$action" in
  create-parent)
    [[ $# -eq 3 ]] || { usage; exit 2; }
    ;;
  verify-current)
    [[ $# -eq 8 ]] || { usage; exit 2; }
    column=$4
    expected_rows=$5
    expected_ndv=$6
    expected_min=$7
    expected_max=$8
    require_identifier "$column" column
    [[ "$expected_rows" =~ ^[0-9]+$ ]] || fail "EXPECTED_ROWS must be an integer"
    [[ "$expected_ndv" =~ ^[0-9]+$ ]] || fail "EXPECTED_NDV must be an integer"
    [[ "$expected_min" =~ ^-?[0-9]+$ ]] || fail "EXPECTED_MIN must be an integer"
    [[ "$expected_max" =~ ^-?[0-9]+$ ]] || fail "EXPECTED_MAX must be an integer"
    ;;
  *)
    usage
    exit 2
    ;;
esac

[[ -n "$runtime_env" ]] || \
  fail "NOVA_ENV_REST_ENV_FILE must name the generated Iceberg REST environment"
runtime_env="$(python3 -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).resolve())' "$runtime_env")"
[[ -f "$runtime_env" ]] || fail "missing generated Iceberg REST environment: $runtime_env"
# Runtime facts must come entirely from the named generated entry. Ambient
# values can belong to another worktree and must not fill omissions in it.
unset NOVA_ENV_SHARED_DOCKER NOVA_ENV_COMPOSE_PROJECT NOVA_ENV_REST_WAREHOUSE_URI
unset AWS_S3_ACCESS_KEY_ID AWS_S3_SECRET_ACCESS_KEY
# shellcheck source=/dev/null
source "$runtime_env"
NOVA_FIXTURE_RUN_CONTEXT="$(python3 "$script_dir/../../../../docker/iceberg-rest/fixture_run.py" context --env-file "$runtime_env")"
export NOVA_FIXTURE_RUN_CONTEXT
trino_owner=(python3 "$script_dir/../../../../docker/iceberg-rest/trino_run.py" --env-file "$runtime_env")
[[ "${NOVA_ENV_READY:-}" == true ]] || fail "the generated Iceberg REST fixture is not ready"
[[ -n "${NOVA_ENV_COMPOSE_PROJECT:-}" ]] || fail "generated Iceberg REST environment has no compose project"
[[ -n "${NOVA_ENV_REST_WAREHOUSE_URI:-}" ]] || fail "generated Iceberg REST environment has no warehouse URI"
[[ -n "${AWS_S3_ACCESS_KEY_ID:-}" ]] || fail "generated Iceberg REST environment has no S3 access key"
[[ -n "${AWS_S3_SECRET_ACCESS_KEY:-}" ]] || fail "generated Iceberg REST environment has no S3 secret key"
network="$(python3 - "$runtime_env" "$script_dir" <<'PYNETWORK'
import pathlib,sys
sys.path.insert(0,str(pathlib.Path(sys.argv[2]).resolve().parents[3]/"docker/iceberg-rest"))
import fixture_run
values,manifest,_ = fixture_run.admit(sys.argv[1])
shared = manifest["shared_docker"]
if not isinstance(shared,bool) or values.get("NOVA_ENV_SHARED_DOCKER") != str(shared).lower():
    raise SystemExit("generated fixture storage owner differs from its manifest")
record = manifest["runtime"]["machine" if shared else "catalog"]
print(record["network"])
PYNETWORK
)"
docker network inspect "$network" >/dev/null 2>&1 || fail "missing Docker network $network; run docker/iceberg-rest/up.sh"
schema_location=""
if [[ "$action" == create-parent ]]; then
  python3 "$script_dir/../../../../docker/iceberg-rest/fixture_run.py" sweep-start --env-file "$runtime_env" >&2
  if [[ "$NOVA_ENV_SHARED_DOCKER" == true ]]; then
    schema_location="$(python3 - "$NOVA_FIXTURE_RUN_CONTEXT" "$namespace" "$script_dir" <<'PYCTX'
import json,pathlib,sys
sys.path.insert(0,str(pathlib.Path(sys.argv[3]).resolve().parents[3]/"docker/iceberg-rest"))
from scratch import resource_run
value=json.loads(sys.argv[1])
if value is None or resource_run(sys.argv[2]) != value["identity"]["run_id"]:
    raise SystemExit("shared Trino writer namespace must belong to the original scratch run")
print(value["identity"]["scope_root"]+"/rest/"+sys.argv[2])
PYCTX
)"
  else
    # The original private publication owns this entire object store. Keep the
    # explicit table location and the actual-file prefix check without minting a
    # shared-machine scratch context.
    schema_location="${NOVA_ENV_REST_WAREHOUSE_URI%/}/$namespace"
  fi
fi

trino_image="${NOVA_TRINO_REFERENCE_IMAGE:?Trino reference is resolved by the run entry; run the trino-reference suite or docker/fixture-inputs/provision.sh --consumer image:trino --result <file>}"

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-trino-rest-interop.XXXXXX")
container_name="nr-trino-${$}-$(date +%s)"
cleanup() {
  local primary=$? cleanup=0
  trap - EXIT INT TERM
  if [[ -n "${container_id:-}" ]]; then
    "${trino_owner[@]}" stop --container "$container_id" || cleanup=$?
  fi
  rm -rf "$work_dir" || cleanup=$?
  (( primary == 0 )) || exit "$primary"
  exit "$cleanup"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
container_id="$("${trino_owner[@]}" start --image "$trino_image" --name "$container_name" --config-dir "$script_dir/rest-catalog")"
container_name="$container_id"
printf '%s\n' "$container_id" > "$work_dir/container-id"

healthy=false
for _ in $(seq 1 120); do
  health_state=$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_name" 2>/dev/null || true)
  if [[ "$health_state" == healthy ]]; then
    healthy=true
    break
  fi
  running=$(docker inspect --format '{{.State.Running}}' "$container_name" 2>/dev/null || true)
  if [[ "$running" != true ]]; then
    docker logs "$container_name" >&2 || true
    fail "Trino container exited before becoming healthy"
  fi
  sleep 1
done
if [[ "$healthy" != true ]]; then
  docker logs "$container_name" >&2 || true
  fail "Trino container did not become healthy within 120 seconds"
fi

trino() {
  "${trino_owner[@]}" exec --container "$container_name" -- /usr/bin/trino --server http://localhost:8080 "$@"
}

show_stats_ndv() {
  local stats_file=$1
  local target_column=$2
  awk -F '\t' -v column="$target_column" '
    NR == 1 {
      if ($1 != "column_name" || $3 != "distinct_values_count") {
        exit 10
      }
      next
    }
    $1 == column {
      matches++
      value = $3
    }
    END {
      if (matches != 1) {
        exit 11
      }
      print value
    }
  ' "$stats_file"
}

qualified="iceberg.${namespace}.${table}"
qualified_snapshots="iceberg.${namespace}.\"${table}\$snapshots\""
if [[ "$action" == create-parent ]]; then
  trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.${namespace} WITH (location = '$schema_location')" >/dev/null
  trino --execute "DROP TABLE IF EXISTS ${qualified}" >/dev/null
  trino --execute "CREATE TABLE ${qualified} (id BIGINT, k BIGINT) WITH (format_version = 3, location = '$schema_location/$table')" >/dev/null
  trino --execute "INSERT INTO ${qualified} VALUES (1, 10), (2, 20)" >/dev/null
  actual_files="$(trino --output-format TSV --execute "SELECT file_path FROM iceberg.${namespace}.\"${table}\$files\"")"
  [[ -n "$actual_files" ]] || fail "Trino parent omitted actual data files"
  while IFS= read -r actual_file; do
    [[ "$actual_file" == "$schema_location/$table/"* ]] || fail "Trino parent data escaped its original scratch location"
  done <<<"$actual_files"
  trino --execute "ANALYZE ${qualified}" >/dev/null
  source_facts=$(trino --output-format TSV --execute \
    "SELECT format('%s|%s|%s|%s', count(*), count(DISTINCT id), min(id), max(id)) FROM ${qualified}")
  [[ "$source_facts" == "2|2|1|2" ]] || fail "Trino parent data source mismatch: $source_facts"
  trino --output-format TSV_HEADER --execute "SHOW STATS FOR ${qualified}" >"$work_dir/show-stats.tsv"
  stats_ndv=$(show_stats_ndv "$work_dir/show-stats.tsv" id) || \
    fail "Trino parent SHOW STATS did not contain exactly one id/distinct_values_count cell"
  [[ "$stats_ndv" =~ ^[0-9]+([.][0-9]+)?([Ee][+-]?[0-9]+)?$ ]] || \
    fail "Trino parent SHOW STATS returned unknown/non-numeric id NDV: ${stats_ndv:-<empty>}"
  awk -v actual="$stats_ndv" 'BEGIN { delta = actual - 2; if (delta < 0) delta = -delta; exit(delta <= 0.01 ? 0 : 1) }' || \
    fail "Trino parent SHOW STATS expected id NDV 2, got $stats_ndv"
  echo "TRINO_PUFFIN_PARENT_READY rows=2 exact_ndv=2 stats_ndv=$stats_ndv"
  exit 0
fi

source_facts=$(trino --output-format TSV --execute \
  "SELECT format('%s|%s|%s|%s', count(*), count(DISTINCT ${column}), min(${column}), max(${column})) FROM ${qualified}")
expected_facts="${expected_rows}|${expected_ndv}|${expected_min}|${expected_max}"
[[ "$source_facts" == "$expected_facts" ]] || \
  fail "Trino current data source expected $expected_facts (rows|exact_ndv|min|max), got $source_facts"

trino --output-format TSV_HEADER --execute "SHOW STATS FOR ${qualified}" >"$work_dir/show-stats.tsv"
stats_ndv=$(show_stats_ndv "$work_dir/show-stats.tsv" "$column") || \
  fail "Trino SHOW STATS did not contain exactly one $column/distinct_values_count cell"
[[ "$stats_ndv" =~ ^[0-9]+([.][0-9]+)?([Ee][+-]?[0-9]+)?$ ]] || \
  fail "Trino SHOW STATS returned unknown/non-numeric $column NDV: ${stats_ndv:-<empty>}"
awk -v actual="$stats_ndv" -v expected="$expected_ndv" \
  'BEGIN { delta = actual - expected; if (delta < 0) delta = -delta; exit(delta <= 0.01 ? 0 : 1) }' || \
  fail "Trino SHOW STATS expected $column NDV $expected_ndv, got $stats_ndv"

current_snapshot=$(trino --output-format TSV --execute \
  "SELECT CAST(snapshot_id AS VARCHAR) FROM ${qualified_snapshots} ORDER BY committed_at DESC, snapshot_id DESC LIMIT 1")
[[ "$current_snapshot" =~ ^[0-9]+$ ]] || fail "Trino did not resolve a numeric current snapshot id"
echo "TRINO_NOVA_CURRENT_PUFFIN_OK snapshot=$current_snapshot exact_facts=$source_facts column=$column stats_ndv=$stats_ndv"
