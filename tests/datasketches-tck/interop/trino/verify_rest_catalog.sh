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
readonly trino_release=483
readonly trino_manifest_digest=sha256:db58cc93e593a2706553745f276bb119c9810e69918be56ecde088ba7ccb0534
readonly default_trino_image="trinodb/trino@${trino_manifest_digest}"

usage() {
  cat >&2 <<'EOF'
Usage:
  verify_rest_catalog.sh create-parent NAMESPACE TABLE
  verify_rest_catalog.sh verify-current NAMESPACE TABLE COLUMN EXPECTED_ROWS EXPECTED_NDV EXPECTED_MIN EXPECTED_MAX

The script starts one transient released Trino 483 container on the existing
Iceberg REST fixture network. It never pulls: the frozen manifest must already
be in this host's image store, under any local name. NOVA_TRINO_IMAGE may name
a specific local reference, but it must retain the frozen manifest digest.
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

trino_image_exposes_pin() {
  local candidate=$1 id digests
  id=$(docker image inspect "$candidate" --format '{{.Id}}' 2>/dev/null) || return 1
  # A containerd image store reports the manifest digest as the image id; a
  # graphdriver store reports the config digest and carries the manifest
  # digest in RepoDigests. Either one proves the frozen identity.
  if [[ "$id" == "$trino_manifest_digest" ]]; then
    return 0
  fi
  digests=$(docker image inspect "$candidate" \
    --format '{{range .RepoDigests}}{{println .}}{{end}}' 2>/dev/null || true)
  grep -Fq "@$trino_manifest_digest" <<<"$digests"
}

resolve_local_trino_image() {
  # Design: ADR-0141 (docs/adr/ADR-0141-fixture-images-never-pull.md)
  # The frozen manifest digest is the authority for which image may run; the
  # name it carries on this host is not. A mirror pull records
  # `<mirror>/trinodb/trino@<digest>` rather than `trinodb/trino@<digest>`,
  # and `docker tag` cannot create a digest reference to paper over that, so
  # look the pinned manifest up under every name it could have locally. A
  # candidate that is present but carries a different manifest is fatal, not
  # skipped.
  local candidate
  for candidate in "$@"; do
    [[ -n "$candidate" ]] || continue
    docker image inspect "$candidate" >/dev/null 2>&1 || continue
    if trino_image_exposes_pin "$candidate"; then
      printf '%s' "$candidate"
      return 0
    fi
    fail "local image $candidate does not expose frozen Trino manifest $trino_manifest_digest"
  done
  return 1
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
[[ -f "$runtime_env" ]] || fail "missing generated Iceberg REST environment: $runtime_env"
# Runtime facts must come entirely from the named generated entry. Ambient
# values can belong to another worktree and must not fill omissions in it.
unset NOVA_ENV_SHARED_DOCKER NOVA_ENV_COMPOSE_PROJECT NOVA_ENV_REST_WAREHOUSE_URI
unset AWS_S3_ACCESS_KEY_ID AWS_S3_SECRET_ACCESS_KEY
# shellcheck source=/dev/null
source "$runtime_env"

[[ "${NOVA_ENV_SHARED_DOCKER:-}" == true ]] || fail "the canonical shared Iceberg REST fixture is not active"
[[ -n "${NOVA_ENV_COMPOSE_PROJECT:-}" ]] || fail "generated Iceberg REST environment has no compose project"
[[ -n "${NOVA_ENV_REST_WAREHOUSE_URI:-}" ]] || fail "generated Iceberg REST environment has no warehouse URI"
[[ -n "${AWS_S3_ACCESS_KEY_ID:-}" ]] || fail "generated Iceberg REST environment has no S3 access key"
[[ -n "${AWS_S3_SECRET_ACCESS_KEY:-}" ]] || fail "generated Iceberg REST environment has no S3 secret key"
network="${NOVA_ENV_COMPOSE_PROJECT}_iceberg_net"
docker network inspect "$network" >/dev/null 2>&1 || fail "missing Docker network $network; run docker/iceberg-rest/up.sh"

if [[ -n "${NOVA_TRINO_IMAGE:-}" ]]; then
  [[ "$NOVA_TRINO_IMAGE" == *@"$trino_manifest_digest" ]] || \
    fail "NOVA_TRINO_IMAGE must pin released Trino $trino_release manifest $trino_manifest_digest"
fi
# Interop runs never pull: the frozen manifest must already be in this host's
# image store, under any name. Resolving it before the container starts also
# proves the identity without having run an unverified image.
trino_candidates=(
  "${NOVA_TRINO_IMAGE:-}"
  "$default_trino_image"
  # A containerd image store keys images by manifest digest, so this finds the
  # frozen manifest whatever local name it carries -- including a mirror name
  # that no `docker tag` could rewrite into `trinodb/trino@<digest>`.
  "$trino_manifest_digest"
)
if ! trino_image=$(resolve_local_trino_image "${trino_candidates[@]}"); then
  cat >&2 <<EOF
Missing local image (Trino $trino_release manifest $trino_manifest_digest)

This interop check never pulls during a run. Import the frozen manifest once,
then re-run:
  docker pull $default_trino_image

If the daemon cannot reach Docker Hub, pull the same digest through a
reachable mirror; no further setup is needed, because the manifest is then
found under the mirror's own name:
  docker pull dockerproxy.net/trinodb/trino@$trino_manifest_digest

Local references tried:
$(printf '  %s\n' "${trino_candidates[@]}")
EOF
  exit 1
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-trino-rest-interop.XXXXXX")
container_name="nr-trino483-${$}-$(date +%s)"
cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
  rm -rf "$work_dir"
}
trap cleanup EXIT INT TERM

docker run --detach --rm \
  --pull never \
  --name "$container_name" \
  --label org.novarocks.test=trino-rest-interop \
  --network "$network" \
  --env NOVA_TRINO_REST_WAREHOUSE="$NOVA_ENV_REST_WAREHOUSE_URI" \
  --env NOVA_TRINO_S3_ACCESS_KEY="$AWS_S3_ACCESS_KEY_ID" \
  --env NOVA_TRINO_S3_SECRET_KEY="$AWS_S3_SECRET_ACCESS_KEY" \
  --mount "type=bind,src=$script_dir/rest-catalog,dst=/etc/trino,readonly" \
  "$trino_image" >"$work_dir/container-id"

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
  docker exec "$container_name" /usr/bin/trino --server http://localhost:8080 "$@"
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

actual_version=$(trino --output-format TSV --execute 'SELECT version()')
[[ "$actual_version" == "$trino_release" ]] || fail "expected Trino $trino_release, got $actual_version"

qualified="iceberg.${namespace}.${table}"
qualified_snapshots="iceberg.${namespace}.\"${table}\$snapshots\""
if [[ "$action" == create-parent ]]; then
  trino --execute "CREATE SCHEMA IF NOT EXISTS iceberg.${namespace}" >/dev/null
  trino --execute "DROP TABLE IF EXISTS ${qualified}" >/dev/null
  trino --execute "CREATE TABLE ${qualified} (id BIGINT, k BIGINT) WITH (format_version = 3)" >/dev/null
  trino --execute "INSERT INTO ${qualified} VALUES (1, 10), (2, 20)" >/dev/null
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
  echo "TRINO_PUFFIN_PARENT_READY version=$actual_version rows=2 exact_ndv=2 stats_ndv=$stats_ndv"
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
echo "TRINO_NOVA_CURRENT_PUFFIN_OK version=$actual_version snapshot=$current_snapshot exact_facts=$source_facts column=$column stats_ndv=$stats_ndv"
