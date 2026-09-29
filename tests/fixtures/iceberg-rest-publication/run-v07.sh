#!/usr/bin/env bash
#
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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
image_name="${UEA7_FIXTURE_IMAGE:-novarocks/iceberg-rest-publication-fixture:test}"
artifact_dir="${UEA7_ARTIFACT_DIR:-}"
storage_mode="local-hadoop"
v11_enabled="${UEA7_RUN_V11:-0}"
[[ "$v11_enabled" == "0" || "$v11_enabled" == "1" ]] \
  || { echo "UEA7_RUN_V11 must be 0 or 1" >&2; exit 1; }
name_suffix="$(printf '%s-%s' "${USER:-user}" "$$" | tr -cd '[:alnum:]-')"
container_name="novarocks-uea7-publication-$name_suffix"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/novarocks-uea7-v07.XXXXXX")"
shared_bucket=""
shared_bucket_created=0
minio_container=""
minio_host=""

cleanup_shared_bucket() {
  if [[ "$shared_bucket_created" == "1" ]]; then
    docker exec \
      --env "MC_HOST_uea7=$minio_host" \
      "$minio_container" \
      mc rb --force "uea7/$shared_bucket" >/dev/null 2>&1 || true
    shared_bucket_created=0
  fi
}

cleanup() {
  docker rm -f "$container_name" >/dev/null 2>&1 || true
  cleanup_shared_bucket
  rm -rf -- "$work_dir"
}
trap cleanup EXIT INT TERM

fail() {
  echo "UEA-7 V07 fixture failed: $*" >&2
  if docker inspect "$container_name" >/dev/null 2>&1; then
    docker logs "$container_name" >&2 || true
  fi
  exit 1
}

for required in docker curl python3; do
  command -v "$required" >/dev/null 2>&1 || fail "missing required command: $required"
done

docker image inspect apache/iceberg-rest-fixture:1.10.1 >/dev/null 2>&1 \
  || fail "missing local image apache/iceberg-rest-fixture:1.10.1"
docker image inspect apache/spark:3.5.5-java17 >/dev/null 2>&1 \
  || fail "missing local image apache/spark:3.5.5-java17"

if [[ "${UEA7_SKIP_BUILD:-0}" != "1" ]]; then
  docker build --pull=false -t "$image_name" "$script_dir"
fi

docker_arguments=(
  --detach
  --pull=never
  --name "$container_name"
  --publish 127.0.0.1::8181
  --publish 127.0.0.1::8182
  --env CATALOG_CATALOG__IMPL=org.apache.iceberg.rest.fixture.HookedJdbcCatalog
  --env CATALOG_URI=jdbc:sqlite:/tmp/uea7-publication-catalog.db
  --env CATALOG_WAREHOUSE=file:///tmp/uea7-publication-warehouse
  --env CATALOG_IO__IMPL=org.apache.iceberg.rest.fixture.TracingFileIO
  --env CATALOG_JDBC_USER=user
  --env CATALOG_JDBC_PASSWORD=password
  --env CATALOG_JDBC_STRICT__MODE=true
  --env UEA7_FAULT_CONTROL_PORT=8182
  --env UEA7_FAULT_MAX_HOLD_SECONDS=60
)

if [[ "${UEA7_USE_SHARED_MINIO:-0}" == "1" ]]; then
  runtime_env="${NOVA_ENV_REST_ENV_FILE:-$repo_root/docker/iceberg-rest/runtime/current/env.sh}"
  runtime_env="$(python3 -c 'import pathlib, sys; print(pathlib.Path(sys.argv[1]).resolve(strict=True))' "$runtime_env")"
  source "$runtime_env"
  for required_variable in NOVA_ENV_COMPOSE_PROJECT NOVA_ENV_OBJECT_STORE_CONTAINER MINIO_ROOT_USER MINIO_ROOT_PASSWORD; do
    [[ -n "${!required_variable:-}" ]] \
      || fail "UEA7_USE_SHARED_MINIO=1 requires $required_variable"
  done
  storage_mode="shared-minio"
  shared_network="${NOVA_ENV_COMPOSE_PROJECT}_iceberg_net"
  minio_container="${NOVA_ENV_OBJECT_STORE_CONTAINER}"
  docker network inspect "$shared_network" >/dev/null 2>&1 \
    || fail "shared MinIO network does not exist: $shared_network"
  [[ "$(docker inspect "$minio_container" --format '{{.State.Running}}' 2>/dev/null)" == "true" ]] \
    || fail "shared MinIO container is not running: $minio_container"
  bucket_suffix="$(printf '%s' "$name_suffix" | tr '[:upper:]' '[:lower:]' | cut -c1-38)"
  shared_bucket="novarocks-uea7-$bucket_suffix"
  [[ "$shared_bucket" =~ ^novarocks-uea7-[a-z0-9-]+$ ]] \
    || fail "generated unsafe shared MinIO bucket: $shared_bucket"
  minio_host="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@localhost:9000"
  docker exec \
    --env "MC_HOST_uea7=$minio_host" \
    "$minio_container" \
    mc mb "uea7/$shared_bucket" >/dev/null \
    || fail "create isolated shared MinIO bucket $shared_bucket"
  shared_bucket_created=1
  docker_arguments+=(
    --network "$shared_network"
    --env UEA7_DELEGATE_FILE_IO=s3
    --env AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER"
    --env AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
    --env AWS_REGION=us-east-1
    --env CATALOG_S3_ENDPOINT=http://minio:9000
    --env CATALOG_S3_PATH__STYLE__ACCESS=true
    --env CATALOG_WAREHOUSE="s3://$shared_bucket/warehouse"
  )
fi

docker run "${docker_arguments[@]}" "$image_name" >/dev/null

rest_port="$(docker port "$container_name" 8181/tcp | awk -F: 'NR == 1 {print $NF}')"
control_port="$(docker port "$container_name" 8182/tcp | awk -F: 'NR == 1 {print $NF}')"
[[ "$rest_port" =~ ^[0-9]+$ ]] || fail "failed to resolve REST port"
[[ "$control_port" =~ ^[0-9]+$ ]] || fail "failed to resolve control port"
rest_uri="http://127.0.0.1:$rest_port"
control_uri="http://127.0.0.1:$control_port"

for attempt in $(seq 1 100); do
  if curl --silent --show-error --fail "$control_uri/health" >/dev/null 2>&1 \
      && curl --silent --show-error --fail "$rest_uri/v1/config" >/dev/null 2>&1; then
    break
  fi
  if ! docker inspect "$container_name" --format '{{.State.Running}}' | grep -qx true; then
    fail "fixture container exited during startup"
  fi
  sleep 0.1
done
curl --silent --show-error --fail "$control_uri/health" >/dev/null \
  || fail "control endpoint did not become ready"
curl --silent --show-error --fail "$rest_uri/v1/config" >/dev/null \
  || fail "REST endpoint did not become ready"

curl --silent --show-error --fail \
  --request POST \
  --header 'Content-Type: application/json' \
  --data '{"namespace":["uea7"],"properties":{}}' \
  "$rest_uri/v1/namespaces" >/dev/null

create_table() {
  local table="$1"
  local properties='{}'
  if [[ "${2:-}" == "v3" ]]; then
    properties='{"format-version":"3","write.row-lineage":"true"}'
  fi
  curl --silent --show-error --fail \
    --request POST \
    --header 'Content-Type: application/json' \
    --data "{\"name\":\"$table\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"identifier-field-ids\":[],\"fields\":[{\"id\":1,\"name\":\"id\",\"required\":true,\"type\":\"long\"}]},\"stage-create\":false,\"properties\":$properties}" \
    "$rest_uri/v1/namespaces/uea7/tables" >/dev/null
}

write_schema_request() {
  local path="$1"
  cat >"$path" <<'JSON'
{
  "requirements": [
    {"type": "assert-current-schema-id", "current-schema-id": 0}
  ],
  "updates": [
    {
      "action": "add-schema",
      "schema": {
        "type": "struct",
        "schema-id": 1,
        "identifier-field-ids": [],
        "fields": [
          {"id": 1, "name": "id", "required": true, "type": "long"},
          {"id": 2, "name": "payload", "required": false, "type": "string"}
        ]
      },
      "last-column-id": 2
    },
    {"action": "set-current-schema", "schema-id": 1}
  ]
}
JSON
}

write_snapshot_request() {
  local path="$1"
  local snapshot_id="$2"
  local expected_snapshot_id="${3:-null}"
  local sequence_number="${4:-1}"
  local timestamp_ms
  timestamp_ms="$(python3 -c 'import time; print(time.time_ns() // 1000000 + 1000)')"
  cat >"$path" <<JSON
{
  "requirements": [
    {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": $expected_snapshot_id}
  ],
  "updates": [
    {
      "action": "add-snapshot",
      "snapshot": {
        "snapshot-id": $snapshot_id,
        "timestamp-ms": $timestamp_ms,
        "sequence-number": $sequence_number,
        "first-row-id": 0,
        "added-rows": 0,
        "summary": {"operation": "append"},
        "manifest-list": "file:///tmp/uea7-publication-snapshot-$snapshot_id.avro",
        "schema-id": 0
      }
    },
    {"action": "set-snapshot-ref", "type": "branch", "ref-name": "main", "snapshot-id": $snapshot_id}
  ]
}
JSON
}

arm_table() {
  local table="$1"
  curl --silent --show-error --fail \
    --request POST "$control_uri/arm?table=uea7.$table" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["arm_id"])'
}

hold_phase() {
  local arm_id="$1"
  curl --silent --show-error --fail "$control_uri/status?arm_id=$arm_id" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["phase"])'
}

wait_for_phase() {
  local arm_id="$1"
  local expected="$2"
  local phase=""
  for attempt in $(seq 1 200); do
    phase="$(hold_phase "$arm_id")"
    if [[ "$phase" == "$expected" ]]; then
      return 0
    fi
    if [[ "$phase" == "conflict" || "$phase" == "succeeded" \
        || "$phase" == "failed" || "$phase" == "timed-out" ]]; then
      fail "arm $arm_id reached terminal phase $phase while waiting for $expected"
    fi
    sleep 0.05
  done
  fail "arm $arm_id stayed in phase $phase while waiting for $expected"
}

wait_for_terminal() {
  local arm_id="$1"
  local phase=""
  for attempt in $(seq 1 200); do
    phase="$(hold_phase "$arm_id")"
    case "$phase" in
      conflict|succeeded|failed|timed-out)
        printf '%s\n' "$phase"
        return 0
        ;;
    esac
    sleep 0.05
  done
  fail "arm $arm_id did not reach a terminal phase"
}

release_hold() {
  local arm_id="$1"
  curl --silent --show-error --fail --request POST \
    "$control_uri/release?arm_id=$arm_id" >/dev/null
}

commit_table() {
  local table="$1"
  local request_path="$2"
  local body_path="$3"
  curl --silent --show-error \
    --output "$body_path" \
    --write-out '%{http_code}' \
    --request POST \
    --header 'Content-Type: application/json' \
    --data-binary "@$request_path" \
    "$rest_uri/v1/namespaces/uea7/tables/$table"
}

assert_current_schema() {
  local table="$1"
  local expected="$2"
  local actual
  actual="$(curl --silent --show-error --fail \
    "$rest_uri/v1/namespaces/uea7/tables/$table" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["current-schema-id"])')"
  [[ "$actual" == "$expected" ]] \
    || fail "table $table has current schema $actual, expected $expected"
}

assert_current_snapshot() {
  local table="$1"
  local expected="$2"
  local actual
  actual="$(curl --silent --show-error --fail \
    "$rest_uri/v1/namespaces/uea7/tables/$table" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["current-snapshot-id"])')"
  [[ "$actual" == "$expected" ]] \
    || fail "table $table has current snapshot $actual, expected $expected"
}

assert_format_version() {
  local table="$1"
  local expected="$2"
  local actual
  actual="$(curl --silent --show-error --fail \
    "$rest_uri/v1/namespaces/uea7/tables/$table" \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["metadata"]["format-version"])')"
  [[ "$actual" == "$expected" ]] \
    || fail "table $table has format version $actual, expected $expected"
}

assert_main_absence_requirement_failed() {
  local response_path="$1"
  python3 - "$response_path" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    error = json.load(handle)["error"]
if error.get("code") != 409 or error.get("message") != (
    "Requirement failed: branch main was created concurrently"
):
    raise SystemExit(f"expected original absent-main requirement failure, got {error!r}")
PY
}

assert_main_snapshot_requirement_failed() {
  local response_path="$1"
  local expected_snapshot_id="$2"
  local actual_snapshot_id="$3"
  python3 - "$response_path" "$expected_snapshot_id" "$actual_snapshot_id" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    error = json.load(handle)["error"]
message = (
    "Requirement failed: branch main has changed: "
    f"expected id {sys.argv[2]} != {sys.argv[3]}"
)
if error.get("code") != 409 or error.get("message") != message:
    raise SystemExit(
        f"expected the original main snapshot {sys.argv[2]} requirement failure, "
        f"got {error!r}"
    )
PY
}

request_path="$work_dir/schema-request.json"
write_schema_request "$request_path"

# New request commits while the old request is held after requirement validation.
create_table new_first
new_first_arm="$(arm_table new_first)"
duplicate_arm_status="$(curl --silent --show-error \
  --output "$work_dir/duplicate-arm.body" \
  --write-out '%{http_code}' \
  --request POST "$control_uri/arm?table=uea7.new_first")"
[[ "$duplicate_arm_status" == "409" ]] \
  || fail "duplicate active arm returned HTTP $duplicate_arm_status instead of 409"
early_release_status="$(curl --silent --show-error \
  --output "$work_dir/early-release.body" \
  --write-out '%{http_code}' \
  --request POST "$control_uri/release?arm_id=$new_first_arm")"
[[ "$early_release_status" == "409" ]] \
  || fail "release before the commit boundary returned HTTP $early_release_status instead of 409"
commit_table new_first "$request_path" "$work_dir/new-first-old.body" \
  >"$work_dir/new-first-old.status" &
new_first_old_pid=$!
wait_for_phase "$new_first_arm" held
new_status="$(commit_table new_first "$request_path" "$work_dir/new-first-new.body")"
[[ "$new_status" == "200" ]] \
  || fail "competing new-first commit returned HTTP $new_status"
assert_current_schema new_first 1
release_hold "$new_first_arm"
wait "$new_first_old_pid" || true
old_status="$(cat "$work_dir/new-first-old.status")"
[[ "$old_status" == "409" ]] \
  || fail "released old new-first request returned HTTP $old_status instead of 409"
[[ "$(wait_for_terminal "$new_first_arm")" == "conflict" ]] \
  || fail "released old new-first request did not record a delegate conflict"

# The held request commits first; the frozen competing request keeps schema-id 0.
create_table old_first
old_first_arm="$(arm_table old_first)"
commit_table old_first "$request_path" "$work_dir/old-first-old.body" \
  >"$work_dir/old-first-old.status" &
old_first_pid=$!
wait_for_phase "$old_first_arm" held
release_hold "$old_first_arm"
wait "$old_first_pid"
old_first_status="$(cat "$work_dir/old-first-old.status")"
[[ "$old_first_status" == "200" ]] \
  || fail "released old-first request returned HTTP $old_first_status"
[[ "$(wait_for_terminal "$old_first_arm")" == "succeeded" ]] \
  || fail "old-first request did not record success"
frozen_status="$(commit_table old_first "$request_path" "$work_dir/old-first-new.body")"
[[ "$frozen_status" == "409" ]] \
  || fail "frozen old-first competitor returned HTTP $frozen_status instead of 409"
assert_current_schema old_first 1

# Closing the waiting HTTP client must not cancel a request accepted by the fixture.
create_table client_exit
client_exit_arm="$(arm_table client_exit)"
curl --silent --show-error \
  --output "$work_dir/client-exit.body" \
  --write-out '%{http_code}' \
  --request POST \
  --header 'Content-Type: application/json' \
  --data-binary "@$request_path" \
  "$rest_uri/v1/namespaces/uea7/tables/client_exit" \
  >"$work_dir/client-exit.status" &
client_pid=$!
wait_for_phase "$client_exit_arm" held
kill "$client_pid" || fail "waiting HTTP client exited before it could be terminated"
wait "$client_pid" >/dev/null 2>&1 || true
[[ "$(hold_phase "$client_exit_arm")" == "held" ]] \
  || fail "server-owned request left the hold when its HTTP client exited"
release_hold "$client_exit_arm"
[[ "$(wait_for_terminal "$client_exit_arm")" == "succeeded" ]] \
  || fail "server-owned request did not commit after its client exited"
assert_current_schema client_exit 1

if [[ "$v11_enabled" == "1" ]]; then
  # The two requests name distinct output snapshots but freeze the same absent
  # main ref. A catalog refresh may retry the original request, never replace
  # its null requirement with the current main snapshot.
  create_table snapshot_new_first v3
  assert_format_version snapshot_new_first 3
  write_snapshot_request "$work_dir/snapshot-old.json" 101
  write_snapshot_request "$work_dir/snapshot-new.json" 102
  snapshot_new_first_arm="$(arm_table snapshot_new_first)"
  commit_table snapshot_new_first "$work_dir/snapshot-old.json" \
    "$work_dir/snapshot-new-first-old.body" \
    >"$work_dir/snapshot-new-first-old.status" &
  snapshot_new_first_old_pid=$!
  wait_for_phase "$snapshot_new_first_arm" held
  snapshot_new_status="$(commit_table snapshot_new_first \
    "$work_dir/snapshot-new.json" "$work_dir/snapshot-new-first-new.body")"
  [[ "$snapshot_new_status" == "200" ]] \
    || fail "competing snapshot commit returned HTTP $snapshot_new_status"
  assert_current_snapshot snapshot_new_first 102
  release_hold "$snapshot_new_first_arm"
  wait "$snapshot_new_first_old_pid" || true
  snapshot_old_status="$(cat "$work_dir/snapshot-new-first-old.status")"
  [[ "$snapshot_old_status" == "409" ]] \
    || fail "released stale snapshot commit returned HTTP $snapshot_old_status instead of 409"
  assert_main_absence_requirement_failed "$work_dir/snapshot-new-first-old.body"
  [[ "$(wait_for_terminal "$snapshot_new_first_arm")" == "conflict" ]] \
    || fail "released stale snapshot commit did not record a delegate conflict"
  assert_current_snapshot snapshot_new_first 102

  create_table snapshot_old_first v3
  assert_format_version snapshot_old_first 3
  write_snapshot_request "$work_dir/snapshot-first.json" 201
  write_snapshot_request "$work_dir/snapshot-frozen.json" 202
  snapshot_old_first_arm="$(arm_table snapshot_old_first)"
  commit_table snapshot_old_first "$work_dir/snapshot-first.json" \
    "$work_dir/snapshot-old-first-old.body" \
    >"$work_dir/snapshot-old-first-old.status" &
  snapshot_old_first_pid=$!
  wait_for_phase "$snapshot_old_first_arm" held
  release_hold "$snapshot_old_first_arm"
  wait "$snapshot_old_first_pid"
  snapshot_first_status="$(cat "$work_dir/snapshot-old-first-old.status")"
  [[ "$snapshot_first_status" == "200" ]] \
    || fail "released first snapshot commit returned HTTP $snapshot_first_status"
  [[ "$(wait_for_terminal "$snapshot_old_first_arm")" == "succeeded" ]] \
    || fail "released first snapshot commit did not record success"
  snapshot_frozen_status="$(commit_table snapshot_old_first \
    "$work_dir/snapshot-frozen.json" "$work_dir/snapshot-old-first-new.body")"
  [[ "$snapshot_frozen_status" == "409" ]] \
    || fail "frozen competing snapshot commit returned HTTP $snapshot_frozen_status instead of 409"
  assert_main_absence_requirement_failed "$work_dir/snapshot-old-first-new.body"
  assert_current_snapshot snapshot_old_first 201

  # A normal refresh starts with an existing main snapshot. Its frozen
  # non-null predecessor must survive the same catalog retry path.
  create_table snapshot_existing_new_first v3
  write_snapshot_request "$work_dir/snapshot-existing-seed.json" 301
  seed_status="$(commit_table snapshot_existing_new_first \
    "$work_dir/snapshot-existing-seed.json" "$work_dir/snapshot-existing-seed.body")"
  [[ "$seed_status" == "200" ]] || fail "existing-main seed returned HTTP $seed_status"
  assert_current_snapshot snapshot_existing_new_first 301
  write_snapshot_request "$work_dir/snapshot-existing-old.json" 302 301 2
  write_snapshot_request "$work_dir/snapshot-existing-new.json" 303 301 2
  snapshot_existing_new_first_arm="$(arm_table snapshot_existing_new_first)"
  commit_table snapshot_existing_new_first "$work_dir/snapshot-existing-old.json" \
    "$work_dir/snapshot-existing-new-first-old.body" \
    >"$work_dir/snapshot-existing-new-first-old.status" &
  snapshot_existing_old_pid=$!
  wait_for_phase "$snapshot_existing_new_first_arm" held
  existing_new_status="$(commit_table snapshot_existing_new_first \
    "$work_dir/snapshot-existing-new.json" "$work_dir/snapshot-existing-new-first-new.body")"
  [[ "$existing_new_status" == "200" ]] \
    || fail "existing-main competitor returned HTTP $existing_new_status"
  assert_current_snapshot snapshot_existing_new_first 303
  release_hold "$snapshot_existing_new_first_arm"
  wait "$snapshot_existing_old_pid" || true
  existing_old_status="$(cat "$work_dir/snapshot-existing-new-first-old.status")"
  [[ "$existing_old_status" == "409" ]] \
    || fail "released stale existing-main request returned HTTP $existing_old_status"
  assert_main_snapshot_requirement_failed "$work_dir/snapshot-existing-new-first-old.body" 301 303
  [[ "$(wait_for_terminal "$snapshot_existing_new_first_arm")" == "conflict" ]] \
    || fail "stale existing-main request did not record a delegate conflict"
  assert_current_snapshot snapshot_existing_new_first 303

  create_table snapshot_existing_old_first v3
  write_snapshot_request "$work_dir/snapshot-existing-old-seed.json" 401
  seed_status="$(commit_table snapshot_existing_old_first \
    "$work_dir/snapshot-existing-old-seed.json" "$work_dir/snapshot-existing-old-seed.body")"
  [[ "$seed_status" == "200" ]] || fail "second existing-main seed returned HTTP $seed_status"
  assert_current_snapshot snapshot_existing_old_first 401
  write_snapshot_request "$work_dir/snapshot-existing-first.json" 402 401 2
  write_snapshot_request "$work_dir/snapshot-existing-frozen.json" 403 401 2
  snapshot_existing_old_first_arm="$(arm_table snapshot_existing_old_first)"
  commit_table snapshot_existing_old_first "$work_dir/snapshot-existing-first.json" \
    "$work_dir/snapshot-existing-old-first-old.body" \
    >"$work_dir/snapshot-existing-old-first-old.status" &
  snapshot_existing_first_pid=$!
  wait_for_phase "$snapshot_existing_old_first_arm" held
  release_hold "$snapshot_existing_old_first_arm"
  wait "$snapshot_existing_first_pid"
  existing_first_status="$(cat "$work_dir/snapshot-existing-old-first-old.status")"
  [[ "$existing_first_status" == "200" ]] \
    || fail "released existing-main request returned HTTP $existing_first_status"
  [[ "$(wait_for_terminal "$snapshot_existing_old_first_arm")" == "succeeded" ]] \
    || fail "existing-main first request did not record success"
  existing_frozen_status="$(commit_table snapshot_existing_old_first \
    "$work_dir/snapshot-existing-frozen.json" "$work_dir/snapshot-existing-old-first-new.body")"
  [[ "$existing_frozen_status" == "409" ]] \
    || fail "frozen existing-main competitor returned HTTP $existing_frozen_status"
  assert_main_snapshot_requirement_failed "$work_dir/snapshot-existing-old-first-new.body" 401 402
  assert_current_snapshot snapshot_existing_old_first 402
fi

curl --silent --show-error --fail "$control_uri/trace" >"$work_dir/trace.ndjson"
curl --silent --show-error --fail "$control_uri/metrics" >"$work_dir/metrics.json"
python3 - "$work_dir/trace.ndjson" "$work_dir/metrics.json" \
  "$new_first_arm" "$old_first_arm" "$client_exit_arm" \
  "$v11_enabled" "${snapshot_new_first_arm:-}" "${snapshot_old_first_arm:-}" \
  "${snapshot_existing_new_first_arm:-}" "${snapshot_existing_old_first_arm:-}" <<'PY'
import json
import sys

(
    path,
    metrics_path,
    new_first_arm,
    old_first_arm,
    client_exit_arm,
    v11_enabled,
    snapshot_new_first_arm,
    snapshot_old_first_arm,
    snapshot_existing_new_first_arm,
    snapshot_existing_old_first_arm,
) = sys.argv[1:]
with open(path, encoding="utf-8") as handle:
    events = [json.loads(line) for line in handle if line.strip()]
with open(metrics_path, encoding="utf-8") as handle:
    metrics = json.load(handle)

def arm_events(arm_id):
    return [event for event in events if event["arm_id"] == arm_id]

def require_order(arm_id, expected):
    actual = [event["event"] for event in arm_events(arm_id)]
    cursor = 0
    for name in expected:
        try:
            cursor = actual.index(name, cursor) + 1
        except ValueError as error:
            raise SystemExit(
                f"arm {arm_id} is missing ordered event {name}; actual={actual}"
            ) from error

require_order(
    new_first_arm,
    [
        "hold-armed",
        "requirements-passed-before-persistent-commit",
        "hold-reached",
        "hold-released",
        "delegate-commit-start",
        "delegate-commit-conflict",
    ],
)
require_order(
    old_first_arm,
    [
        "hold-armed",
        "requirements-passed-before-persistent-commit",
        "hold-reached",
        "hold-released",
        "delegate-commit-start",
        "delegate-commit-success",
    ],
)
require_order(
    client_exit_arm,
    [
        "hold-armed",
        "requirements-passed-before-persistent-commit",
        "hold-reached",
        "hold-released",
        "delegate-commit-start",
        "delegate-commit-success",
    ],
)

for arm_id in (new_first_arm, old_first_arm, client_exit_arm):
    starts = [event for event in arm_events(arm_id) if event["event"] == "delegate-commit-start"]
    if len(starts) != 1:
        raise SystemExit(f"arm {arm_id} has {len(starts)} delegated commits, expected one")
    reached = next(event for event in arm_events(arm_id) if event["event"] == "hold-reached")
    if (
        starts[0]["base_metadata"] != reached["base_metadata"]
        or starts[0]["updated_metadata"] != reached["updated_metadata"]
    ):
        raise SystemExit(f"arm {arm_id} substituted base or updated metadata after release")

held = next(event for event in arm_events(new_first_arm) if event["event"] == "hold-reached")
old_conflict = next(
    event for event in arm_events(new_first_arm) if event["event"] == "delegate-commit-conflict"
)
competitor_success = [
    event
    for event in events
    if event["table"] == "uea7.new_first"
    and event["arm_id"] == ""
    and event["event"] == "delegate-commit-success"
    and event["sequence"] > held["sequence"]
]
if not competitor_success:
    raise SystemExit("new-first trace has no unheld competing commit success")
if competitor_success[0]["sequence"] >= old_conflict["sequence"]:
    raise SystemExit("old request conflicted before the competing commit succeeded")

old_thread = held["thread"]
retry_refresh = [
    event
    for event in events
    if event["table"] == "uea7.new_first"
    and event["thread"] == old_thread
    and event["event"] == "refresh"
    and event["sequence"] > old_conflict["sequence"]
]
if not retry_refresh:
    raise SystemExit("old request did not refresh its base after the JDBC conflict")
second_old_delegate = [
    event
    for event in events
    if event["table"] == "uea7.new_first"
    and event["thread"] == old_thread
    and event["event"] == "delegate-commit-start"
    and event["sequence"] > old_conflict["sequence"]
]
if second_old_delegate:
    raise SystemExit(
        "old request delegated another commit after refresh instead of rejecting "
        "its original schema requirement"
    )

if v11_enabled == "1":
    require_order(
        snapshot_new_first_arm,
        [
            "hold-armed",
            "requirements-passed-before-persistent-commit",
            "hold-reached",
            "hold-released",
            "delegate-commit-start",
            "delegate-commit-conflict",
        ],
    )
    require_order(
        snapshot_old_first_arm,
        [
            "hold-armed",
            "requirements-passed-before-persistent-commit",
            "hold-reached",
            "hold-released",
            "delegate-commit-start",
            "delegate-commit-success",
        ],
    )
    for arm_id in (snapshot_new_first_arm, snapshot_old_first_arm):
        starts = [
            event for event in arm_events(arm_id)
            if event["event"] == "delegate-commit-start"
        ]
        if len(starts) != 1:
            raise SystemExit(f"snapshot arm {arm_id} delegated {len(starts)} commits")
        reached = next(event for event in arm_events(arm_id) if event["event"] == "hold-reached")
        if (
            starts[0]["base_metadata"] != reached["base_metadata"]
            or starts[0]["updated_metadata"] != reached["updated_metadata"]
        ):
            raise SystemExit(f"snapshot arm {arm_id} changed its frozen commit after release")

    held = next(
        event for event in arm_events(snapshot_new_first_arm)
        if event["event"] == "hold-reached"
    )
    conflict = next(
        event for event in arm_events(snapshot_new_first_arm)
        if event["event"] == "delegate-commit-conflict"
    )
    successes = [
        event for event in events
        if event["table"] == "uea7.snapshot_new_first"
        and event["arm_id"] == ""
        and event["event"] == "delegate-commit-success"
        and held["sequence"] < event["sequence"] < conflict["sequence"]
    ]
    if len(successes) != 1:
        raise SystemExit("snapshot competitor did not commit between hold and conflict")
    old_thread = held["thread"]
    refreshed = any(
        event["table"] == "uea7.snapshot_new_first"
        and event["thread"] == old_thread
        and event["event"] == "refresh"
        and event["sequence"] > conflict["sequence"]
        for event in events
    )
    if not refreshed:
        raise SystemExit("stale snapshot request did not refresh after the JDBC conflict")
    if any(
        event["table"] == "uea7.snapshot_new_first"
        and event["thread"] == old_thread
        and event["event"] == "delegate-commit-start"
        and event["sequence"] > conflict["sequence"]
        for event in events
    ):
        raise SystemExit("stale snapshot request delegated again after refreshing its base")

    for arm_id, terminal in (
        (snapshot_existing_new_first_arm, "delegate-commit-conflict"),
        (snapshot_existing_old_first_arm, "delegate-commit-success"),
    ):
        require_order(
            arm_id,
            [
                "hold-armed",
                "requirements-passed-before-persistent-commit",
                "hold-reached",
                "hold-released",
                "delegate-commit-start",
                terminal,
            ],
        )
        starts = [
            event for event in arm_events(arm_id)
            if event["event"] == "delegate-commit-start"
        ]
        reached = next(event for event in arm_events(arm_id) if event["event"] == "hold-reached")
        if len(starts) != 1 or any(
            starts[0][key] != reached[key]
            for key in ("base_metadata", "updated_metadata")
        ):
            raise SystemExit(f"existing-main arm {arm_id} changed its frozen commit")

    held = next(
        event for event in arm_events(snapshot_existing_new_first_arm)
        if event["event"] == "hold-reached"
    )
    conflict = next(
        event for event in arm_events(snapshot_existing_new_first_arm)
        if event["event"] == "delegate-commit-conflict"
    )
    table = "uea7.snapshot_existing_new_first"
    competing_successes = [
        event for event in events
        if event["table"] == table
        and event["arm_id"] == ""
        and event["event"] == "delegate-commit-success"
        and held["sequence"] < event["sequence"] < conflict["sequence"]
    ]
    if len(competing_successes) != 1:
        raise SystemExit("existing-main competitor did not commit inside the held window")
    old_thread = held["thread"]
    if not any(
        event["table"] == table
        and event["thread"] == old_thread
        and event["event"] == "refresh"
        and event["sequence"] > conflict["sequence"]
        for event in events
    ):
        raise SystemExit("stale existing-main request did not refresh after the JDBC conflict")
    if any(
        event["table"] == table
        and event["thread"] == old_thread
        and event["event"] == "delegate-commit-start"
        and event["sequence"] > conflict["sequence"]
        for event in events
    ):
        raise SystemExit("stale existing-main request delegated again after refreshing")

expected_mutations = {
    "commit_attempts": 19 if v11_enabled == "1" else 7,
    "commit_successes": 16 if v11_enabled == "1" else 6,
    "commit_conflicts": 3 if v11_enabled == "1" else 1,
    "commit_failures": 0,
}
for name, expected in expected_mutations.items():
    if metrics.get(name) != expected:
        raise SystemExit(
            f"unexpected mutation counter {name}={metrics.get(name)!r}, expected {expected}"
        )
for name in ("input_files", "input_streams", "input_bytes", "output_files", "output_streams", "output_bytes"):
    if not isinstance(metrics.get(name), int) or metrics[name] <= 0:
        raise SystemExit(f"object-I/O counter {name} is not positive: {metrics.get(name)!r}")

print(
    "V07 trace validated: requirements-passed hold, both commit orders, "
    "original-base retry rejection, client-exit survival, and exact I/O counters"
)
if v11_enabled == "1":
    print("V11 snapshot trace validated: absent and existing main, both orders, original requirement retry")
PY

container_id="$(docker inspect "$container_name" --format '{{.Id}}')"
[[ -n "$container_id" ]] || fail "fixture container identity is empty"

if [[ -n "$artifact_dir" ]]; then
  [[ "$artifact_dir" == /* ]] || fail "UEA7_ARTIFACT_DIR must be an absolute path"
  mkdir -p "$artifact_dir"
  if find "$artifact_dir" -mindepth 1 -print -quit | grep -q .; then
    fail "UEA7_ARTIFACT_DIR must be empty: $artifact_dir"
  fi
  cp -R "$work_dir/." "$artifact_dir/"
  docker logs "$container_name" >"$artifact_dir/container.log" 2>&1
  image_id="$(docker image inspect "$image_name" --format '{{.Id}}')"
  base_image_id="$(docker image inspect apache/iceberg-rest-fixture:1.10.1 --format '{{.Id}}')"
  git_head="$(git -C "$repo_root" rev-parse HEAD)"
  python3 - "$artifact_dir/manifest.json" \
    "$git_head" "$image_name" "$image_id" "$base_image_id" "$container_id" \
    "$script_dir" "$storage_mode" "$shared_bucket" "$v11_enabled" <<'PY'
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

(
    path,
    git_head,
    image_name,
    image_id,
    base_image_id,
    container_id,
    fixture_source,
    storage_mode,
    shared_bucket,
    v11_enabled,
) = sys.argv[1:]
source_root = Path(fixture_source)
source_digest = hashlib.sha256()
for source_path in sorted(path for path in source_root.rglob("*") if path.is_file()):
    relative = source_path.relative_to(source_root).as_posix().encode()
    content = source_path.read_bytes()
    source_digest.update(len(relative).to_bytes(8, "big"))
    source_digest.update(relative)
    source_digest.update(len(content).to_bytes(8, "big"))
    source_digest.update(content)
with open(path, "w", encoding="utf-8") as handle:
    json.dump(
        {
            "schema_version": 1,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "git_head": git_head,
            "fixture_image": image_name,
            "fixture_image_id": image_id,
            "fixture_source_sha256": source_digest.hexdigest(),
            "base_image": "apache/iceberg-rest-fixture:1.10.1",
            "base_image_id": base_image_id,
            "container_id": container_id,
            "storage_mode": storage_mode,
            "shared_minio_bucket": shared_bucket,
            "scenarios": ["new-first", "old-first", "client-exit"] + (
                ["snapshot-new-first", "snapshot-old-first",
                 "snapshot-existing-new-first", "snapshot-existing-old-first"]
                if v11_enabled == "1" else []
            ),
        },
        handle,
        indent=2,
        sort_keys=True,
    )
    handle.write("\n")
PY
fi

docker rm -f "$container_name" >/dev/null
if docker inspect "$container_name" >/dev/null 2>&1; then
  fail "fixture container still exists after cleanup"
fi
cleanup_shared_bucket
if [[ -n "$shared_bucket" ]]; then
  remaining_buckets="$(
    docker exec --env "MC_HOST_uea7=$minio_host" "$minio_container" \
      mc ls --json uea7
  )" || fail "list shared MinIO buckets after fixture cleanup"
  bucket_presence="$(
    printf '%s\n' "$remaining_buckets" \
      | python3 -c 'import json, sys; target = sys.argv[1]; print("present" if any(json.loads(line).get("key", "").rstrip("/") == target for line in sys.stdin if line.strip()) else "absent")' \
        "$shared_bucket"
  )" || fail "decode shared MinIO bucket listing after fixture cleanup"
  if [[ "$bucket_presence" == "present" ]]; then
    fail "shared MinIO bucket still exists after cleanup: $shared_bucket"
  fi
fi
trap - EXIT INT TERM
rm -rf -- "$work_dir"

if [[ "$v11_enabled" == "1" ]]; then
  echo "UEA-7 V07/V11 fixture passed (container $container_id removed)"
else
  echo "UEA-7 V07 fixture passed (container $container_id removed)"
fi
if [[ -n "$artifact_dir" ]]; then
  echo "UEA-7 fixture evidence: $artifact_dir"
fi
