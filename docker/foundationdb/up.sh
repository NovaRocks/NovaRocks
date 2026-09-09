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
VERSION="7.3.69"
API_VERSION="730"
prepare_only=false

sha1_text() {
  if command -v sha1sum >/dev/null 2>&1; then
    sha1sum | awk '{print $1}'
  else
    shasum -a 1 | awk '{print $1}'
  fi
}

sha256_text() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

run_with_timeout() {
  local timeout_seconds="$1"
  shift
  # Give the command and all descendants a dedicated process group so a
  # timeout cannot leave a Compose CLI plugin or downloader running.
  set -m
  "$@" &
  local child="$!"
  set +m
  local elapsed=0
  while kill -0 "$child" >/dev/null 2>&1; do
    if (( elapsed >= timeout_seconds )); then
      kill -TERM -- "-$child" >/dev/null 2>&1 || true
      sleep 1
      kill -KILL -- "-$child" >/dev/null 2>&1 || true
      wait "$child" 2>/dev/null || true
      echo "command timed out after ${timeout_seconds}s: $*" >&2
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$child"
}

self_check() {
  (
    set -euo pipefail
    # shellcheck disable=SC1090
    source "$exports_file"
    test "$NOVAROCKS_FDB_VERSION" = "$VERSION"
    test "$NOVAROCKS_FDB_API_VERSION" = "$API_VERSION"
    test -n "$NOVA_FDB_ENV_ID"
    test -n "$NOVA_FDB_COMPOSE_PROJECT"
    test -n "$NOVAROCKS_FDB_KEYSPACE_ID"
    test -f "$NOVAROCKS_FDB_CLUSTER_FILE"
    test -d "$FDB_CLIENT_LIB_PATH"
    test "$FDB_CLIENT_LIB_PATH" = "$NOVA_FDB_CLIENT_LIBRARY_DIR"
    test -f "$NOVA_FDB_CLIENT_LIBRARY_FILE"
    test -x "$NOVA_FDB_FDBCLI"
    test "$NOVA_FDB_CLIENT_ASSET_SHA256" = "$expected_client_sha"
  )
}

port_in_use() {
  local candidate="$1"
  (echo >/dev/tcp/127.0.0.1/"$candidate") >/dev/null 2>&1
}

for arg in "$@"; do
  case "$arg" in
    --prepare-only)
      prepare_only=true
      ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: $0 [--prepare-only]" >&2
      exit 2
      ;;
  esac
done

workspace_hash="$(printf '%s' "$WORKSPACE_ROOT" | sha1_text)"
env_id="nr-fdb-${workspace_hash:0:12}"
compose_project="$env_id"
runtime_base="$SCRIPT_DIR/runtime"
runtime_dir="$runtime_base/$env_id"
current_link="$runtime_base/current"
compose_file="$SCRIPT_DIR/compose.yml"
compose_env="$runtime_dir/compose.env"
exports_file="$runtime_dir/env.sh"
cluster_file="$runtime_dir/fdb.cluster"
client_manifest="$runtime_dir/client.env"
if [[ -f "$exports_file" ]]; then
  port="$(bash -c 'source "$1"; printf "%s" "$NOVA_FDB_PORT"' _ "$exports_file")"
else
  port=$((40000 + 16#${workspace_hash:0:4} % 20000))
  initial_port="$port"
  while port_in_use "$port"; do
    port=$((port + 1))
    if (( port > 59999 )); then
      port=40000
    fi
    if (( port == initial_port )); then
      echo "no free FoundationDB fixture port in range 40000-59999" >&2
      exit 1
    fi
  done
fi

keyspace_hex="$(printf '%s' "${WORKSPACE_ROOT}:novarocks-fdb-keyspace-v1" | sha256_text)"
# Set RFC 9562 version/variant bits while retaining deterministic worktree identity.
keyspace_id="${keyspace_hex:0:8}-${keyspace_hex:8:4}-5${keyspace_hex:13:3}-8${keyspace_hex:17:3}-${keyspace_hex:20:12}"

mkdir -p "$runtime_dir/data" "$runtime_dir/logs"
"$SCRIPT_DIR/install-client.sh" "$runtime_dir"
# shellcheck disable=SC1090
source "$client_manifest"

case "$NOVA_FDB_CLIENT_PLATFORM" in
  darwin-arm64)
    expected_client_sha="6bfbd48ac21356de0baa0c1e84c6e33d15d95d0b9d022c35a7625e5d9293b71e"
    ;;
  linux-x86_64)
    expected_client_sha="ea59d1708519798c7bc4f514cd29af1ac8e41dccbec4371f22d86b713ea81cbf"
    ;;
  *)
    echo "unexpected FoundationDB client platform: $NOVA_FDB_CLIENT_PLATFORM" >&2
    exit 1
    ;;
esac

printf 'novarocks:novarocks@127.0.0.1:%s\n' "$port" > "$cluster_file"
chmod 600 "$cluster_file"

{
  printf 'NOVA_FDB_ENV_ID=%s\n' "$env_id"
  printf 'NOVA_FDB_PORT=%s\n' "$port"
  printf 'NOVA_FDB_CLUSTER_FILE=%s\n' "$cluster_file"
  printf 'NOVA_FDB_CONTAINER_CLUSTER_FILE=/var/fdb/fdb.cluster\n'
} > "$compose_env"

{
  printf 'export NOVAROCKS_FDB_VERSION=%q\n' "$VERSION"
  printf 'export NOVAROCKS_FDB_API_VERSION=%q\n' "$API_VERSION"
  printf 'export NOVA_FDB_ENV_ID=%q\n' "$env_id"
  printf 'export NOVA_FDB_COMPOSE_PROJECT=%q\n' "$compose_project"
  printf 'export NOVA_FDB_RUNTIME_DIR=%q\n' "$runtime_dir"
  printf 'export NOVA_FDB_COMPOSE_FILE=%q\n' "$compose_file"
  printf 'export NOVA_FDB_COMPOSE_ENV=%q\n' "$compose_env"
  printf 'export NOVA_FDB_PORT=%q\n' "$port"
  printf 'export NOVAROCKS_FDB_CLUSTER_FILE=%q\n' "$cluster_file"
  printf 'export FDB_CLUSTER_FILE=%q\n' "$cluster_file"
  printf 'export NOVAROCKS_FDB_KEYSPACE_ID=%q\n' "$keyspace_id"
  printf 'export NOVA_FDB_CLIENT_PLATFORM=%q\n' "$NOVA_FDB_CLIENT_PLATFORM"
  printf 'export NOVA_FDB_CLIENT_ASSET_PATH=%q\n' "$NOVA_FDB_CLIENT_ASSET_PATH"
  printf 'export NOVA_FDB_CLIENT_ASSET_SHA256=%q\n' "$NOVA_FDB_CLIENT_ASSET_SHA256"
  printf 'export NOVA_FDB_CLIENT_LIBRARY_DIR=%q\n' "$NOVA_FDB_CLIENT_LIBRARY_DIR"
  printf 'export NOVA_FDB_CLIENT_LIBRARY_FILE=%q\n' "$NOVA_FDB_CLIENT_LIBRARY_FILE"
  printf 'export FDB_CLIENT_LIB_PATH=%q\n' "$FDB_CLIENT_LIB_PATH"
  printf 'export NOVA_FDB_FDBCLI=%q\n' "$NOVA_FDB_FDBCLI"
  if [[ "$NOVA_FDB_CLIENT_PLATFORM" == "darwin-arm64" ]]; then
    printf 'export DYLD_LIBRARY_PATH=%q${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}\n' "$NOVA_FDB_CLIENT_LIBRARY_DIR"
  else
    printf 'export LD_LIBRARY_PATH=%q${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}\n' "$NOVA_FDB_CLIENT_LIBRARY_DIR"
  fi
} > "$exports_file"

rm -f "$current_link"
ln -s "$env_id" "$current_link"
self_check

if [[ "$prepare_only" == true ]]; then
  echo "FoundationDB fixture prepared without starting Docker"
  echo "  environment: $exports_file"
  echo "  cluster file: $cluster_file"
  echo "  keyspace UUID: $keyspace_id"
  exit 0
fi

command -v docker >/dev/null 2>&1 || {
  echo "docker is required to start the FoundationDB fixture" >&2
  exit 1
}
docker_client_version="$(run_with_timeout 10 docker version --format '{{.Client.Version}}')"
docker_client_major="${docker_client_version%%.*}"
if [[ ! "$docker_client_major" =~ ^[0-9]+$ || "$docker_client_major" -lt 29 ]]; then
  echo "Docker client 29 or newer is required; found: $docker_client_version" >&2
  exit 1
fi
run_with_timeout 10 docker compose version >/dev/null

docker_timeout_seconds="${NOVA_FDB_DOCKER_TIMEOUT_SECONDS:-60}"
ready_timeout_seconds="${NOVA_FDB_READY_TIMEOUT_SECONDS:-60}"
if [[ ! "$docker_timeout_seconds" =~ ^[1-9][0-9]*$ || ! "$ready_timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "FoundationDB fixture timeouts must be positive integers" >&2
  exit 2
fi

# Fixtures never pull during a run. `pull_policy: never` in compose.yml
# refuses the download; this turns a missing image into an actionable message.
fdb_image="$(run_with_timeout 15 docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  config --images | awk 'NF { print; exit }')"
if [[ -z "$fdb_image" ]]; then
  echo "cannot resolve the FoundationDB service image from $compose_file" >&2
  exit 1
fi
if ! run_with_timeout 15 docker image inspect "$fdb_image" >/dev/null 2>&1; then
  cat >&2 <<EOF
Missing local image (FoundationDB server): $fdb_image

This fixture never pulls during a run. Import it once, then re-run:
  docker pull $fdb_image
EOF
  exit 1
fi

run_with_timeout "$docker_timeout_seconds" docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  up -d foundationdb

# shellcheck disable=SC1090
source "$exports_file"
ready=false
ready_deadline=$((SECONDS + ready_timeout_seconds))
while (( SECONDS < ready_deadline )); do
  if run_with_timeout 7 "$NOVA_FDB_FDBCLI" -C "$NOVAROCKS_FDB_CLUSTER_FILE" --exec status --timeout 5 >/dev/null 2>&1; then
    ready=true
    break
  fi
  if run_with_timeout 7 "$NOVA_FDB_FDBCLI" -C "$NOVAROCKS_FDB_CLUSTER_FILE" \
    --exec 'configure new single ssd; status' --timeout 5 >/dev/null 2>&1; then
    ready=true
    break
  fi
  running_id="$(run_with_timeout 10 docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" ps --status running --quiet foundationdb)"
  if [[ -z "$running_id" ]]; then
    echo "FoundationDB container stopped before becoming ready" >&2
    run_with_timeout 10 docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" logs foundationdb >&2 || true
    exit 1
  fi
  sleep 1
done

if [[ "$ready" != true ]]; then
  echo "FoundationDB did not become reachable through the host cluster file" >&2
  exit 1
fi

server_version="$(run_with_timeout 10 docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" exec -T foundationdb fdbserver --version 2>&1)"
if [[ "$server_version" != *"$VERSION"* ]]; then
  echo "unexpected FoundationDB server version: $server_version" >&2
  exit 1
fi

"$SCRIPT_DIR/status.sh" --self-check
echo "FoundationDB fixture is ready"
