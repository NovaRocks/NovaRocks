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
requested_workspace_root="${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}"
# Teardown must survive a workspace root that no longer exists.  A throwaway
# fixture roots its workspace in a temporary directory, and once that directory
# is gone an aborting `cd` here would strand the generated runtime entry and the
# Docker project forever.  Fall back to the literal path so cleanup still runs.
if ! WORKSPACE_ROOT="$(cd "$requested_workspace_root" 2>/dev/null && pwd)"; then
  WORKSPACE_ROOT="$requested_workspace_root"
  echo "workspace root no longer exists; continuing teardown for: $WORKSPACE_ROOT" >&2
fi

slug="$(basename "$WORKSPACE_ROOT" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/^-*//;s/-*$//;s/--*/-/g')"
if [[ -z "$slug" ]]; then
  slug="novarocks"
fi
slug="$(printf '%s' "$slug" | cut -c1-24)"
hash="$(printf '%s' "$WORKSPACE_ROOT" | shasum -a 1 | awk '{print substr($1, 1, 8)}')"
env_id="${slug}-${hash}"
# The derived id is only a guess once the workspace root is gone, because it
# hashes that exact path.  A caller that already knows which generated entry it
# owns passes it explicitly so teardown stays exact and repeatable.
if [[ -n "${NOVA_ENV_ID:-}" ]]; then
  case "${NOVA_ENV_ID}" in
    ""|*/*|"."|"..")
      echo "NOVA_ENV_ID must be a single path segment; got: ${NOVA_ENV_ID}" >&2
      exit 2
      ;;
  esac
  env_id="${NOVA_ENV_ID}"
fi
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
canonical_compose_project="nr-iceberg-rest"
shared_benchmark_root="${NOVA_ENV_SHARED_BENCHMARK_ROOT:-s3://novarocks/shared/benchmarks}"
if [[ ! "$shared_benchmark_root" =~ ^s3://[^/[:space:]]+/[^[:space:]]+$ ]]; then
  echo "NOVA_ENV_SHARED_BENCHMARK_ROOT must be a non-empty s3://bucket/prefix URI" >&2
  exit 2
fi
shared_benchmark_mc_prefix="minio/${shared_benchmark_root#s3://}"
shared_benchmark_mc_prefix="${shared_benchmark_mc_prefix%/}/"
if [[ "$shared_docker" == "true" ]]; then
  compose_project="$configured_compose_project"
  stop_docker=false
else
  compose_project="${NOVA_ENV_COMPOSE_PROJECT:-nr-${env_id}}"
  stop_docker=true
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

down_args=()
remove_runtime=false
purge_requested=false
volume_delete_requested=false
for arg in "$@"; do
  case "$arg" in
    --docker)
      stop_docker=true
      ;;
    --runtime-only)
      stop_docker=false
      ;;
    -v|--volumes)
      down_args+=("--volumes")
      stop_docker=true
      volume_delete_requested=true
      ;;
    --purge)
      remove_runtime=true
      purge_requested=true
      ;;
    *)
      echo "unknown argument: $arg" >&2
      echo "usage: $0 [--docker] [--runtime-only] [--volumes|--purge]" >&2
      exit 2
      ;;
  esac
done

if [[ "$purge_requested" == true && "$stop_docker" == true ]]; then
  # `--docker --purge` has always meant a full Docker teardown. Treat its
  # implied volume deletion exactly like an explicit `--volumes` request.
  down_args+=("--volumes")
  volume_delete_requested=true
fi

if [[ "$volume_delete_requested" == true ]]; then
  expected_project="${NOVA_ENV_EXPECTED_COMPOSE_PROJECT:-}"
  expected_volume="${NOVA_ENV_EXPECTED_MINIO_VOLUME:-}"
  actual_volume="${compose_project}_minio-data"
  if [[ "$compose_project" == "$canonical_compose_project" ]]; then
    echo "refusing to delete canonical shared Docker volume for project: $compose_project" >&2
    exit 2
  fi
  if [[ "${NOVA_ENV_ALLOW_VOLUME_DELETE:-}" != "true" ]]; then
    echo "refusing volume deletion without NOVA_ENV_ALLOW_VOLUME_DELETE=true" >&2
    exit 2
  fi
  if [[ "$expected_project" != "$compose_project" || "$expected_volume" != "$actual_volume" ]]; then
    echo "refusing volume deletion without exact task-owned project and volume confirmation" >&2
    exit 2
  fi
  echo "Volume deletion authorized for exact project: $compose_project; volume: $actual_volume"
fi

purge_object_store_prefixes() {
  if [[ "$purge_requested" != true || "$stop_docker" == true ]]; then
    return 0
  fi

  case "$env_id" in
    ""|*/*|"."|"..")
      echo "refusing to purge object-store prefixes for invalid environment id: $env_id" >&2
      return 1
      ;;
  esac

  if [[ ! -f "$compose_env" ]]; then
    echo "object-store purge skipped; environment is not initialized: $runtime_dir" >&2
    return 0
  fi

  local target
  for target in "minio/novarocks/$env_id/" "minio/warehouse/$env_id/"; do
    case "$target" in
      "$shared_benchmark_mc_prefix"*)
        echo "refusing to purge shared benchmark root: $target" >&2
        return 1
        ;;
    esac
    echo "Purging object-store prefix: $target"
    docker compose \
      --env-file "$compose_env" \
      -p "$compose_project" \
      -f "$compose_file" \
      run --rm --no-deps -T \
      --entrypoint /bin/sh \
      mc -c '/usr/bin/mc alias set minio http://minio:9000 "${MINIO_ROOT_USER:-admin}" "${MINIO_ROOT_PASSWORD:-admin123}" >/dev/null && /usr/bin/mc rm --recursive --force --quiet "$1"' \
      _ "$target"
  done
}

purge_object_store_prefixes

if [[ "$stop_docker" == true ]]; then
  if [[ ! -f "$compose_env" ]]; then
    echo "environment is not initialized: $runtime_dir" >&2
  else
    if [[ "$volume_delete_requested" != true ]]; then
      echo "Stopping Docker project: $compose_project (preserving volume: ${compose_project}_minio-data)"
    fi
    docker compose \
      --env-file "$compose_env" \
      -p "$compose_project" \
      -f "$compose_file" \
      down "${down_args[@]}"
  fi
else
  echo "Shared Docker is left running: $compose_project"
fi

if [[ "$remove_runtime" == true ]]; then
  # A caller that never claimed `runtime/current` must not remove it either:
  # the link still belongs to the surrounding worktree environment.
  if [[ "${NOVA_ENV_UPDATE_CURRENT:-true}" == "true" ]] &&
    [[ -L "$current_link" || -e "$current_link" ]]; then
    current_target="$(cd "$current_link" 2>/dev/null && pwd || true)"
    current_ref="$(readlink "$current_link" 2>/dev/null || true)"
    if [[ "$current_target" == "$runtime_dir" || "$current_ref" == "$env_id" ]]; then
      rm -rf "$current_link"
    fi
  fi
  rm -rf "$runtime_dir"
fi
