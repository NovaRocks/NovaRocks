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
umask 077

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "${NOVAROCKS_WORKSPACE_ROOT:-$SCRIPT_DIR/../..}" && pwd)"
VERSION="8.4.10"
IMAGE_NAME="mysql:8.4.10"
prepare_only=false

sha256_text() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

random_hex() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -hex "$1"
  else
    od -An -N "$1" -tx1 /dev/urandom | tr -d ' \n'
  fi
}

run_with_timeout() {
  local timeout_seconds="$1"
  shift
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
      echo "command timed out after ${timeout_seconds}s: $1" >&2
      return 124
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done
  wait "$child"
}

port_in_use() {
  local candidate="$1"
  (echo >/dev/tcp/127.0.0.1/"$candidate") >/dev/null 2>&1
}

write_runtime_files() {
  local database="$1"
  local env_tmp="$runtime_dir/env.sh.tmp"
  local compose_tmp="$runtime_dir/compose.env.tmp"
  local provisioner_tmp="$runtime_dir/provisioner.cnf.tmp"
  local provider_tmp="$runtime_dir/provider.cnf.tmp"

  {
    printf 'NOVA_MYSQL_PORT=%s\n' "$port"
    printf 'NOVA_MYSQL_RUNTIME_DIR=%s\n' "$runtime_dir"
    printf 'NOVA_MYSQL_PROVISIONER_PASSWORD=%s\n' "$provisioner_password"
  } > "$compose_tmp"
  chmod 600 "$compose_tmp"
  mv "$compose_tmp" "$compose_env"

  {
    printf '[client]\n'
    printf 'user=%s\n' "$provisioner_username"
    printf 'password=%s\n' "$provisioner_password"
  } > "$provisioner_tmp"
  chmod 600 "$provisioner_tmp"
  mv "$provisioner_tmp" "$provisioner_config"

  {
    printf '[client]\n'
    printf 'user=%s\n' "$provider_username"
    printf 'password=%s\n' "$provider_password"
    printf 'host=127.0.0.1\n'
    printf 'protocol=tcp\n'
  } > "$provider_tmp"
  chmod 600 "$provider_tmp"
  mv "$provider_tmp" "$provider_config"

  {
    printf 'export NOVA_MYSQL_ENV_ID=%q\n' "$env_id"
    printf 'export NOVA_MYSQL_COMPOSE_PROJECT=%q\n' "$compose_project"
    printf 'export NOVA_MYSQL_COMPOSE_FILE=%q\n' "$compose_file"
    printf 'export NOVA_MYSQL_COMPOSE_ENV=%q\n' "$compose_env"
    printf 'export NOVA_MYSQL_RUNTIME_DIR=%q\n' "$runtime_dir"
    printf 'export NOVAROCKS_MYSQL_HOST=%q\n' "127.0.0.1"
    printf 'export NOVAROCKS_MYSQL_PORT=%q\n' "$port"
    printf 'export NOVAROCKS_MYSQL_DATABASE=%q\n' "$database"
    printf 'export NOVAROCKS_MYSQL_USERNAME=%q\n' "$provider_username"
    printf 'export NOVAROCKS_MYSQL_PASSWORD=%q\n' "$provider_password"
    printf 'export NOVAROCKS_MYSQL_PASSWORD_ENV=%q\n' "NOVAROCKS_MYSQL_PASSWORD"
    printf 'export NOVA_MYSQL_PROVISIONER_USERNAME=%q\n' "$provisioner_username"
    printf 'export NOVA_MYSQL_PROVISIONER_PASSWORD=%q\n' "$provisioner_password"
    printf 'export NOVAROCKS_MYSQL_VERSION=%q\n' "$VERSION"
    printf 'export NOVAROCKS_MYSQL_IMAGE=%q\n' "$IMAGE_NAME"
  } > "$env_tmp"
  chmod 600 "$env_tmp"
  mv "$env_tmp" "$exports_file"
}

for arg in "$@"; do
  case "$arg" in
    --prepare-only)
      prepare_only=true
      ;;
    *)
      echo "usage: $0 [--prepare-only]" >&2
      exit 2
      ;;
  esac
done

workspace_hash="$(printf '%s' "$WORKSPACE_ROOT" | sha256_text)"
env_id="nr-mysql-${workspace_hash:0:12}"
compose_project="nrss3${workspace_hash:0:12}"
runtime_base="$SCRIPT_DIR/runtime"
runtime_dir="$runtime_base/$env_id"
current_link="$runtime_base/current"
compose_file="$SCRIPT_DIR/compose.yml"
compose_env="$runtime_dir/compose.env"
exports_file="$runtime_dir/env.sh"
provisioner_config="$runtime_dir/provisioner.cnf"
provider_config="$runtime_dir/provider.cnf"

mkdir -p "$runtime_dir/data"
if [[ -f "$exports_file" ]]; then
  # shellcheck disable=SC1090
  source "$exports_file"
  port="$NOVAROCKS_MYSQL_PORT"
  provider_username="$NOVAROCKS_MYSQL_USERNAME"
  provider_password="$NOVAROCKS_MYSQL_PASSWORD"
  provisioner_username="$NOVA_MYSQL_PROVISIONER_USERNAME"
  provisioner_password="$NOVA_MYSQL_PROVISIONER_PASSWORD"
  database="$NOVAROCKS_MYSQL_DATABASE"
else
  port=$((43000 + 16#${workspace_hash:0:4} % 10000))
  initial_port="$port"
  while port_in_use "$port"; do
    port=$((port + 1))
    if (( port > 52999 )); then
      port=43000
    fi
    if (( port == initial_port )); then
      echo "no free MySQL fixture port in range 43000-52999" >&2
      exit 1
    fi
  done
  provider_username="nrss3_${workspace_hash:0:12}"
  provider_password="$(random_hex 24)"
  provisioner_username="root"
  provisioner_password="$(random_hex 24)"
  database=""
fi
previous_database="$database"

write_runtime_files "$database"
rm -f "$current_link"
ln -s "$env_id" "$current_link"

if [[ "$prepare_only" == true ]]; then
  echo "MySQL state-store fixture prepared without starting Docker"
  echo "  environment: $exports_file"
  exit 0
fi

command -v docker >/dev/null 2>&1 || {
  echo "docker is required to start the MySQL state-store fixture" >&2
  exit 1
}
run_with_timeout 15 docker compose version >/dev/null
docker_timeout="${NOVA_MYSQL_DOCKER_TIMEOUT_SECONDS:-180}"
ready_timeout="${NOVA_MYSQL_READY_TIMEOUT_SECONDS:-120}"
if [[ ! "$docker_timeout" =~ ^[1-9][0-9]*$ || ! "$ready_timeout" =~ ^[1-9][0-9]*$ ]]; then
  echo "MySQL fixture timeouts must be positive integers" >&2
  exit 2
fi

pinned_image="$(run_with_timeout 15 docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  config --images | awk 'NF { print; exit }')"
if run_with_timeout 15 docker image inspect "$pinned_image" >/dev/null 2>&1; then
  echo "Using locally verified pinned MySQL image"
else
  # Fixtures never pull during a run: a missing image is an error, not a
  # download, so the fixture cannot silently start against a freshly fetched
  # image on one host and a long-cached one on another.
  cat >&2 <<EOF
Missing local image (MySQL state store): $pinned_image

This fixture never pulls during a run. Import it once, then re-run:
  docker pull $pinned_image
EOF
  exit 1
fi
run_with_timeout "$docker_timeout" docker compose \
  --env-file "$compose_env" \
  -p "$compose_project" \
  -f "$compose_file" \
  up -d mysql

ready_database=""
deadline=$((SECONDS + ready_timeout))
while (( SECONDS < deadline )); do
  if ready_database="$("$SCRIPT_DIR/provision-test-database.sh" create runtime-readiness 2>/dev/null)"; then
    break
  fi
  running_id="$(run_with_timeout 15 docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" ps --status running --quiet mysql || true)"
  if [[ -z "$running_id" ]]; then
    echo "MySQL container stopped before SQL readiness" >&2
    run_with_timeout 15 docker compose --env-file "$compose_env" -p "$compose_project" -f "$compose_file" logs mysql >&2 || true
    exit 1
  fi
  sleep 1
done
if [[ -z "$ready_database" ]]; then
  echo "MySQL did not become ready for provisioned SQL within ${ready_timeout}s" >&2
  exit 1
fi
if [[ -n "$previous_database" && "$previous_database" != "$ready_database" ]]; then
  "$SCRIPT_DIR/provision-test-database.sh" drop "$previous_database"
fi

write_runtime_files "$ready_database"
# shellcheck disable=SC1090
source "$exports_file"
readiness="$(run_with_timeout 15 docker compose \
  --env-file "$NOVA_MYSQL_COMPOSE_ENV" \
  -p "$NOVA_MYSQL_COMPOSE_PROJECT" \
  -f "$NOVA_MYSQL_COMPOSE_FILE" \
  exec -T mysql mysql \
  --defaults-extra-file=/run/secrets/novarocks-mysql-provider.cnf \
  --database="$NOVAROCKS_MYSQL_DATABASE" \
  --batch --skip-column-names \
  --execute="SELECT VERSION(), @@innodb_page_size, @@default_storage_engine, @@session.time_zone, @@session.sql_mode;")"
IFS=$'\t' read -r actual_version page_size engine time_zone sql_mode <<<"$readiness"
test "$actual_version" = "$VERSION"
test "$page_size" = "16384"
test "${engine,,}" = "innodb"
test "$time_zone" = "+00:00"
case ",$sql_mode," in
  *,STRICT_TRANS_TABLES,*) ;;
  *)
    echo "MySQL fixture does not expose strict SQL mode" >&2
    exit 1
    ;;
esac

run_with_timeout 15 docker compose \
  --env-file "$NOVA_MYSQL_COMPOSE_ENV" \
  -p "$NOVA_MYSQL_COMPOSE_PROJECT" \
  -f "$NOVA_MYSQL_COMPOSE_FILE" \
  exec -T mysql mysql \
  --defaults-extra-file=/run/secrets/novarocks-mysql-provider.cnf \
  --database="$NOVAROCKS_MYSQL_DATABASE" \
  --batch --skip-column-names \
  --execute="CREATE TABLE fixture_readiness (id INT PRIMARY KEY) ENGINE=InnoDB ROW_FORMAT=DYNAMIC; SELECT ENGINE, ROW_FORMAT FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'fixture_readiness'; DROP TABLE fixture_readiness;" \
  | grep -Fx $'InnoDB\tDynamic' >/dev/null

"$SCRIPT_DIR/status.sh" --self-check
echo "MySQL state-store fixture is ready"
echo "  environment: $exports_file"
