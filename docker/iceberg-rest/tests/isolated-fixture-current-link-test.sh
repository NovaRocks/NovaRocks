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
#
# `docker/iceberg-rest/runtime/current` is the single documented environment
# entrypoint for a worktree: CLAUDE.md tells every agent and developer to
# `source docker/iceberg-rest/runtime/current/env.sh`.  An isolated system-test
# fixture creates a throwaway environment whose ports die with it, so it must
# leave that link exactly as it found it.  When it did not, every later command
# in the worktree silently targeted the dead fixture's ports and failed with
# errors that pointed nowhere near the cause.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"

tmpdir="$(mktemp -d)"
runtime_base="$REPO_ROOT/docker/iceberg-rest/runtime"
current_link="$runtime_base/current"
runtime_dir=""
saved_current_kind="missing"
saved_current_ref=""
saved_current_backup="$tmpdir/current.backup"

cleanup() {
  if [[ -n "$runtime_dir" ]]; then
    rm -rf "$runtime_dir"
  fi
  rm -rf "$current_link"
  case "$saved_current_kind" in
    symlink)
      ln -s "$saved_current_ref" "$current_link"
      ;;
    path)
      mv "$saved_current_backup" "$current_link"
      ;;
    missing)
      ;;
  esac
  rm -rf "$tmpdir"
}
trap cleanup EXIT

if [[ -L "$current_link" ]]; then
  saved_current_kind="symlink"
  saved_current_ref="$(readlink "$current_link")"
elif [[ -e "$current_link" ]]; then
  saved_current_kind="path"
  mv "$current_link" "$saved_current_backup"
fi

fail() {
  echo "$1" >&2
  exit 1
}

# A pre-existing worktree entry that the fixture must not disturb.
sentinel_entry="isolated-fixture-test-sentinel"
sentinel_dir="$runtime_base/$sentinel_entry"
mkdir -p "$sentinel_dir"
rm -rf "$current_link"
ln -s "$sentinel_entry" "$current_link"

workspace="$tmpdir/workspace/cca1-vended-rest-1-2-3"
mkdir -p "$workspace"
slug="$(basename "$workspace" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9' '-' | sed 's/^-*//;s/-*$//;s/--*/-/g')"
slug="$(printf '%s' "$slug" | cut -c1-24)"
hash="$(printf '%s' "$workspace" | shasum -a 1 | awk '{print substr($1, 1, 8)}')"
env_id="${slug}-${hash}"
runtime_dir="$runtime_base/$env_id"

compose_project="nr-cca1-vended-rest-1-2-3"
config_file="$workspace/isolated-compose.env"
cat >"$config_file" <<EOF
NOVA_ENV_SHARED_DOCKER=false
NOVA_ENV_COMPOSE_PROJECT='$compose_project'
MINIO_ROOT_USER='isolatedtestuser'
MINIO_ROOT_PASSWORD='isolatedtestsecret'
EOF

fakebin="$tmpdir/bin"
mkdir -p "$fakebin"
cat >"$fakebin/docker" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$DOCKER_CALLS"
exit 0
EOF
chmod +x "$fakebin/docker"
cat >"$fakebin/curl" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "$fakebin/curl"
export DOCKER_CALLS="$tmpdir/docker.calls"
touch "$DOCKER_CALLS"

isolated_env=(
  "NOVAROCKS_WORKSPACE_ROOT=$workspace"
  "NOVA_ENV_CONFIG_FILE=$config_file"
  "NOVA_ENV_SHARED_DOCKER=false"
  "NOVA_ENV_COMPOSE_PROJECT=$compose_project"
  "NOVA_ENV_UPDATE_CURRENT=false"
  "NOVA_ENV_ALLOW_VOLUME_DELETE=true"
  "NOVA_ENV_EXPECTED_COMPOSE_PROJECT=$compose_project"
  "NOVA_ENV_EXPECTED_MINIO_VOLUME=${compose_project}_minio-data"
)

# --- up.sh must not claim the link ----------------------------------------
if ! env "${isolated_env[@]}" PATH="$fakebin:$PATH" \
  "$REPO_ROOT/docker/iceberg-rest/up.sh" --prepare-only \
  >"$tmpdir/up.stdout" 2>"$tmpdir/up.stderr"; then
  cat "$tmpdir/up.stderr" >&2
  fail "isolated up.sh --prepare-only must succeed"
fi

[[ -d "$runtime_dir" ]] || fail "isolated up.sh must generate its own runtime entry"
[[ "$(readlink "$current_link")" == "$sentinel_entry" ]] ||
  fail "isolated up.sh must not repoint runtime/current (got: $(readlink "$current_link"))"

# The generated entry must describe itself, not the untouched shared link.
grep -q "NOVA_ENV_CURRENT_DIR=\"$runtime_dir\"" "$runtime_dir/env.sh" ||
  fail "isolated env.sh must point NOVA_ENV_CURRENT_DIR at its own entry"

# --- down.sh must not remove the link -------------------------------------
if ! env "${isolated_env[@]}" "NOVA_ENV_ID=$env_id" PATH="$fakebin:$PATH" \
  "$REPO_ROOT/docker/iceberg-rest/down.sh" --docker --purge \
  >"$tmpdir/down.stdout" 2>"$tmpdir/down.stderr"; then
  cat "$tmpdir/down.stderr" >&2
  fail "isolated down.sh --docker --purge must succeed"
fi

[[ -L "$current_link" ]] || fail "isolated down.sh must not remove runtime/current"
[[ "$(readlink "$current_link")" == "$sentinel_entry" ]] ||
  fail "isolated down.sh must leave runtime/current pointing at the worktree entry"
[[ ! -e "$runtime_dir" ]] || fail "isolated down.sh --purge must remove its own runtime entry"

# --- teardown must survive a workspace root that is already gone ----------
mkdir -p "$runtime_dir"
printf 'compose env\n' >"$runtime_dir/compose.env"
rm -rf "$tmpdir/workspace"
if ! env "${isolated_env[@]}" "NOVA_ENV_ID=$env_id" PATH="$fakebin:$PATH" \
  "$REPO_ROOT/docker/iceberg-rest/down.sh" --docker --purge \
  >"$tmpdir/down2.stdout" 2>"$tmpdir/down2.stderr"; then
  cat "$tmpdir/down2.stderr" >&2
  fail "teardown must still reclaim its entry when the workspace root is gone"
fi
[[ ! -e "$runtime_dir" ]] ||
  fail "teardown with a vanished workspace root must still remove the runtime entry"

# --- repeating teardown must stay clean -----------------------------------
if ! env "${isolated_env[@]}" "NOVA_ENV_ID=$env_id" PATH="$fakebin:$PATH" \
  "$REPO_ROOT/docker/iceberg-rest/down.sh" --docker --purge \
  >"$tmpdir/down3.stdout" 2>"$tmpdir/down3.stderr"; then
  cat "$tmpdir/down3.stderr" >&2
  fail "teardown must be idempotent so a previous crash cannot block a later run"
fi
[[ "$(readlink "$current_link")" == "$sentinel_entry" ]] ||
  fail "repeated teardown must still leave runtime/current alone"

rmdir "$sentinel_dir"
echo "isolated fixture current-link test passed"
