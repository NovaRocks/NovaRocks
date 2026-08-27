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

if [[ "$#" -ne 0 ]]; then
  echo "usage: $0" >&2
  exit 2
fi

os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
  Linux/x86_64|Linux/amd64) ;;
  *)
    echo "the FoundationDB experimental fixture requires Linux x86_64; found: $os/$arch" >&2
    exit 1
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
FDB_ENV="$WORKSPACE_ROOT/docker/foundationdb/runtime/current/env.sh"
if [[ ! -f "$FDB_ENV" ]]; then
  echo "FoundationDB environment is not initialized; run docker/foundationdb/up.sh first" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$FDB_ENV"
test "$NOVA_FDB_CLIENT_PLATFORM" = "linux-x86_64"
test -n "$NOVAROCKS_FDB_VERSION"
test -n "$NOVAROCKS_FDB_API_VERSION"
test -n "$NOVA_FDB_CLIENT_ASSET_SHA256"
test -n "$NOVAROCKS_FDB_CLUSTER_FILE"
test -n "$NOVAROCKS_FDB_KEYSPACE_ID"
test -d "$FDB_CLIENT_LIB_PATH"
test -f "$NOVA_FDB_CLIENT_LIBRARY_FILE"
test -x "$NOVA_FDB_FDBCLI"

cd "$WORKSPACE_ROOT"

cargo fmt --all -- --check
cargo test -p novarocks-spi
cargo check -p novarocks-spi --no-default-features
"$SCRIPT_DIR/check-spi-dependency-boundary.py" \
  --manifest-path "$WORKSPACE_ROOT/Cargo.toml"
cargo check -p novarocks-state-store-foundationdb --all-targets
cargo check -p novarocks-state-store-foundationdb --features foundationdb-provider,state-store-test-hooks --all-targets
cargo test -p novarocks-state-store-foundationdb --features foundationdb-provider,state-store-test-hooks --test state_store_foundationdb_runtime foundationdb_runtime_lifecycle -- --nocapture --test-threads=1
cargo test -p novarocks-state-store-foundationdb --features foundationdb-provider,state-store-test-hooks --test state_store_foundationdb foundationdb_suite -- --nocapture --test-threads=1
cargo test -p novarocks-state-store-foundationdb --features foundationdb-provider,state-store-test-hooks --test state_store_foundationdb_cross_process -- --nocapture --test-threads=1

git diff --check
