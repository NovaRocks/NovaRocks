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
CHECKER="$REPO_ROOT/tools/ci/check-connector-role-binding-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

metadata="$tmpdir/metadata.json"
source_root="$tmpdir/source"
mkdir -p "$source_root/novarocks/frontend-application/src/connector" \
  "$source_root/novarocks/native-adapter/src/connector" \
  "$source_root/novarocks/spi/src/connector/binding" \
  "$source_root/novarocks/spi/src/connector/provider" \
  "$source_root/novarocks/connector/starrocks/src" \
  "$source_root/novarocks-server/src"
touch "$source_root/novarocks/spi/src/connector/binding/role.rs"
touch "$source_root/novarocks/spi/src/connector/provider/role.rs"
cat >"$source_root/novarocks/connector/starrocks/src/role_binding.rs" <<'EOF'
impl ConnectorControlRoleBindingFactory for StarRocksControlRoleBindingFactory {}
impl ConnectorExecutionRoleBindingFactory for StarRocksExecutionRoleBindingFactory {}
EOF
cargo metadata --manifest-path "$REPO_ROOT/Cargo.toml" --format-version 1 >"$metadata"

"$CHECKER" --metadata-path "$metadata" --source-root "$source_root"

assert_rejected() {
  local metadata_path="$1"
  local expected="$2"
  if "$CHECKER" --metadata-path "$metadata_path" --source-root "$source_root" \
    >"$metadata_path.stdout" 2>"$metadata_path.stderr"; then
    echo "connector role binding mutation was accepted: $metadata_path" >&2
    exit 1
  fi
  grep -Fq "$expected" "$metadata_path.stderr"
}

retired_binding="$tmpdir/retired-binding.json"
jq '
  .packages += [{name: "novarocks-connector-binding", id: "fixture#binding", dependencies: []}]
' "$metadata" >"$retired_binding"
assert_rejected "$retired_binding" \
  "retired package must be absent: novarocks-connector-binding"

native_adapter_missing="$tmpdir/native-adapter-missing-spi.json"
jq '
  (.packages[] | select(.name == "novarocks-native-adapter") | .dependencies) |= map(
    select(.name != "novarocks-spi")
  )
' "$metadata" >"$native_adapter_missing"
assert_rejected "$native_adapter_missing" \
  "novarocks-native-adapter must directly declare a normal dependency on novarocks-spi"

touch "$source_root/novarocks-server/src/connector_role_binding.rs"
if "$CHECKER" --metadata-path "$metadata" --source-root "$source_root" \
  >"$tmpdir/server.stdout" 2>"$tmpdir/server.stderr"; then
  echo "Server StarRocks factory adapter mutation was accepted" >&2
  exit 1
fi
grep -Fq "Server must not define a parallel StarRocks role-binding factory adapter" \
  "$tmpdir/server.stderr"

echo "connector-role-binding-boundary-test: PASS"
