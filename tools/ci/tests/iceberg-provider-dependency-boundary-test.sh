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
CHECKER="$REPO_ROOT/tools/ci/check-iceberg-provider-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

write_package() {
  local root="$1"
  local name="$2"
  mkdir -p "$root/$name/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n' "$name" \
    >"$root/$name/Cargo.toml"
  : >"$root/$name/src/lib.rs"
}

write_fixture() {
  local root="$1"
  mkdir -p "$root"
  for package in novarocks-connector-iceberg novarocks-server novarocks-frontend novarocks-state-store-sqlite novarocks-fs novarocks-spi; do
    write_package "$root" "$package"
  done
  cat >"$root/Cargo.toml" <<'EOF'
[workspace]
members = [
  "novarocks-connector-iceberg",
  "novarocks-server",
  "novarocks-frontend",
  "novarocks-state-store-sqlite",
  "novarocks-fs",
  "novarocks-spi",
]
resolver = "3"
EOF
  cat >>"$root/novarocks-connector-iceberg/Cargo.toml" <<'EOF'

[dependencies]
novarocks-fs = { path = "../novarocks-fs" }
novarocks-spi = { path = "../novarocks-spi" }
EOF
  cat >>"$root/novarocks-server/Cargo.toml" <<'EOF'

[dependencies]
novarocks-connector-iceberg = { path = "../novarocks-connector-iceberg" }
novarocks-state-store-sqlite = { path = "../novarocks-state-store-sqlite" }
EOF
}

assert_rejected() {
  local root="$1"
  local expected="$2"
  if "$CHECKER" --manifest-path "$root/Cargo.toml" >"$root/stdout" 2>"$root/stderr"; then
    echo "Iceberg dependency boundary mutation was accepted: $root" >&2
    exit 1
  fi
  grep -Fq "$expected" "$root/stderr"
}

valid="$tmpdir/valid"
write_fixture "$valid"
"$CHECKER" --manifest-path "$valid/Cargo.toml"

forbidden="$tmpdir/forbidden"
cp -R "$valid" "$forbidden"
write_package "$forbidden" novarocks
cat >>"$forbidden/novarocks-connector-iceberg/Cargo.toml" <<'EOF'
novarocks = { path = "../novarocks" }
EOF
assert_rejected "$forbidden" "provider closure contains forbidden packages: novarocks"

role="$tmpdir/role"
cp -R "$valid" "$role"
cat >>"$role/novarocks-frontend/Cargo.toml" <<'EOF'

[dependencies]
novarocks-connector-iceberg = { path = "../novarocks-connector-iceberg" }
EOF
assert_rejected "$role" "novarocks-frontend must not directly depend on novarocks-connector-iceberg"

echo "iceberg-provider-dependency-boundary-test: PASS"
