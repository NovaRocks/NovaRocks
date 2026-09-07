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
CHECKER="$REPO_ROOT/tools/ci/check-ncp8-statistics-boundary.py"
fixture_root="$(mktemp -d)"
trap 'rm -rf "$fixture_root"' EXIT

write_package() {
  local root="$1"
  local directory="$2"
  local name="$3"
  mkdir -p "$root/$directory/src"
  printf '[package]\nname = "%s"\nversion = "0.1.0"\nedition = "2024"\n' "$name" \
    >"$root/$directory/Cargo.toml"
  printf 'pub fn marker() {}\n' >"$root/$directory/src/lib.rs"
}

write_fixture() {
  local root="$1"
  mkdir -p "$root/novarocks/connector" "$root/novarocks-server/src" \
    "$root/idl/novarocks"
  write_package "$root" "novarocks/execution" "novarocks-execution"
  write_package "$root" "novarocks/functions" "novarocks-functions"
  write_package "$root" "novarocks/connector/iceberg-functions" "novarocks-connector-iceberg-functions"
  write_package "$root" "novarocks/connector/iceberg" "novarocks-connector-iceberg"
  write_package "$root" "novarocks/frontend" "novarocks-frontend"
  write_package "$root" "novarocks/backend" "novarocks-backend"
  write_package "$root" "novarocks/proto-models" "novarocks-proto-models"
  write_package "$root" "novarocks-server" "novarocks-server"
  write_package "$root" "datasketches" "datasketches"
  printf '\n[features]\ndefault = []\nhll = []\ntheta = []\n' >>"$root/datasketches/Cargo.toml"

  cat >"$root/Cargo.toml" <<'EOF'
[workspace]
resolver = "2"
members = [
  "novarocks/execution",
  "novarocks/functions",
  "novarocks/connector/iceberg-functions",
  "novarocks/connector/iceberg",
  "novarocks/frontend",
  "novarocks/backend",
  "novarocks/proto-models",
  "novarocks-server",
  "datasketches",
]

[workspace.dependencies]
datasketches = { path = "datasketches", version = "=0.1.0", default-features = false }
EOF
  cat >>"$root/novarocks/execution/Cargo.toml" <<'EOF'

[dependencies]
datasketches = { workspace = true, features = ["hll"] }

[dev-dependencies]
novarocks-connector-iceberg-functions = { path = "../connector/iceberg-functions" }
EOF
  cat >>"$root/novarocks/connector/iceberg-functions/Cargo.toml" <<'EOF'

[dependencies]
datasketches = { workspace = true, features = ["theta"] }
novarocks-functions = { path = "../../functions" }
EOF
  cat >>"$root/novarocks/connector/iceberg/Cargo.toml" <<'EOF'

[dependencies]
novarocks-connector-iceberg-functions = { path = "../iceberg-functions" }
EOF
  cat >>"$root/novarocks/frontend/Cargo.toml" <<'EOF'

[dependencies]
novarocks-execution = { path = "../execution" }
EOF
  cat >>"$root/novarocks/backend/Cargo.toml" <<'EOF'

[dependencies]
novarocks-execution = { path = "../execution" }
EOF
  cat >>"$root/novarocks-server/Cargo.toml" <<'EOF'

[dependencies]
novarocks-connector-iceberg = { path = "../novarocks/connector/iceberg" }
novarocks-connector-iceberg-functions = { path = "../novarocks/connector/iceberg-functions" }
EOF

  cat >"$root/idl/novarocks/plan.proto" <<'EOF'
syntax = "proto3";
message DataSink {
  reserved 8;
  reserved "statistics";
  bool result = 1;
}
EOF
}

assert_rejected() {
  local root="$1"
  local expected="$2"
  if "$CHECKER" --repo-root "$root" --manifest-path "$root/Cargo.toml" \
    >"$root/stdout" 2>"$root/stderr"; then
    echo "NCP-8 statistics boundary mutation was accepted: $root" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$root/stderr"; then
    echo "NCP-8 mutation produced the wrong diagnostic: $root" >&2
    cat "$root/stderr" >&2
    exit 1
  fi
}

replace_once() {
  local path="$1"
  local old="$2"
  local new="$3"
  python3 - "$path" "$old" "$new" <<'PY'
import sys
from pathlib import Path

path = Path(sys.argv[1])
old = sys.argv[2]
new = sys.argv[3]
source = path.read_text(encoding="utf-8")
if source.count(old) != 1:
    raise SystemExit(f"expected exactly one mutation target in {path}: {old!r}")
path.write_text(source.replace(old, new, 1), encoding="utf-8")
PY
}

valid="$fixture_root/valid"
write_fixture "$valid"
"$CHECKER" --repo-root "$valid" --manifest-path "$valid/Cargo.toml"

sink_tag="$fixture_root/sink-tag"
cp -R "$valid" "$sink_tag"
replace_once "$sink_tag/idl/novarocks/plan.proto" \
  'reserved 8;' 'bool statistics = 8;'
assert_rejected "$sink_tag" "DataSink must reserve field number 8"

sink_name="$fixture_root/sink-name"
cp -R "$valid" "$sink_name"
replace_once "$sink_name/idl/novarocks/plan.proto" \
  'reserved "statistics";' 'bool statistics = 9;'
assert_rejected "$sink_name" "DataSink must reserve field name statistics"

role="$fixture_root/role"
cp -R "$valid" "$role"
printf 'datasketches = { workspace = true, features = ["hll"] }\n' \
  >>"$role/novarocks/frontend/Cargo.toml"
assert_rejected "$role" "novarocks-frontend must not directly depend on datasketches"

provider_sketch="$fixture_root/provider-sketch"
cp -R "$valid" "$provider_sketch"
printf 'datasketches = { workspace = true, features = ["theta"] }\n' \
  >>"$provider_sketch/novarocks/connector/iceberg/Cargo.toml"
assert_rejected "$provider_sketch" \
  "production package novarocks-connector-iceberg must not directly depend on datasketches"

execution_feature="$fixture_root/execution-feature"
cp -R "$valid" "$execution_feature"
replace_once "$execution_feature/novarocks/execution/Cargo.toml" \
  'features = ["hll"]' 'features = ["hll", "theta"]'
assert_rejected "$execution_feature" "features must be exactly ['hll']"

theta_feature="$fixture_root/theta-feature"
cp -R "$valid" "$theta_feature"
replace_once "$theta_feature/novarocks/connector/iceberg-functions/Cargo.toml" \
  'features = ["theta"]' 'features = ["hll", "theta"]'
assert_rejected "$theta_feature" "features must be exactly ['theta']"

reverse="$fixture_root/reverse"
cp -R "$valid" "$reverse"
replace_once "$reverse/novarocks/connector/iceberg/Cargo.toml" \
  $'novarocks-connector-iceberg-functions = { path = "../iceberg-functions" }\n' ''
printf 'novarocks-connector-iceberg = { path = "../iceberg" }\n' \
  >>"$reverse/novarocks/connector/iceberg-functions/Cargo.toml"
assert_rejected "$reverse" "internal normal closure must be exactly"

execution_normal="$fixture_root/execution-normal"
cp -R "$valid" "$execution_normal"
replace_once "$execution_normal/novarocks/execution/Cargo.toml" \
  $'[dev-dependencies]\nnovarocks-connector-iceberg-functions = { path = "../connector/iceberg-functions" }' \
  $'[dependencies.novarocks-connector-iceberg-functions]\npath = "../connector/iceberg-functions"'
assert_rejected "$execution_normal" "dependency kind must be dev, got normal"

execution_provider="$fixture_root/execution-provider"
cp -R "$valid" "$execution_provider"
printf 'novarocks-connector-iceberg = { path = "../connector/iceberg" }\n' \
  >>"$execution_provider/novarocks/execution/Cargo.toml"
assert_rejected "$execution_provider" "must not directly depend on novarocks-connector-iceberg"

indirect_provider="$fixture_root/indirect-provider"
cp -R "$valid" "$indirect_provider"
write_package "$indirect_provider" "helper" "provider-test-helper"
replace_once "$indirect_provider/Cargo.toml" \
  '  "datasketches",' '  "datasketches", "helper",'
cat >>"$indirect_provider/helper/Cargo.toml" <<'EOF'

[dependencies]
novarocks-connector-iceberg = { path = "../novarocks/connector/iceberg" }
EOF
printf 'provider-test-helper = { path = "../../helper" }\n' \
  >>"$indirect_provider/novarocks/execution/Cargo.toml"
assert_rejected "$indirect_provider" \
  "normal/build/dev closure must not contain novarocks-connector-iceberg"

indirect_sketch="$fixture_root/indirect-sketch"
cp -R "$valid" "$indirect_sketch"
write_package "$indirect_sketch" "helper" "statistics-helper"
replace_once "$indirect_sketch/Cargo.toml" \
  '  "datasketches",' '  "datasketches", "helper",'
cat >>"$indirect_sketch/helper/Cargo.toml" <<'EOF'

[dependencies]
datasketches = { workspace = true, features = ["theta"] }
EOF
cat >>"$indirect_sketch/novarocks/execution/Cargo.toml" <<'EOF'

[dependencies.statistics-helper]
path = "../../helper"
EOF
assert_rejected "$indirect_sketch" "normal closure has indirect datasketches owners"

echo "ncp8-statistics-boundary-test: PASS"
