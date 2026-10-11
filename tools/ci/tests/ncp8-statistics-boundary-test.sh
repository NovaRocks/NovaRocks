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
  write_package "$root" "novarocks/type-contract" "novarocks-type-contract"
  write_package "$root" "novarocks/result-contract" "novarocks-result-contract"
  write_package "$root" "novarocks/functions" "novarocks-functions"
  write_package "$root" "novarocks/function-contract" "novarocks-function-contract"
  write_package "$root" "novarocks/constant-contract" "novarocks-constant-contract"
  write_package "$root" "novarocks/connector/iceberg-functions" "novarocks-connector-iceberg-functions"
  write_package "$root" "novarocks/connector/iceberg" "novarocks-connector-iceberg"
  write_package "$root" "novarocks/frontend-application" "novarocks-frontend-application"
  write_package "$root" "novarocks/native-adapter" "novarocks-native-adapter"
  write_package "$root" "novarocks/proto-models" "novarocks-proto-models"
  write_package "$root" "novarocks-server" "novarocks-server"
  write_package "$root" "datasketches" "datasketches"
  printf '\n[features]\ndefault = []\nhll = []\ntheta = []\n' >>"$root/datasketches/Cargo.toml"

  cat >"$root/Cargo.toml" <<'EOF'
[workspace]
resolver = "2"
members = [
  "novarocks/execution",
  "novarocks/type-contract",
  "novarocks/result-contract",
  "novarocks/functions",
  "novarocks/function-contract",
  "novarocks/constant-contract",
  "novarocks/connector/iceberg-functions",
  "novarocks/connector/iceberg",
  "novarocks/frontend-application",
  "novarocks/native-adapter",
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
novarocks-functions = { path = "../functions" }

[dev-dependencies]
novarocks-connector-iceberg-functions = { path = "../connector/iceberg-functions" }
EOF
  cat >>"$root/novarocks/functions/Cargo.toml" <<'EOF'

[dependencies]
datasketches = { workspace = true, features = ["hll"] }
novarocks-type-contract = { path = "../type-contract" }
novarocks-function-contract = { path = "../function-contract" }
novarocks-constant-contract = { path = "../constant-contract" }
EOF
  cat >>"$root/novarocks/function-contract/Cargo.toml" <<'EOF'

[dependencies]
novarocks-type-contract = { path = "../type-contract" }
novarocks-constant-contract = { path = "../constant-contract" }
EOF
  cat >>"$root/novarocks/constant-contract/Cargo.toml" <<'EOF'

[dependencies]
novarocks-type-contract = { path = "../type-contract" }
EOF
  cat >>"$root/novarocks/type-contract/Cargo.toml" <<'EOF'

[dependencies]
novarocks-result-contract = { path = "../result-contract" }
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
  cat >>"$root/novarocks/frontend-application/Cargo.toml" <<'EOF'

[dependencies]
novarocks-execution = { path = "../execution" }
EOF
  cat >>"$root/novarocks/native-adapter/Cargo.toml" <<'EOF'

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
  local exit_code
  if "$CHECKER" --repo-root "$root" --manifest-path "$root/Cargo.toml" \
    >"$root/stdout" 2>"$root/stderr"; then
    echo "NCP-8 statistics boundary mutation was accepted: $root" >&2
    exit 1
  else
    exit_code=$?
  fi
  if [[ "$exit_code" != 1 ]]; then
    echo "NCP-8 mutation produced unexpected exit $exit_code: $root" >&2
    cat "$root/stderr" >&2
    exit 1
  fi
  if ! grep -Fq "$expected" "$root/stderr"; then
    echo "NCP-8 mutation produced the wrong diagnostic: $root" >&2
    cat "$root/stderr" >&2
    exit 1
  fi
  printf 'NCP-8 mutation rejected: %s (exit %s)\n' "${root##*/}" "$exit_code"
  cat "$root/stderr"
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
  >>"$role/novarocks/frontend-application/Cargo.toml"
assert_rejected "$role" "novarocks-frontend-application must not directly depend on datasketches"

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

extra_owner="$fixture_root/extra-owner"
cp -R "$valid" "$extra_owner"
write_package "$extra_owner" "novarocks/unexpected-owner" "novarocks-unexpected-owner"
printf 'novarocks-unexpected-owner = { path = "../unexpected-owner" }\n' \
  >>"$extra_owner/novarocks/type-contract/Cargo.toml"
assert_rejected "$extra_owner" "internal normal closure must be exactly"

# Use a non-NovaRocks helper to prove dependency freedom independently of the
# exact internal closure, including edges omitted by a normal resolved graph.
for dependency_kind in dependencies build-dependencies dev-dependencies; do
  result_dependency="$fixture_root/result-$dependency_kind"
  cp -R "$valid" "$result_dependency"
  write_package "$result_dependency" "helper" "result-helper"
  printf '\n[%s]\nresult-helper = { path = "../../helper" }\n' "$dependency_kind" \
    >>"$result_dependency/novarocks/result-contract/Cargo.toml"
  assert_rejected "$result_dependency" "novarocks-result-contract must not declare dependencies"
done

result_optional="$fixture_root/result-optional"
cp -R "$valid" "$result_optional"
write_package "$result_optional" "helper" "result-helper"
cat >>"$result_optional/novarocks/result-contract/Cargo.toml" <<'EOF'

[target.'cfg(target_os = "none")'.dependencies]
result-helper = { path = "../../helper", optional = true }
EOF
assert_rejected "$result_optional" "novarocks-result-contract must not declare dependencies"

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

functions_missing="$fixture_root/functions-missing"
cp -R "$valid" "$functions_missing"
replace_once "$functions_missing/novarocks/functions/Cargo.toml" \
  $'datasketches = { workspace = true, features = ["hll"] }\n' ''
assert_rejected "$functions_missing" \
  "novarocks-functions must declare exactly one direct datasketches dependency"

functions_wrong_feature="$fixture_root/functions-wrong-feature"
cp -R "$valid" "$functions_wrong_feature"
replace_once "$functions_wrong_feature/novarocks/functions/Cargo.toml" \
  'features = ["hll"]' 'features = ["theta"]'
assert_rejected "$functions_wrong_feature" \
  "novarocks-functions datasketches features must be exactly ['hll']"

functions_broad_feature="$fixture_root/functions-broad-feature"
cp -R "$valid" "$functions_broad_feature"
replace_once "$functions_broad_feature/novarocks/functions/Cargo.toml" \
  'features = ["hll"]' 'features = ["hll", "theta"]'
assert_rejected "$functions_broad_feature" \
  "novarocks-functions datasketches features must be exactly ['hll']"

functions_defaults="$fixture_root/functions-defaults"
cp -R "$valid" "$functions_defaults"
replace_once "$functions_defaults/novarocks/functions/Cargo.toml" \
  'workspace = true, features = ["hll"]' \
  'workspace = true, default-features = true, features = ["hll"]'
assert_rejected "$functions_defaults" \
  "novarocks-functions datasketches must disable default features"

functions_optional="$fixture_root/functions-optional"
cp -R "$valid" "$functions_optional"
replace_once "$functions_optional/novarocks/functions/Cargo.toml" \
  'workspace = true, features = ["hll"]' \
  'workspace = true, optional = true, features = ["hll"]'
assert_rejected "$functions_optional" \
  "novarocks-functions datasketches must not be optional"

functions_renamed="$fixture_root/functions-renamed"
cp -R "$valid" "$functions_renamed"
replace_once "$functions_renamed/novarocks/functions/Cargo.toml" \
  'datasketches = { workspace = true, features = ["hll"] }' \
  'sketches = { package = "datasketches", path = "../../datasketches", version = "=0.1.0", default-features = false, features = ["hll"] }'
assert_rejected "$functions_renamed" "novarocks-functions must not rename datasketches"

for dependency_kind in dev build; do
  functions_kind="$fixture_root/functions-$dependency_kind-kind"
  cp -R "$valid" "$functions_kind"
  replace_once "$functions_kind/novarocks/functions/Cargo.toml" \
    $'datasketches = { workspace = true, features = ["hll"] }\n' ''
  printf '\n[%s-dependencies]\ndatasketches = { workspace = true, features = ["hll"] }\n' \
    "$dependency_kind" >>"$functions_kind/novarocks/functions/Cargo.toml"
  assert_rejected "$functions_kind" \
    "novarocks-functions datasketches dependency kind must be normal, got $dependency_kind"
done

functions_duplicate="$fixture_root/functions-duplicate-target"
cp -R "$valid" "$functions_duplicate"
cat >>"$functions_duplicate/novarocks/functions/Cargo.toml" <<'EOF'

[target.'cfg(target_os = "none")'.dependencies]
datasketches = { workspace = true, features = ["hll"] }
EOF
assert_rejected "$functions_duplicate" \
  "novarocks-functions must declare exactly one direct datasketches dependency"

# Keep the valid direct HLL declaration so that this helper reaches the
# indirect-owner rule instead of an earlier missing-declaration rejection.
functions_helper="$fixture_root/functions-indirect-helper"
cp -R "$valid" "$functions_helper"
write_package "$functions_helper" "helper" "statistics-helper"
replace_once "$functions_helper/Cargo.toml" \
  '  "datasketches",' '  "datasketches", "helper",'
cat >>"$functions_helper/helper/Cargo.toml" <<'EOF'

[dependencies]
datasketches = { workspace = true, features = ["hll"] }
EOF
printf 'statistics-helper = { path = "../../helper" }\n' \
  >>"$functions_helper/novarocks/functions/Cargo.toml"
assert_rejected "$functions_helper" \
  "novarocks-execution normal closure has indirect datasketches owners: statistics-helper"

echo "ncp8-statistics-boundary-test: PASS"
