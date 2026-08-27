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
CHECKER="$REPO_ROOT/tools/ci/check-native-wire-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

base_metadata="$tmpdir/base-metadata.json"
cargo metadata --manifest-path "$REPO_ROOT/Cargo.toml" --format-version 1 >"$base_metadata"
"$CHECKER" --metadata-path "$base_metadata"

package_id() {
  local package_name="$1"
  jq -er --arg package_name "$package_name" \
    '.packages[] | select(.name == $package_name) | .id' "$base_metadata"
}

assert_rejected() {
  local metadata_path="$1"
  local expected_error="$2"

  if "$CHECKER" --metadata-path "$metadata_path" \
      >"$metadata_path.stdout" 2>"$metadata_path.stderr"; then
    echo "Native wire dependency boundary mutation was accepted: $metadata_path" >&2
    exit 1
  fi
  grep -Fq "$expected_error" "$metadata_path.stderr"
}

add_normal_resolve_edge() {
  local source_package="$1"
  local target_package="$2"
  local output_path="$3"
  local source_id
  local target_id

  source_id="$(package_id "$source_package")"
  target_id="$(package_id "$target_package")"
  jq --arg source_id "$source_id" --arg target_id "$target_id" '
    .resolve.nodes |= map(
      if .id == $source_id then
        .deps += [{
          name: "native_wire_boundary_mutation",
          pkg: $target_id,
          dep_kinds: [{kind: null, target: null}]
        }]
      elif .id != $source_id then
        # Keep this mutation local to the requested source owner.  Otherwise
        # an existing lower-layer consumer of that owner could fail first and
        # conceal which root closure the fixture is exercising.
        .deps |= map(select(.pkg != $source_id))
      else
        .
      end
    )
  ' "$base_metadata" >"$output_path"
}

models_internal="$tmpdir/models-internal.json"
jq '
  (.packages[] | select(.name == "novarocks-proto-models") | .dependencies) += [{
    name: "novarocks-spi", kind: null, optional: false
  }]
' "$base_metadata" >"$models_internal"
assert_rejected "$models_internal" \
  "novarocks-proto-models has forbidden internal normal dependencies: novarocks-spi"

proto_missing_spi="$tmpdir/proto-missing-spi.json"
jq '
  (.packages[] | select(.name == "novarocks-proto-codec") | .dependencies) |= map(
    select(.name != "novarocks-spi")
  )
' "$base_metadata" >"$proto_missing_spi"
assert_rejected "$proto_missing_spi" \
  "novarocks-proto-codec internal normal dependencies must be exactly"

for role in novarocks-frontend novarocks-backend; do
  role_missing_models="$tmpdir/${role}-missing-models.json"
  jq --arg role "$role" '
    (.packages[] | select(.name == $role) | .dependencies) |= map(
      select(.name != "novarocks-proto-models")
    )
  ' "$base_metadata" >"$role_missing_models"
  assert_rejected "$role_missing_models" \
    "$role must directly declare normal dependencies on: novarocks-proto-models"
done

# Models may never reach Tonic.  Proto is checked against every forbidden
# application/role/provider/state-store/Tonic package so each protected edge
# has an independent mutation witness.
models_tonic="$tmpdir/models-tonic.json"
add_normal_resolve_edge novarocks-proto-models tonic "$models_tonic"
assert_rejected "$models_tonic" \
  "novarocks-proto-models normal dependency closure contains forbidden packages: tonic"

for forbidden in \
  tonic \
  novarocks-backend \
  novarocks-connector-starrocks \
  novarocks-execution \
  novarocks-frontend \
  novarocks-server \
  novarocks-sql \
  novarocks-state-store-foundationdb \
  novarocks-state-store-mysql \
  novarocks-state-store-sqlite; do
  proto_forbidden="$tmpdir/proto-${forbidden}.json"
  add_normal_resolve_edge novarocks-proto-codec "$forbidden" "$proto_forbidden"
  assert_rejected "$proto_forbidden" \
    "novarocks-proto-codec normal dependency closure contains forbidden packages:"
  grep -Fq "$forbidden" "$proto_forbidden.stderr"
done

# Lower-layer owners must never acquire either wire crate, including through a
# transitive normal edge.  ADR-0114 deliberately excludes Iceberg and
# Execution: Iceberg owns typed provider conversion and Execution owns the
# typed per-attempt split queue.  The Server is also excluded because it
# composes FE/BE and therefore has a wire-containing transitive closure by
# design.
for lower_layer in \
  novarocks-spi \
  novarocks-types \
  novarocks-sql \
  novarocks-state-store-foundationdb \
  novarocks-state-store-mysql \
  novarocks-state-store-sqlite \
  novarocks-connector-starrocks; do
  lower_layer_wire="$tmpdir/${lower_layer}-wire.json"
  add_normal_resolve_edge "$lower_layer" novarocks-proto-models "$lower_layer_wire"
  assert_rejected "$lower_layer_wire" \
    "$lower_layer normal dependency closure contains forbidden wire packages:"
  grep -Fq "novarocks-proto-models" "$lower_layer_wire.stderr"
done

server_direct_wire="$tmpdir/server-direct-wire.json"
jq '
  (.packages[] | select(.name == "novarocks-server") | .dependencies) += [{
    name: "novarocks-proto-codec", kind: null, optional: false
  }]
' "$base_metadata" >"$server_direct_wire"
assert_rejected "$server_direct_wire" \
  "novarocks-server must not directly declare normal wire dependencies: novarocks-proto-codec"

for forbidden in novarocks-proto-codec novarocks-proto-models novarocks-spi; do
  failpoint_forbidden="$tmpdir/failpoint-${forbidden}.json"
  add_normal_resolve_edge novarocks-failpoint "$forbidden" "$failpoint_forbidden"
  assert_rejected "$failpoint_forbidden" \
    "novarocks-failpoint typed normal dependency closure contains forbidden packages:"
  grep -Fq "$forbidden" "$failpoint_forbidden.stderr"
done

failpoint_typed_feature="$tmpdir/failpoint-typed-feature.json"
jq '
  (.packages[] | select(.name == "novarocks-failpoint") | .features.typed) += [
    "dep:novarocks-spi"
  ]
' "$base_metadata" >"$failpoint_typed_feature"
assert_rejected "$failpoint_typed_feature" \
  "novarocks-failpoint feature typed must be exactly: dep:novarocks-types"

echo "native-wire-dependency-boundary-test: PASS"
