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
CHECKER="$REPO_ROOT/tools/ci/check-application-domain-dependency-boundary.py"
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

base_metadata="$tmpdir/base.json"
cargo metadata --manifest-path "$REPO_ROOT/Cargo.toml" --format-version 1 >"$base_metadata"
python3 "$CHECKER" --metadata-path "$base_metadata"

package_id() {
  jq -er --arg name "$1" '.packages[] | select(.name == $name) | .id' "$base_metadata"
}

assert_edge_rejected() {
  local source="$1"
  local target="$2"
  local expected="$3"
  local output="$tmpdir/${source}-${target}.json"
  local source_id
  local target_id
  source_id="$(package_id "$source")"
  target_id="$(package_id "$target")"
  jq --arg source_id "$source_id" --arg target_id "$target_id" '
    .resolve.nodes |= map(
      if .id == $source_id then
        .deps += [{
          name: "application_domain_boundary_mutation",
          pkg: $target_id,
          dep_kinds: [{kind: null, target: null}]
        }]
      else
        .deps |= map(select(.pkg != $source_id))
      end
    )
  ' "$base_metadata" >"$output"
  if python3 "$CHECKER" --metadata-path "$output" >"$output.stdout" 2>"$output.stderr"; then
    echo "Application domain dependency mutation was accepted: $source -> $target" >&2
    exit 1
  fi
  grep -Fq "$expected" "$output.stderr"
  grep -Fq "$target" "$output.stderr"
}

assert_edge_rejected novarocks-query-application novarocks-worker \
  "novarocks-query-application normal dependency closure contains forbidden domains:"
assert_edge_rejected novarocks-worker novarocks-query-application \
  "novarocks-worker normal dependency closure contains forbidden domains:"
assert_edge_rejected novarocks-execution novarocks-query-application \
  "novarocks-execution normal dependency closure contains forbidden domains:"
assert_edge_rejected novarocks-workload-control novarocks-task-codec \
  "novarocks-workload-control normal dependency closure contains forbidden domains:"
assert_edge_rejected novarocks-query-application novarocks-task-codec \
  "novarocks-query-application normal dependency closure contains forbidden domains:"
assert_edge_rejected novarocks-worker novarocks-proto-models \
  "novarocks-worker normal dependency closure contains forbidden domains:"

# The checker follows package ids in the resolved graph, so dependency aliases
# cannot hide a violation. A target predicate remains a normal edge for the
# architecture boundary and is checked independent of this host platform.
query_id="$(package_id novarocks-query-application)"
worker_id="$(package_id novarocks-worker)"
target_gated="$tmpdir/target-gated.json"
jq --arg query_id "$query_id" --arg worker_id "$worker_id" '
  .resolve.nodes |= map(
    if .id == $query_id then
      .deps += [{
        name: "renamed_worker_dependency",
        pkg: $worker_id,
        dep_kinds: [{kind: null, target: "cfg(target_os = \"none\")"}]
      }]
    else
      .deps |= map(select(.pkg != $query_id))
    end
  )
' "$base_metadata" >"$target_gated"
if python3 "$CHECKER" --metadata-path "$target_gated" \
    >"$target_gated.stdout" 2>"$target_gated.stderr"; then
  echo "Target-gated renamed query-to-worker dependency was accepted" >&2
  exit 1
fi
grep -Fq "novarocks-query-application normal dependency closure contains forbidden domains:" \
  "$target_gated.stderr"
grep -Fq "novarocks-worker" "$target_gated.stderr"

# A violation hidden below an otherwise legal dependency must be rejected by
# closure rather than by inspecting only direct manifest entries.
types_id="$(package_id novarocks-types)"
transitive="$tmpdir/transitive.json"
jq --arg types_id "$types_id" --arg worker_id "$worker_id" '
  .resolve.nodes |= map(
    if .id == $types_id then
      .deps += [{
        name: "transitive_worker_dependency",
        pkg: $worker_id,
        dep_kinds: [{kind: null, target: null}]
      }]
    else
      .
    end
  )
' "$base_metadata" >"$transitive"
if python3 "$CHECKER" --metadata-path "$transitive" \
    >"$transitive.stdout" 2>"$transitive.stderr"; then
  echo "Transitive query-to-worker dependency was accepted" >&2
  exit 1
fi
grep -Fq "novarocks-execution normal dependency closure contains forbidden domains:" \
  "$transitive.stderr"
grep -Fq "novarocks-worker" "$transitive.stderr"

missing_resolve="$tmpdir/missing-resolve.json"
jq '.resolve = null' "$base_metadata" >"$missing_resolve"
if python3 "$CHECKER" --metadata-path "$missing_resolve" \
    >"$missing_resolve.stdout" 2>"$missing_resolve.stderr"; then
  echo "Metadata without a resolved graph was accepted" >&2
  exit 1
fi
grep -Fq "Cargo metadata must include resolve nodes" "$missing_resolve.stderr"

workload_id="$(package_id novarocks-workload-control)"
query_without_workload="$tmpdir/query-without-workload.json"
jq --arg query_id "$query_id" --arg workload_id "$workload_id" '
  .resolve.nodes |= map(
    if .id == $query_id then
      .deps |= map(select(.pkg != $workload_id))
    else
      .
    end
  )
' "$base_metadata" >"$query_without_workload"
if python3 "$CHECKER" --metadata-path "$query_without_workload" \
    >"$query_without_workload.stdout" 2>"$query_without_workload.stderr"; then
  echo "Query application without workload control was accepted" >&2
  exit 1
fi
grep -Fq \
  "novarocks-query-application normal dependency closure must contain novarocks-workload-control" \
  "$query_without_workload.stderr"

# Future product packages are checked by package identity. A legal product to
# query edge is accepted; adding wire ownership beside it is rejected.
product_id="path+file:///application-boundary-fixture#novarocks-mv-application@0.0.0"
task_codec_id="$(package_id novarocks-task-codec)"
legal_product="$tmpdir/legal-product.json"
jq \
  --arg product_id "$product_id" \
  --arg query_id "$query_id" \
  --arg workload_id "$workload_id" '
  (.packages[] | select(.name == "novarocks-workload-control")) as $template
  | .packages += [
      ($template
        | .name = "novarocks-mv-application"
        | .id = $product_id
        | .dependencies = [])
    ]
  | (.resolve.nodes[] | select(.id == $workload_id)) as $node
  | .resolve.nodes += [
      ($node
        | .id = $product_id
        | .deps = [{
            name: "novarocks_query_application",
            pkg: $query_id,
            dep_kinds: [{kind: null, target: null}]
          }])
    ]
' "$base_metadata" >"$legal_product"
python3 "$CHECKER" --metadata-path "$legal_product" >/dev/null

illegal_product="$tmpdir/illegal-product.json"
jq --arg product_id "$product_id" --arg task_codec_id "$task_codec_id" '
  .resolve.nodes |= map(
    if .id == $product_id then
      .deps += [{
        name: "optional_task_wire_when_enabled",
        pkg: $task_codec_id,
        dep_kinds: [{kind: null, target: null}]
      }]
    else
      .
    end
  )
' "$legal_product" >"$illegal_product"
if python3 "$CHECKER" --metadata-path "$illegal_product" \
    >"$illegal_product.stdout" 2>"$illegal_product.stderr"; then
  echo "Product dependency on task wire was accepted" >&2
  exit 1
fi
grep -Fq "novarocks-mv-application normal dependency closure contains forbidden owners:" \
  "$illegal_product.stderr"
grep -Fq "novarocks-task-codec" "$illegal_product.stderr"

echo "application-domain-dependency-boundary-test: PASS"
