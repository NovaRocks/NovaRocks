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

base_ref=${1:?usage: check-against-base.sh <base-ref>}
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_root=$(cd -- "$script_dir/../.." && pwd)
# NOVA_CI_ARTIFACT_RECEIPT must name the last canonical preparation receipt.
# Consume its verified executable without starting another Cargo graph.
source "$repo_root/tools/ci/lib/build_capabilities.sh"
error_manifest="$(REPO_ROOT="$repo_root" ci_bound_binary novarocks-error-manifest)"
merge_base=$(git -C "$repo_root" merge-base "$base_ref" HEAD)
ledger_path=tools/error-manifest/error-ledger-v1.tsv
baseline=$(mktemp)
trap 'rm -f "$baseline"' EXIT

if git -C "$repo_root" cat-file -e "$merge_base:$ledger_path" 2>/dev/null; then
  git -C "$repo_root" show "$merge_base:$ledger_path" > "$baseline"
else
  printf 'schema_version=0\n' > "$baseline"
fi

"$error_manifest" --check-against "$baseline"
