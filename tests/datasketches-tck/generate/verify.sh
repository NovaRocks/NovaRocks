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

repo_root=$(cd "$(dirname "$0")/../../.." && pwd)
fixture_root="$repo_root/tests/datasketches-tck/fixtures"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/novarocks-datasketches-fixtures.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

compare_generated() {
  local generated_root=$1
  local count=0
  while IFS= read -r generated; do
    local family
    local name
    family=$(basename "$(dirname "$generated")")
    name=$(basename "$generated")
    cmp "$generated" "$fixture_root/$family/$name"
    count=$((count + 1))
  done < <(find "$generated_root" -type f \( -name '*.sk' -o -name '*.tsv' \) | sort)
  test "$count" -gt 0
  echo "$count"
}

"$repo_root/tests/datasketches-tck/generate/java/generate.sh" "$work_dir/java"
java_count=$(compare_generated "$work_dir/java")

"$repo_root/tests/datasketches-tck/generate/cpp/generate.sh" "$work_dir/cpp"
cpp_count=$(compare_generated "$work_dir/cpp")

cargo run --quiet --locked -p novarocks-datasketches-tck --bin generate-rust-fixtures -- \
  "$work_dir/rust"
rust_count=$(compare_generated "$work_dir/rust")

printf 'verified fixtures: java=%s cpp=%s rust=%s\n' \
  "$java_count" "$cpp_count" "$rust_count"
