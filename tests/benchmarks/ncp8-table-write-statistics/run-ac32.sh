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

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/../../.." && pwd)"
revision="$(git -C "$repo_root" rev-parse HEAD)"
if [[ -n "$(git -C "$repo_root" status --porcelain=v1 --untracked-files=all)" ]]; then
  echo "NCP-8 AC32 formal run requires a clean worktree" >&2
  exit 2
fi
artifact_root="${1:-$(mktemp -d "${TMPDIR:-/tmp}/ncp8-ac32.XXXXXX")}"
mkdir -p "$artifact_root"

execution_log="$artifact_root/execution.jsonl"
iceberg_log="$artifact_root/iceberg-owner-test.log"
report="$artifact_root/report.json"

set +e
(
  cd "$repo_root"
  cargo bench -p novarocks-execution --bench table_write_statistics
) 2>&1 | tee "$execution_log"
execution_exit=${PIPESTATUS[0]}

(
  cd "$repo_root"
  cargo test -p novarocks-connector-iceberg \
    ac32_publication_observability_report_has_real_puffin_io_and_no_data_reread \
    --lib -- --nocapture
) 2>&1 | tee "$iceberg_log"
iceberg_exit=${PIPESTATUS[0]}
set -e

set +e
python3 "$script_dir/combine_ac32.py" \
  --execution-jsonl "$execution_log" \
  --iceberg-test-log "$iceberg_log" \
  --output "$report" \
  --revision "$revision" \
  --execution-exit "$execution_exit" \
  --iceberg-exit "$iceberg_exit" \
  --source-clean true
combine_exit=$?
set -e

(
  cd "$artifact_root"
  shasum -a 256 execution.jsonl iceberg-owner-test.log report.json > checksums.sha256
)

echo "NCP-8 AC32 artifacts: $artifact_root"
echo "NCP-8 AC32 status: $(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["status"])' "$report")"
exit "$combine_exit"
