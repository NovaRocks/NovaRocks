-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- @sequential=true
-- @expect_error_tier=drift

-- query 1
-- @result_contains=IRU9_BOUNDS_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode bounds --namespace ${case_db} --table java_variant_bounds_${uuid0} --output "$receipt_dir" >"$receipt_dir/run.log"
cat "$receipt_dir/run.log" >&2
grep -E '^IRU9_(BOUNDS_OK|FIXTURE_COMPLETE)' "$receipt_dir/run.log"

-- query 2
-- Variant-history hints remain private provider diagnostics, covered by the
-- provider history/reader regressions. This bounds fixture retains the exact
-- existing Unavailable fact; reason 4:8 is not a Variant-specific refusal.
-- @expect_sql_code=query.execution.dependency
-- @expect_sql_phase=Execution
-- @expect_error=(reason 4:8)
SELECT id FROM iceberg_compat_${suite_uuid0}.${case_db}.java_variant_bounds_${uuid0};

-- query 3
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS iceberg_compat_${suite_uuid0}.${case_db}.java_variant_bounds_${uuid0} ;
