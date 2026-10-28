-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @tags=optimizer,iceberg,variant_path_pushdown
-- Test Objective:
-- Verify VariantPathPushdown exposes scan-level variant path materialization in
-- EXPLAIN VERBOSE, and that disabling the rule removes the scan hint.
-- Golden derivation: intrinsic::variant_get is MayRaise, so
-- PushDownPredicateScan leaves its predicate on a FILTER above the scan.
-- registry::query_rewrite_pipeline runs VariantPathPushdown after the final
-- predicate pass: it materializes __nr_var_v_0 and rewrites that FILTER but
-- does not rerun predicate pushdown. Disabling the rule retains the original
-- variant_get FILTER. Keep the existing 25000 estimate above the filter;
-- the unfiltered scan uses the 100000 missing-statistics fallback, not zero.
-- Add FILTER plus its predicate line and remove the old scan predicate line
-- (net +1 row per plan); scan materialization/min-max capability stay intact.
-- This is a predicate-placement change, not a display-only correction.
-- completion_predicate::lower_column explicitly rejects synthetic variant
-- slots as provider predicate columns; completion_driver::provider_columns
-- also excludes them from provider assignments. This refusal is at provider
-- lowering, not a synthetic-slot guard in PushDownPredicateScan itself.
-- contract_lowering::lower_scan retains the exact scalar binding and path
-- in derived_values; Worker ConnectorVariantPathTransform materializes the
-- input page before the downstream FILTER. This query has no other predicate
-- that could suppress the strict extraction's original row-error domain.
-- The deterministic namespace/table preserve the full EXPLAIN golden.
-- This case owns that REST name under the global SQL-runner mutex. A prior
-- failed table is not dropped or reused: Java creation must fail explicitly.
-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG `iru9_variant_path_${uuid0}`
PROPERTIES (
    "type"="iceberg",
    "iceberg.catalog.type"="rest",
    "uri"="${iceberg_rest_uri}",
    "warehouse"="${iceberg_rest_warehouse}",
    "credential.object-store-metadata.consumer-role" = "frontend",
    "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
    "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
    "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
    "credential.object-store-data.consumer-role" = "backend",
    "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
    "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
    "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
    "aws.s3.endpoint"="${oss_endpoint}",
    "aws.s3.region"="${oss_region}",
    "aws.s3.enable_path_style_access"="${oss_path_style}"
);

-- query 2
-- @result_contains=IRU9_EMPTY_INT_OK
shell: set -eu
receipt_parent="${XDG_STATE_HOME:-$HOME/.local/state}/novarocks/experiments/iceberg-variant"
mkdir -p "$receipt_parent"
receipt_dir="$(mktemp -d "$receipt_parent/$(date -u +%Y%m%dT%H%M%SZ)-XXXXXX")"
"${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-variant/run.sh" --mode empty-int --namespace ${case_db} --table t_variant_path_pushdown --output "$receipt_dir" >"$receipt_dir/run.log"
cat "$receipt_dir/run.log" >&2
grep -E '^IRU9_(EMPTY_INT_OK|FIXTURE_COMPLETE)' "$receipt_dir/run.log"

-- query 3
-- @explain_contains=variant columns:
-- @explain_contains=variant_get(v, '$.a', 'bigint')
-- @explain_contains=variant columns: __nr_var_v_0 := variant_get(v, '$.a', 'bigint')
-- @explain_contains=columns: id, v, __nr_var_v_0
-- @explain_contains=2:FILTER stats={rows=25000}
-- @explain_contains=predicate: 1 = __nr_var_v_0
-- @explain_contains=stats={rows=100000}
-- @explain_not_contains=predicates:
EXPLAIN VERBOSE SELECT id
FROM iru9_variant_path_${uuid0}.${case_db}.t_variant_path_pushdown
WHERE variant_get(v, '$.a', 'bigint') = 1;

-- query 4
SET disable_optimizer_rules = 'VariantPathPushdown';

-- query 5
-- @explain_not_contains=variant columns:
-- @explain_not_contains=__nr_var_v_0
-- @explain_contains=2:FILTER stats={rows=25000}
-- @explain_contains=predicate: variant_get(v, '$.a', 'bigint') = 1
-- @explain_contains=columns: id, v
-- @explain_contains=stats={rows=100000}
-- @explain_not_contains=predicates:
EXPLAIN VERBOSE SELECT id
FROM iru9_variant_path_${uuid0}.${case_db}.t_variant_path_pushdown
WHERE variant_get(v, '$.a', 'bigint') = 1;

-- query 6
SET disable_optimizer_rules = '';

-- query 7
DROP TABLE iru9_variant_path_${uuid0}.${case_db}.t_variant_path_pushdown FORCE;
DROP CATALOG iru9_variant_path_${uuid0};
