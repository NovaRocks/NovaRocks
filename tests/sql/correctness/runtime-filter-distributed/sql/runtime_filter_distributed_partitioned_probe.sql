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

-- @order_sensitive=true
-- @tags=runtime_filter,cross_process,distributed
-- Test Objective:
-- 1. M2: partitioned (Shuffle) hash-join probe runtime filters now cross a
--    key-aligned HASH_PARTITIONED exchange into the producing fragment
--    (`hash_partition_carries_probe_key` in planner/runtime_filter_placement.rs). Under a
--    real 1FE+3BE cross-process cluster this places the probe RF on the BE
--    fragment that scans the probe table, upstream of its own shuffle send.
-- 2. Force a PARTITIONED join (not BROADCAST) via
--    cbo_broadcast_node_mem_budget_bytes = 0, with the join key equal to the
--    shuffle key, so the RF must cross the exchange to reach the scan.
-- 3. Prove the crossing does NOT drop rows: RF-on and RF-off must return the
--    same fingerprint end-to-end under real cross-process BE execution.

CREATE TABLE ${case_db}.rf_dist_part_probe (
    id INT NOT NULL,
    k INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_dist_part_build (
    k INT,
    flag VARCHAR(8)
)
TBLPROPERTIES ("format-version" = "3");

-- 2000 probe rows, key space 0..199 (10 probe rows per key).
INSERT INTO ${case_db}.rf_dist_part_probe
SELECT generate_series AS id, generate_series % 200 AS k
FROM TABLE(generate_series(1, 2000));

-- 200 build rows, one per key; only even keys pass the flag = 'Y' predicate,
-- so the runtime filter has real work to prune (~half the probe rows). Write
-- three Iceberg data files so the partitioned build scan has cross-BE producer
-- work and the native terminal rollup observes a remote transport route.
INSERT INTO ${case_db}.rf_dist_part_build
SELECT generate_series % 200 AS k,
       CASE WHEN (generate_series % 200) % 2 = 0 THEN 'Y' ELSE 'N' END AS flag
FROM TABLE(generate_series(1, 67));

INSERT INTO ${case_db}.rf_dist_part_build
SELECT generate_series % 200 AS k,
       CASE WHEN (generate_series % 200) % 2 = 0 THEN 'Y' ELSE 'N' END AS flag
FROM TABLE(generate_series(68, 134));

INSERT INTO ${case_db}.rf_dist_part_build
SELECT generate_series % 200 AS k,
       CASE WHEN (generate_series % 200) % 2 = 0 THEN 'Y' ELSE 'N' END AS flag
FROM TABLE(generate_series(135, 200));

ANALYZE TABLE ${case_db}.rf_dist_part_probe;
ANALYZE TABLE ${case_db}.rf_dist_part_build;

-- Force a Shuffle (PARTITIONED) hash join: make broadcast prohibitively
-- expensive, and relax the RF build-size/selectivity gates so the M2 probe
-- placement is exercised deterministically regardless of ANALYZE stats
-- shape (mirrors tests/sql/correctness/optimizer/sql/runtime_filter_cross_fragment_shuffle.sql).
SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 0;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'partitioned_probe' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_part_probe p
JOIN ${case_db}.rf_dist_part_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_not_contains=RuntimeFilterApply:
EXPLAIN ANALYZE
SELECT COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_part_probe p
JOIN ${case_db}.rf_dist_part_build b ON p.k = b.k
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=HASH_PARTITIONED (p.k)
-- @explain_contains=HASH_PARTITIONED (b.k)
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
-- @expect_runtime_filter_available=available
-- @expect_runtime_filter_detail=completed-channel
-- @expect_runtime_filter_detail=accepted-producer
-- @expect_runtime_filter_detail=sent-acked-transport
-- @expect_runtime_filter_detail=delivered-applied-consumer
-- @expect_runtime_filter_total_at_least=channel_completed_count,1
-- @expect_runtime_filter_total_at_least=producer_accepted_count,1
-- @expect_runtime_filter_total_at_least=transport_sent_count,1
-- @expect_runtime_filter_total_at_least=transport_acked_count,1
-- @expect_runtime_filter_total_at_least=consumer_input_rows,1
SELECT 'partitioned_probe' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_part_probe p
JOIN ${case_db}.rf_dist_part_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_contains=RuntimeFilterApply: input_rows=2000 output_rows=1000
-- @result_contains=Profile: tasks=
-- @result_contains=HASH JOIN (PARTITIONED
EXPLAIN ANALYZE
SELECT COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_part_probe p
JOIN ${case_db}.rf_dist_part_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- The preceding @explain_contains directives already assert this plan shape.
-- Do not golden unstable query-local runtime-filter channel/binding ids.
-- @skip_result_check=true
EXPLAIN VERBOSE
SELECT 'partitioned_probe' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_part_probe p
JOIN ${case_db}.rf_dist_part_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- Add nullable rows only after the existing ordinary-equality proof so its
-- stable row-count and RuntimeFilterApply assertions remain unchanged.
INSERT INTO ${case_db}.rf_dist_part_probe VALUES
    (2001, NULL),
    (2002, NULL);

INSERT INTO ${case_db}.rf_dist_part_build VALUES
    (NULL, 'Y');

ANALYZE TABLE ${case_db}.rf_dist_part_probe;
ANALYZE TABLE ${case_db}.rf_dist_part_build;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
-- @result_contains=partitioned_null_safe_ok
SELECT CASE
         WHEN COUNT(*) = 2 AND COALESCE(SUM(p.id), 0) = 4003
           THEN 'partitioned_null_safe_ok'
         ELSE 'partitioned_null_safe_bad'
       END AS verdict
FROM ${case_db}.rf_dist_part_probe p
JOIN ${case_db}.rf_dist_part_build b ON p.k <=> b.k
WHERE p.id >= 2001 AND b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @result_contains=partitioned_null_safe_ok
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=HASH_PARTITIONED (p.k)
-- @explain_contains=HASH_PARTITIONED (b.k)
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
-- @expect_runtime_filter_available=available
-- @expect_runtime_filter_detail=completed-channel
-- @expect_runtime_filter_detail=accepted-producer
-- @expect_runtime_filter_detail=sent-acked-transport
-- @expect_runtime_filter_detail=delivered-applied-consumer
-- @expect_runtime_filter_total_at_least=channel_completed_count,1
-- @expect_runtime_filter_total_at_least=producer_accepted_count,1
-- @expect_runtime_filter_total_at_least=transport_sent_count,1
-- @expect_runtime_filter_total_at_least=transport_acked_count,1
-- @expect_runtime_filter_total_at_least=consumer_input_rows,1
SELECT CASE
         WHEN COUNT(*) = 2 AND COALESCE(SUM(p.id), 0) = 4003
           THEN 'partitioned_null_safe_ok'
         ELSE 'partitioned_null_safe_bad'
       END AS verdict
FROM ${case_db}.rf_dist_part_probe p
JOIN ${case_db}.rf_dist_part_build b ON p.k <=> b.k
WHERE p.id >= 2001 AND b.flag = 'Y';
