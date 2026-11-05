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
-- 1. Force a partitioned hash join whose producer fragment has multiple
--    participants and shares at least one BE with a distinct consumer fragment.
-- 2. Prove the native runtime-filter service actively prunes the shared probe path.
-- 3. Keep the result identical with runtime-filter placement disabled and enabled.
--    The exact local-shard missing-key counterexample is covered by the paired
--    deterministic pipeline fixture; this case supplies the actual 1FE+3BE topology.

CREATE TABLE ${case_db}.rf_active_partial_probe (
    id INT NOT NULL,
    k INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_active_partial_build (
    k INT,
    flag VARCHAR(8)
)
TBLPROPERTIES ("format-version" = "3");

-- Ten probe rows for each key in 0..599. Keys 11 and 29 deliberately occur
-- throughout the input rather than in one adjacent range.
INSERT INTO ${case_db}.rf_active_partial_probe
SELECT generate_series AS id, generate_series % 600 AS k
FROM TABLE(generate_series(1, 6000));

-- The build scan is non-trivial, but only keys 11 and 29 survive into the join.
INSERT INTO ${case_db}.rf_active_partial_build
SELECT generate_series % 600 AS k,
       CASE WHEN generate_series % 600 IN (11, 29) THEN 'Y' ELSE 'N' END AS flag
FROM TABLE(generate_series(1, 600));

ANALYZE TABLE ${case_db}.rf_active_partial_probe;
ANALYZE TABLE ${case_db}.rf_active_partial_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 0;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'partial_coverage' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_active_partial_probe p
JOIN ${case_db}.rf_active_partial_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_not_contains=RuntimeFilterApply:
EXPLAIN ANALYZE
SELECT COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_active_partial_probe p
JOIN ${case_db}.rf_active_partial_build b ON p.k = b.k
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=HASH_PARTITIONED (p.k)
-- @explain_contains=HASH_PARTITIONED (b.k)
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT 'partial_coverage' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_active_partial_probe p
JOIN ${case_db}.rf_active_partial_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_contains=RuntimeFilterApply: input_rows=6000 output_rows=20
-- @result_contains=Profile: tasks=
-- @result_contains=HASH JOIN (PARTITIONED
EXPLAIN ANALYZE
SELECT COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_active_partial_probe p
JOIN ${case_db}.rf_active_partial_build b ON p.k = b.k
WHERE b.flag = 'Y';
