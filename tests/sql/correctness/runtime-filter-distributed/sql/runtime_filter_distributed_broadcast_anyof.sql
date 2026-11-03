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
-- 1. Force a Broadcast hash join with a small selective build side.
-- 2. Prove RF-off and RF-on return the same deterministic fingerprint.
-- 3. Prove the native consumer actively prunes 6000 probe rows to 20.

CREATE TABLE ${case_db}.rf_dist_broadcast_probe (
    id INT NOT NULL,
    k INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_dist_broadcast_build (
    k INT,
    flag VARCHAR(8)
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_dist_broadcast_probe
SELECT generate_series AS id, generate_series % 600 AS k
FROM TABLE(generate_series(1, 6000));

INSERT INTO ${case_db}.rf_dist_broadcast_build
SELECT generate_series % 600 AS k,
       CASE WHEN generate_series % 600 IN (11, 29) THEN 'Y' ELSE 'N' END AS flag
FROM TABLE(generate_series(1, 600));

ANALYZE TABLE ${case_db}.rf_dist_broadcast_probe;
ANALYZE TABLE ${case_db}.rf_dist_broadcast_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 10737418240;

SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT 'broadcast_anyof' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_broadcast_probe p
JOIN ${case_db}.rf_dist_broadcast_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_not_contains=RuntimeFilterApply:
EXPLAIN ANALYZE
SELECT COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_broadcast_probe p
JOIN ${case_db}.rf_dist_broadcast_build b ON p.k = b.k
WHERE b.flag = 'Y';

SET disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (BROADCAST
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
SELECT 'broadcast_anyof' AS scenario, COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_broadcast_probe p
JOIN ${case_db}.rf_dist_broadcast_build b ON p.k = b.k
WHERE b.flag = 'Y';

-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_contains=RuntimeFilterApply: input_rows=6000 output_rows=20
-- @result_contains=Profile: tasks=
-- @result_contains=HASH JOIN (BROADCAST
EXPLAIN ANALYZE
SELECT COUNT(*) AS row_count, COALESCE(SUM(p.id), 0) AS id_sum
FROM ${case_db}.rf_dist_broadcast_probe p
JOIN ${case_db}.rf_dist_broadcast_build b ON p.k = b.k
WHERE b.flag = 'Y';
