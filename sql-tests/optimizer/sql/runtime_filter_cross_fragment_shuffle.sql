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

-- M2: partitioned (Shuffle) hash-join probe runtime filters now cross a
-- key-aligned shuffle exchange into the producing fragment
-- (`hash_partition_carries_probe_key` in planner/runtime_filter_placement.rs). With real
-- ANALYZE stats small joins default to BROADCAST, so force a partitioned join
-- deterministically via cbo_broadcast_node_mem_budget_bytes = 0 (see
-- runtime_filter_cross_exchange.sql for the BROADCAST-only companion case).
CREATE TABLE ${case_db}.rf_shuffle_probe (k INT, v INT);
CREATE TABLE ${case_db}.rf_shuffle_build (k INT, v INT);
INSERT INTO ${case_db}.rf_shuffle_probe VALUES (1, 1), (2, 2), (3, 3), (4, 4);
INSERT INTO ${case_db}.rf_shuffle_build VALUES (1, 10), (2, 20), (3, 30), (4, 40);
ANALYZE TABLE ${case_db}.rf_shuffle_probe;
ANALYZE TABLE ${case_db}.rf_shuffle_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 0;

-- @explain_contains=HASH JOIN (PARTITIONED, INNER
-- @explain_contains=PARTITION: HASH_PARTITIONED (k)
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (p.k)
-- @explain_not_contains=HASH JOIN (BROADCAST
SELECT count(*) AS cnt
FROM ${case_db}.rf_shuffle_probe p
JOIN ${case_db}.rf_shuffle_build b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT count(*) AS cnt
FROM ${case_db}.rf_shuffle_probe p
JOIN ${case_db}.rf_shuffle_build b ON p.k = b.k;
