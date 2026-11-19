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

-- @tags=pir8,optimizer,runtime_filter,distributed
-- PIR-8 M3 guard: a partitioned hash join must keep the runtime-filter probe
-- target visible across the shuffle exchange after planner/codegen layering
-- guards are tightened.
-- Golden derivation: append_exchange retains the probe p.k and build b.k
-- names. Assert both hash sinks instead of the old ambiguous k substring.
-- Keep producer fragment 1/node 4 and consumer fragment 2/node 7, including
-- BlockingSnapshot, the two hash exchanges, and the four-row result.

CREATE TABLE ${case_db}.pir8_rf_probe (k INT, v INT);
CREATE TABLE ${case_db}.pir8_rf_build (k INT, v INT);

INSERT INTO ${case_db}.pir8_rf_probe VALUES
  (1, 1),
  (2, 2),
  (3, 3),
  (4, 4);
INSERT INTO ${case_db}.pir8_rf_build VALUES
  (1, 10),
  (2, 20),
  (3, 30),
  (4, 40);

ANALYZE TABLE ${case_db}.pir8_rf_probe;
ANALYZE TABLE ${case_db}.pir8_rf_build;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 0;

-- @explain_contains=HASH JOIN (PARTITIONED, INNER
-- @explain_contains=PARTITION: HASH_PARTITIONED (p.k)
-- @explain_contains=PARTITION: HASH_PARTITIONED (b.k)
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
-- @explain_contains=expr = (p.k)
-- @explain_not_contains=HASH JOIN (BROADCAST
SELECT count() AS cnt
FROM ${case_db}.pir8_rf_probe p
JOIN ${case_db}.pir8_rf_build b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT count() AS cnt
FROM ${case_db}.pir8_rf_probe p
JOIN ${case_db}.pir8_rf_build b ON p.k = b.k;
