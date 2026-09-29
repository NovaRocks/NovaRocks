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

-- @tags=optimizer,bc1,distribution,dist-only
CREATE DATABASE IF NOT EXISTS ${case_db};
USE ${case_db};
SET disable_optimizer_rules = 'JoinReorder,JoinCommutativity';
-- Assert the selected distribution and the edge contract, without node numbers.
-- @skip_result_check=true
-- @result_contains=HASH JOIN (PARTITIONED, INNER
-- @result_contains=HASH EXCHANGE
-- @result_contains=PARTITION: HASH_PARTITIONED
-- @result_contains=HASH AGGREGATE (LOCAL)
-- @result_contains=HASH AGGREGATE (GLOBAL)
-- @result_not_contains=HASH JOIN (BROADCAST
-- @result_not_contains=BROADCAST EXCHANGE
-- @result_not_contains=memory=inf
-- @result_contains=GENERATE_SERIES(1, 100000000, 1)
EXPLAIN COSTS
WITH probe_1m AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000000))
),
build_fallback_risky AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 100000000))
)
SELECT COUNT(*) AS cnt
FROM probe_1m p JOIN build_fallback_risky b ON p.k = b.k;

SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- The build has ten i64 keys: 80 payload bytes; every backend receives it.
-- @skip_result_check=true
-- @result_contains=HASH JOIN (BROADCAST, INNER
-- @result_contains=BROADCAST EXCHANGE
-- @result_contains=PARTITION: BROADCAST
-- @result_contains=bcast_verdict=feasible
-- @result_contains=RUNTIME FILTER GRAPH
-- @result_contains=activation = BlockingSnapshot
-- @result_not_contains=HASH JOIN (PARTITIONED
-- @query_stats_contract={"tables":[],"broadcast":{"distribution":"BROADCAST","join_kind":"INNER","verdict":"feasible","forced":false,"backends":3,"risk_multiplier":2,"per_node_budget_bytes":268435456,"cluster_network_budget_bytes":268435456},"payload":{"kind":"exact","bytes":80},"hash_table":{"kind":"exact","build_rows":10,"load_factor":0.75,"per_row_overhead_bytes":16}}
EXPLAIN COSTS
WITH p AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 1000))
),
b AS (
    SELECT generate_series AS k
    FROM TABLE(generate_series(1, 10))
)
SELECT COUNT(*) AS cnt
FROM p JOIN b ON p.k = b.k;
