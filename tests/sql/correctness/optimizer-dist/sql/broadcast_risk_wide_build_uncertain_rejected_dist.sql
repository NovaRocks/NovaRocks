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
CREATE TABLE probe_1m_exact (k INT);
CREATE TABLE build_wide_unanalyzed (k INT, pad VARCHAR(500))
TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');
INSERT INTO probe_1m_exact
    SELECT generate_series FROM TABLE(generate_series(1, 1000000));
INSERT INTO build_wide_unanalyzed
    SELECT generate_series, repeat('z', 500) FROM TABLE(generate_series(1, 200000));
ANALYZE TABLE probe_1m_exact;
SET cbo_broadcast_node_mem_budget_bytes = 268435456;
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_not_contains=BROADCAST, INNER
-- @explain_not_contains=memory=inf
SELECT COUNT(*) AS cnt FROM probe_1m_exact p JOIN build_wide_unanalyzed b ON p.k = b.k;

EXPLAIN VERBOSE
SELECT COUNT(*) AS cnt FROM probe_1m_exact p JOIN build_wide_unanalyzed b ON p.k = b.k;

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
