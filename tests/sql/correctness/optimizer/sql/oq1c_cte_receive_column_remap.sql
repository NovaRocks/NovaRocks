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

DROP TABLE IF EXISTS ${case_db}.oq1c_cte_src;
-- This case isolates CTE column remapping; write-time NDV must not alter the
-- exchange/cardinality golden that is incidental to that plan-shape contract.
CREATE TABLE ${case_db}.oq1c_cte_src (
    k INT,
    v INT,
    payload INT
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');
INSERT INTO ${case_db}.oq1c_cte_src VALUES
    (1, 10, 100),
    (2, 20, 200),
    (3, 30, 300);

-- Single consumer reads one producer column.
WITH c AS (
    SELECT k, v, payload FROM ${case_db}.oq1c_cte_src
)
SELECT k FROM c WHERE k >= 2 ORDER BY k;

-- Two consumers read different producer columns through different aliases.
WITH c AS (
    SELECT k, v, payload FROM ${case_db}.oq1c_cte_src
)
SELECT l.k, r.payload
FROM c l
JOIN c r ON l.k = r.k
WHERE l.v >= 20
ORDER BY l.k;

-- Predicate-only consumer column must still translate to producer required columns.
WITH c AS (
    SELECT k, v, payload FROM ${case_db}.oq1c_cte_src
)
SELECT payload FROM c WHERE v = 30;

-- Aggregate-backed producer keeps its full child output when aggregate pruning is disabled.
WITH c AS (
    SELECT k, COUNT(*) AS cnt FROM ${case_db}.oq1c_cte_src GROUP BY k
)
SELECT l.k
FROM c l
JOIN c r ON l.k = r.k
ORDER BY l.k;

-- @skip_result_check=true
-- @result_contains=CTE
EXPLAIN LOGICAL
WITH c AS (
    SELECT k, v, payload FROM ${case_db}.oq1c_cte_src
)
SELECT l.k, r.payload
FROM c l
JOIN c r ON l.k = r.k
WHERE l.v >= 20
ORDER BY l.k;

-- @skip_result_check=true
-- @explain_contains=EXCHANGE
WITH c AS (
    SELECT k, v, payload FROM ${case_db}.oq1c_cte_src
)
SELECT l.k, r.payload
FROM c l
JOIN c r ON l.k = r.k
WHERE l.v >= 20
ORDER BY l.k;

EXPLAIN VERBOSE
WITH c AS (
    SELECT k, v, payload FROM ${case_db}.oq1c_cte_src
)
SELECT l.k, r.payload
FROM c l
JOIN c r ON l.k = r.k
WHERE l.v >= 20
ORDER BY l.k;

DROP TABLE IF EXISTS ${case_db}.oq1c_cte_src;
