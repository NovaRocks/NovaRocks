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

-- Regression: a CTE referenced through multiple aliases with different per-alias
-- predicates must surface independent rows per alias.
--
-- This previously failed because the analyzer reused the CTE producer's
-- column_id list for every consume, so different aliases shared the same
-- ColumnId. Downstream joins, projections, and filters could no longer
-- distinguish the aliases. TPC-DS q11 (and many similar reports queries)
-- collapses to 0 rows when this is broken.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_cte_multi;
CREATE TABLE ${case_db}.t_cte_multi (
    y bigint ,
    v bigint
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_cte_multi VALUES ROW(2001, 10), ROW(2002, 20), ROW(2003, 30);

-- query 2
-- @order_sensitive=true
-- Two aliases of the same CTE filtered to different rows; cross join
-- should pick one row per side: (2001, 10) x (2002, 20).
WITH yt AS (SELECT y, v FROM ${case_db}.t_cte_multi)
SELECT a.y, a.v, b.y, b.v
FROM yt a, yt b
WHERE a.y = 2001 AND b.y = 2002
ORDER BY a.y, b.y;

-- query 3
-- @order_sensitive=true
-- Three aliases joined on a shared key, each filtered to a distinct row.
-- All three columns must come from their own alias (not collapse to one).
WITH yt AS (SELECT y, v FROM ${case_db}.t_cte_multi)
SELECT a.y, b.y, c.y, a.v, b.v, c.v
FROM yt a JOIN yt b ON 1=1 JOIN yt c ON 1=1
WHERE a.y = 2001 AND b.y = 2002 AND c.y = 2003
ORDER BY a.y, b.y, c.y;

-- query 4
-- @order_sensitive=true
-- Aggregated CTE consumed by two aliases joined on the aggregate key.
-- Validates that aggregates flowing through multiple consumers retain
-- per-alias identity.
WITH agg AS (
    SELECT y AS gy, SUM(v) AS sv FROM ${case_db}.t_cte_multi GROUP BY y
)
SELECT a.gy, a.sv, b.gy, b.sv
FROM agg a, agg b
WHERE a.gy = 2001 AND b.gy = 2002
ORDER BY a.gy, b.gy;
