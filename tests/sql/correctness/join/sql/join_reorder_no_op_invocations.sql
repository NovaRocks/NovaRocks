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

-- @tags=join,reorder,performance
-- Test Objective:
-- 1. Regression: the JoinReorder rewrite rule used to run its full
--    `reorder_joins_cbo` walk at *every* node visited by the pipeline,
--    including non-Join nodes (Project, Filter, CTEAnchor, SubqueryAlias,
--    Scan, …). For plans with deeply-nested CTEs / set ops / correlated
--    subqueries (the TPC-DS q14 shape), the redundant per-node walks
--    alone exhausted the 10-second optimizer budget before any of the
--    real per-join work could finish, surfacing as
--    `optimizer timeout during JoinReorder`.
-- 2. This test shapes a small query that hits the same redundancy
--    pattern (multiple CTEs feeding both an IN-subquery in WHERE and a
--    scalar subquery in HAVING, plus a derived-table self-join). With
--    the fix, optimization completes well under the budget.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_jr_left;
DROP TABLE IF EXISTS ${case_db}.t_jr_right;
DROP TABLE IF EXISTS ${case_db}.t_jr_dim;
CREATE TABLE ${case_db}.t_jr_left (
    k BIGINT ,
    v BIGINT ,
    d BIGINT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_jr_right (
    k BIGINT ,
    v BIGINT ,
    d BIGINT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_jr_dim (
    d BIGINT ,
    label BIGINT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_jr_left  VALUES ROW(1, 10, 100), ROW(2, 20, 200), ROW(3, 30, 100);
INSERT INTO ${case_db}.t_jr_right VALUES ROW(1, 11, 100), ROW(2, 22, 200), ROW(3, 33, 200);
INSERT INTO ${case_db}.t_jr_dim   VALUES ROW(100, 1), ROW(200, 2), ROW(300, 3);

-- query 2
-- @order_sensitive=true
-- TPC-DS q14 shape, miniaturised: two CTEs (one referenced via IN in WHERE,
-- the other via a scalar in HAVING), plus a derived-table self-join. This
-- compounded into a deeply-nested plan that the old JoinReorder rule
-- spent the entire 10-second budget walking redundantly.
WITH allowed_keys AS (
    SELECT k FROM ${case_db}.t_jr_left
    INTERSECT
    SELECT k FROM ${case_db}.t_jr_right
),
avg_val AS (
    SELECT AVG(v) AS avg_v FROM (
        SELECT v FROM ${case_db}.t_jr_left
        UNION ALL
        SELECT v FROM ${case_db}.t_jr_right
    ) x
)
SELECT t_now.k AS k, t_now.s AS s_now, t_prev.s AS s_prev
FROM
    (SELECT l.k AS k, SUM(l.v * r.v) AS s
     FROM ${case_db}.t_jr_left l, ${case_db}.t_jr_right r, ${case_db}.t_jr_dim d
     WHERE l.k = r.k
       AND l.d = d.d
       AND l.k IN (SELECT k FROM allowed_keys)
     GROUP BY l.k
     HAVING SUM(l.v * r.v) > (SELECT avg_v FROM avg_val)) t_now,
    (SELECT l.k AS k, SUM(l.v * r.v) AS s
     FROM ${case_db}.t_jr_left l, ${case_db}.t_jr_right r, ${case_db}.t_jr_dim d
     WHERE l.k = r.k
       AND r.d = d.d
       AND l.k IN (SELECT k FROM allowed_keys)
     GROUP BY l.k
     HAVING SUM(l.v * r.v) > (SELECT avg_v FROM avg_val)) t_prev
WHERE t_now.k = t_prev.k
ORDER BY t_now.k;
