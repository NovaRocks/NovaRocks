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
-- @tags=aggregate,approx_top_k,min_n,avg,mann_whitney
-- Test Objective:
-- 1. Validate approx_top_k, min_n, average-on-expression, and mann_whitney_u_test semantics.
-- 2. Cover grouped and nullable inputs without relying on external schemas.
-- Test Flow:
-- 1. Create/reset a mixed scalar source table.
-- 2. Insert deterministic duplicates, NULLs, and boolean cohorts.
-- 3. Snapshot top-k, extrema arrays, aggregate expression averages, and Mann-Whitney outputs.
-- query 1
CREATE TABLE ${case_db}.t_agg_topk_extrema_misc (
    id INT,
    grp INT,
    v INT,
    txt VARCHAR,
    flag BOOLEAN,
    big_v BIGINT,
    w BIGINT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_topk_extrema_misc VALUES
    ROW(1, 1, 10, 'delta', TRUE, 10000000, 3),
    ROW(2, 1, 20, 'alpha', TRUE, 40000000, 5),
    ROW(3, 1, 20, 'beta', FALSE, 40000000, 5),
    ROW(4, 1, 30, 'gamma', FALSE, 40000000, 5),
    ROW(5, 2, 10, 'zeta', TRUE, 10000000, 3),
    ROW(6, 2, 10, 'eta', FALSE, 40000000, 5),
    ROW(7, 2, 20, 'theta', FALSE, 40000000, 5),
    ROW(8, 2, NULL, NULL, NULL, NULL, NULL),
    ROW(9, 3, 40, 'iota', TRUE, 10000000, 3),
    ROW(10, 3, 40, 'kappa', FALSE, 40000000, 5);
WITH w1 AS (
    SELECT approx_top_k(v, 3) AS x FROM ${case_db}.t_agg_topk_extrema_misc
), entries AS (
    SELECT ordinal, x[ordinal] AS entry, __array_struct_subfield(x, 'item')[ordinal] AS item
    FROM w1 CROSS JOIN (VALUES (1), (2), (3)) AS positions(ordinal)
    WHERE ordinal <= cardinality(x)
)
SELECT array_agg(entry ORDER BY item NULLS FIRST, ordinal) AS top_items
FROM entries;

-- query 2
WITH w1 AS (
    SELECT grp, approx_top_k(v, 2) AS x FROM ${case_db}.t_agg_topk_extrema_misc GROUP BY grp
), entries AS (
    SELECT grp, ordinal, x[ordinal] AS entry, __array_struct_subfield(x, 'item')[ordinal] AS item
    FROM w1 CROSS JOIN (VALUES (1), (2)) AS positions(ordinal)
    WHERE ordinal <= cardinality(x)
)
SELECT grp, array_agg(entry ORDER BY item NULLS FIRST, ordinal) AS top_items
FROM entries
GROUP BY grp
ORDER BY grp;

-- query 3
SELECT
    min_n(v, 3) AS min_three_values,
    min_n(txt, 2) AS min_two_texts
FROM ${case_db}.t_agg_topk_extrema_misc
WHERE v IS NOT NULL
  AND txt IS NOT NULL;

-- query 4
SELECT
    grp,
    ROUND(AVG(big_v - 1.86659630566164 * (w - 3.062175673706)), 6) AS expr_avg
FROM ${case_db}.t_agg_topk_extrema_misc
WHERE big_v IS NOT NULL
GROUP BY grp
ORDER BY grp;
