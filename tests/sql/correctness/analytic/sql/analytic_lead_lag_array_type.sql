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

-- Migrated from: dev/test/sql/test_lead_lag_support_array_type/T/test_lead_lag_support_array_type
-- Test Objective:
-- 1. Validate first_value/last_value/lead/lag produce consistent results across
--    parallel VARCHAR columns (c2, c3) and an ARRAY<VARCHAR> column (c4).
-- 2. Cover both default (respect nulls) and IGNORE NULLS variants.
-- The assertion checks that concat(varchar_a, ",", varchar_b) == array_join(array_c, ",")
-- for every non-null row in the window function output, ensuring array and varchar
-- window semantics are in sync.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
  c0 INT ,
  c1 INT ,
  c2 VARCHAR(65533) ,
  c3 VARCHAR(65533) ,
  c4 ARRAY<VARCHAR(65533)>
) WITH (format_version = 3);

INSERT INTO ${case_db}.t0
SELECT
    i%10 AS c0,
    i AS c1,
    IF(i % 3 = 0, NULL, CONCAT('foo_', i)) AS c2,
    IF(i % 3 = 0, NULL, CONCAT('bar_', i)) AS c3,
    IF(i % 3 = 0, NULL, [CONCAT('foo_', i), CONCAT('bar_', i)]) AS c4
FROM TABLE(sequence(1, 10000)) t(i);

-- query 2
-- first_value: varchar and array results must match on every non-null row
WITH cte AS (
    SELECT
        first_value(c2) OVER (PARTITION BY c0 ORDER BY c1) AS a,
        first_value(c3) OVER (PARTITION BY c0 ORDER BY c1) AS b,
        first_value(c4) OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;

-- query 3
-- first_value IGNORE NULLS
WITH cte AS (
    SELECT
        first_value(c2 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS a,
        first_value(c3 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS b,
        first_value(c4 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;

-- query 4
-- last_value
WITH cte AS (
    SELECT
        last_value(c2) OVER (PARTITION BY c0 ORDER BY c1) AS a,
        last_value(c3) OVER (PARTITION BY c0 ORDER BY c1) AS b,
        last_value(c4) OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;

-- query 5
-- last_value IGNORE NULLS
WITH cte AS (
    SELECT
        last_value(c2 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS a,
        last_value(c3 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS b,
        last_value(c4 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;

-- query 6
-- lead
WITH cte AS (
    SELECT
        lead(c2) OVER (PARTITION BY c0 ORDER BY c1) AS a,
        lead(c3) OVER (PARTITION BY c0 ORDER BY c1) AS b,
        lead(c4) OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;

-- query 7
-- lead IGNORE NULLS
WITH cte AS (
    SELECT
        lead(c2 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS a,
        lead(c3 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS b,
        lead(c4 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;

-- query 8
-- lag
WITH cte AS (
    SELECT
        lag(c2) OVER (PARTITION BY c0 ORDER BY c1) AS a,
        lag(c3) OVER (PARTITION BY c0 ORDER BY c1) AS b,
        lag(c4) OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;

-- query 9
-- lag IGNORE NULLS
WITH cte AS (
    SELECT
        lag(c2 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS a,
        lag(c3 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS b,
        lag(c4 ) IGNORE NULLS OVER (PARTITION BY c0 ORDER BY c1) AS c
    FROM ${case_db}.t0
),
cte1 AS (
    SELECT concat(a, ',', b) = array_join(c, ',') AS pred FROM cte
)
SELECT assert_true(count(pred) = count(IF(pred, 1, NULL))) FROM cte1;
