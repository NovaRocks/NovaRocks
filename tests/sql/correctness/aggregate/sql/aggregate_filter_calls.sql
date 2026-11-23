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

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.filter_error_input (x INTEGER);

-- query 2
WITH input(g, x, p, q) AS (
    VALUES ROW(1, 10, true, false),
           ROW(1, CAST(NULL AS integer), true, false),
           ROW(1, 30, false, true),
           ROW(1, 40, CAST(NULL AS boolean), true),
           ROW(2, 50, false, false),
           ROW(2, 60, CAST(NULL AS boolean), CAST(NULL AS boolean))
)
SELECT g, count(*) AS n, count(*) FILTER (WHERE p) AS p_rows,
       count(x) FILTER (WHERE p) AS p_values, sum(x) FILTER (WHERE q) AS q_sum
FROM input GROUP BY g ORDER BY g;

-- query 3
SELECT count(*) FILTER (WHERE true) AS n, sum(x) FILTER (WHERE true) AS total
FROM ${case_db}.filter_error_input;

-- query 4
SELECT x, count(*) FILTER (WHERE true) AS n
FROM ${case_db}.filter_error_input GROUP BY x;

-- query 5
WITH input(x, sort_key, keep_row) AS (
    VALUES ROW(7, 4, false), ROW(7, 3, true),
           ROW(CAST(NULL AS integer), 2, true), ROW(8, 1, true)
)
SELECT count(DISTINCT x) FILTER (WHERE keep_row) AS distinct_x,
       array_agg(x ORDER BY sort_key) FILTER (WHERE keep_row) AS ordered_x
FROM input;

-- query 6
SELECT count(*) FILTER (WHERE NULL) AS untyped_null,
       count(*) FILTER (WHERE CAST(NULL AS boolean)) AS boolean_null
FROM (VALUES ROW(7)) AS input(x);

-- query 7
WITH input(g, x) AS (
    VALUES ROW(1, 1), ROW(1, 30), ROW(1, 40), ROW(2, 9), ROW(2, 50), ROW(3, 70)
)
SELECT g, count(*) FILTER (WHERE x > 10) AS large_rows
FROM input GROUP BY g
HAVING count(*) FILTER (WHERE x <= 10) > 0
ORDER BY large_rows DESC, g;

-- query 8
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT count(*) FILTER (WHERE 1);

-- query 9
-- @expect_trino_error=FUNCTION_NOT_AGGREGATE,USER_ERROR
SELECT abs(-1) FILTER (WHERE true);

-- query 10
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT count(*) FILTER (WHERE true) OVER ();

-- query 11
-- @expect_trino_error=EXPRESSION_NOT_SCALAR,USER_ERROR
SELECT count(*) FILTER (WHERE sum(x) > 0)
FROM (VALUES ROW(7)) AS input(x);

-- query 12
-- @expect_trino_error=EXPRESSION_NOT_SCALAR,USER_ERROR
SELECT count(*) FILTER (WHERE row_number() OVER () > 0)
FROM (VALUES ROW(7)) AS input(x);

-- query 13
-- @expect_trino_error=EXPRESSION_NOT_SCALAR,USER_ERROR
SELECT count(*) FILTER (WHERE GROUPING(x) = 0)
FROM (VALUES ROW(7)) AS input(x)
GROUP BY GROUPING SETS ((x), ());

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.filter_error_input VALUES ROW(0), ROW(2);

-- query 15
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT count(*) FILTER (WHERE 10 / x > 0) FROM ${case_db}.filter_error_input;

-- query 16
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT sum(10 / x) FILTER (WHERE x <> 0) FROM ${case_db}.filter_error_input;

-- query 17
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT array_agg(x ORDER BY 10 / x) FILTER (WHERE x <> 0) FROM ${case_db}.filter_error_input;

-- query 18
SELECT sum(IF(x <> 0, 10 / x, CAST(NULL AS INTEGER))) FILTER (WHERE x <> 0) AS total
FROM ${case_db}.filter_error_input;

-- query 19
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.filter_error_input;
