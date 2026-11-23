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
-- Keep original aggregate inputs separate from nullifiable Repeat keys.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_grouping_input (a INT, b INT);
INSERT INTO ${case_db}.t_grouping_input VALUES ROW(1, 10);
INSERT INTO ${case_db}.t_grouping_input VALUES ROW(1, 20);
INSERT INTO ${case_db}.t_grouping_input VALUES ROW(2, 30);
INSERT INTO ${case_db}.t_grouping_input VALUES ROW(NULL, 40);

-- query 2
SELECT GROUPING(a) AS g, a, count(*) AS rows_seen,
       count(a) AS nonnull_a, count(DISTINCT a) AS distinct_a,
       sum(a) AS sum_a, sum(b) AS sum_b,
       array_agg(a ORDER BY b) AS ordered_a
FROM ${case_db}.t_grouping_input
GROUP BY ROLLUP(a)
ORDER BY g, a NULLS FIRST;

-- query 3
SELECT GROUPING(k) AS g, k AS k, sum(k) AS total
FROM (SELECT a + 1 AS k FROM ${case_db}.t_grouping_input) AS s
GROUP BY ROLLUP(k)
ORDER BY g, k NULLS FIRST;

-- query 4
SELECT GROUPING(a) AS g, a, sum(a) AS total
FROM ${case_db}.t_grouping_input
GROUP BY ROLLUP(a)
HAVING sum(a) > 0
ORDER BY g, a NULLS FIRST;

-- query 5
SELECT GROUPING(a) AS g, a,
       sum(CAST(b BETWEEN 15 AND 35 AS BIGINT)) AS in_range
FROM ${case_db}.t_grouping_input
GROUP BY ROLLUP(a)
ORDER BY g, a NULLS FIRST;

-- query 6
SELECT GROUPING("sum(a)") AS g, "sum(a)" AS k, sum(a) AS total
FROM (SELECT a, b AS "sum(a)" FROM ${case_db}.t_grouping_input) AS s
GROUP BY ROLLUP("sum(a)")
ORDER BY g, k NULLS FIRST;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_grouping_input ;

-- query 8
WITH grouping_input(k) AS (SELECT 1 UNION ALL SELECT 2)
SELECT GROUPING(k) AS g, -k AS negated
FROM grouping_input
GROUP BY GROUPING SETS ((k), ())
ORDER BY g, negated;

-- query 9
WITH input(a, b) AS (
    VALUES ROW(1, 10), ROW(1, 20), ROW(2, 30), ROW(CAST(NULL AS integer), 40)
)
SELECT CAST(GROUPING(a) AS integer) AS g, a,
       count(*) FILTER (WHERE a IS NULL) AS original_nulls,
       sum(b) FILTER (WHERE a > 0) AS positive_keys,
       count(*) FILTER (WHERE false) AS none_count,
       sum(b) FILTER (WHERE false) AS none_sum
FROM input GROUP BY GROUPING SETS ((a), ())
ORDER BY g, a NULLS FIRST;

-- query 10
WITH input(k) AS (SELECT 1)
SELECT count(*) AS groups_seen, count(total) AS nonnull_values,
       count(DISTINCT total) AS distinct_values
FROM (
    SELECT CAST(GROUPING(k) AS integer) AS g,
           sum(random()) FILTER (WHERE k > 0) AS total
    FROM input GROUP BY GROUPING SETS ((k), ())
) AS grouped;
