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

-- @order_sensitive=false
-- @tags=join,float,null_semantics
-- Test Objective: Preserve IEEE equality, null-safe identity, and evaluation of fallible join conditions.
-- Oracle: Trino 484 terminal values, type signatures, and error names.
-- Maintenance: Join suite owner; small case-owned tables exercise distributed build, probe, and cleanup.

-- query 1
-- Reference: relation_inner_equal
-- @explain_contains=HASH JOIN
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / 0e0)), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / 0e0)), (30, CAST(NULL AS DOUBLE))
)
SELECT l.id FROM l JOIN r ON l.x = r.y ORDER BY l.id;

-- query 2
-- Reference: relation_inner_nullsafe
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / 0e0)), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / 0e0)), (30, CAST(NULL AS DOUBLE))
)
SELECT l.id FROM l JOIN r ON l.x IS NOT DISTINCT FROM r.y ORDER BY l.id;

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc6a_float_left (id INTEGER, x DOUBLE) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc6a_float_right (id INTEGER, y DOUBLE) WITH (format_version = 3);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.trc6a_float_left VALUES
    ROW(1, 0e0 / 0e0), ROW(2, -0e0), ROW(3, NULL),
    ROW(4, 2e0), ROW(5, 0e0);

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.trc6a_float_right VALUES
    ROW(11, 0e0 / 0e0), ROW(12, 0e0), ROW(13, NULL),
    ROW(14, 3e0), ROW(15, 0e0 / 0e0);

-- query 7
-- Reference: relation_filter_equals
SELECT l.id AS left_id, r.id AS right_id
FROM ${case_db}.trc6a_float_left l CROSS JOIN ${case_db}.trc6a_float_right r
WHERE l.x = r.y
ORDER BY l.id, r.id;

-- query 8
-- Reference: relation_outer_equal
-- @explain_contains=HASH JOIN (PARTITIONED, FULL OUTER
SELECT l.id AS left_id, r.id AS right_id
FROM ${case_db}.trc6a_float_left l FULL OUTER JOIN ${case_db}.trc6a_float_right r ON l.x = r.y
ORDER BY l.id, r.id;

-- query 9
-- Reference: relation_outer_nullsafe
-- @explain_contains=HASH JOIN
SELECT l.id AS left_id, r.id AS right_id
FROM ${case_db}.trc6a_float_left l FULL OUTER JOIN ${case_db}.trc6a_float_right r ON l.x IS NOT DISTINCT FROM r.y
ORDER BY l.id, r.id;

-- query 10
-- @skip_result_check=true
DROP TABLE ${case_db}.trc6a_float_left;

-- query 11
-- @skip_result_check=true
DROP TABLE ${case_db}.trc6a_float_right;

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc6a_decimal_probe (id INTEGER, k DECIMAL(2,1)) WITH (format_version = 3);

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc6a_decimal_build (id INTEGER, n DECIMAL(2,1), d DECIMAL(2,1)) WITH (format_version = 3);

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.trc6a_decimal_probe VALUES ROW(1, 1.0);

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.trc6a_decimal_build VALUES ROW(2, 1.0, 0.0), ROW(3, 2.0, 2.0);

-- query 16
-- Reference: relation_decimal_key_empty_side
SELECT l.id AS left_id, r.id AS right_id
FROM (SELECT id, k FROM ${case_db}.trc6a_decimal_probe WHERE id = 0) l
JOIN ${case_db}.trc6a_decimal_build r ON l.k = r.n / r.d;

-- query 17
-- Reference: relation_decimal_key_filtered_side
-- @explain_contains=NEST LOOP JOIN
-- @explain_not_contains=HASH JOIN
SELECT l.id AS left_id, r.id AS right_id
FROM ${case_db}.trc6a_decimal_probe l
JOIN (SELECT id, n, d FROM ${case_db}.trc6a_decimal_build WHERE id = 3) r ON l.k = r.n / r.d;

-- query 18
-- Reference: relation_decimal_key_selected
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT l.id AS left_id, r.id AS right_id
FROM ${case_db}.trc6a_decimal_probe l JOIN ${case_db}.trc6a_decimal_build r ON l.k = r.n / r.d;

-- query 19
-- @skip_result_check=true
DROP TABLE ${case_db}.trc6a_decimal_probe;

-- query 20
-- @skip_result_check=true
DROP TABLE ${case_db}.trc6a_decimal_build;

-- query 21
-- NovaRocks baseline: uncorrelated_in_filter
-- @explain_contains=LEFT SEMI
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / CAST(0e0 AS REAL))), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / CAST(0e0 AS REAL))), (30, CAST(NULL AS DOUBLE))
)
SELECT id FROM l WHERE x IN (SELECT y FROM r) ORDER BY id;

-- query 22
-- NovaRocks baseline: correlated_in_filter
-- @explain_contains=LEFT SEMI
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / CAST(0e0 AS REAL))), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / CAST(0e0 AS REAL))), (30, CAST(NULL AS DOUBLE))
)
SELECT id FROM l WHERE x IN (SELECT y FROM r WHERE r.g = l.g) ORDER BY id;

-- query 23
-- NovaRocks baseline: uncorrelated_not_in_nonnull_filter
-- @explain_contains=NULL AWARE LEFT ANTI
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / CAST(0e0 AS REAL))), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / CAST(0e0 AS REAL))), (30, CAST(NULL AS DOUBLE))
)
SELECT id FROM l WHERE x NOT IN (SELECT y FROM r WHERE y IS NOT NULL) ORDER BY id;

-- query 24
-- NovaRocks baseline: uncorrelated_not_in_with_null_filter
-- @explain_contains=NULL AWARE LEFT ANTI
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / CAST(0e0 AS REAL))), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / CAST(0e0 AS REAL))), (30, CAST(NULL AS DOUBLE))
)
SELECT id FROM l WHERE x NOT IN (SELECT y FROM r) ORDER BY id;

-- query 25
-- NovaRocks baseline: correlated_not_in_filter
-- @explain_contains=NULL AWARE LEFT ANTI
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / CAST(0e0 AS REAL))), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / CAST(0e0 AS REAL))), (30, CAST(NULL AS DOUBLE))
)
SELECT id FROM l WHERE x NOT IN (SELECT y FROM r WHERE r.g = l.g) ORDER BY id;

-- query 26
-- NovaRocks baseline: correlated_membership_value
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
WITH l(id,g,x) AS (
  VALUES (1, 10, (0e0 / CAST(0e0 AS REAL))), (2, 10, 1.0e0),
         (3, 10, CAST(NULL AS DOUBLE)), (4, 20, 1.0e0),
         (5, 20, CAST(NULL AS DOUBLE))
), r(g,y) AS (
  VALUES (10, (0e0 / CAST(0e0 AS REAL))), (30, CAST(NULL AS DOUBLE))
)
SELECT id, x IN (SELECT y FROM r WHERE r.g = l.g) AS in_result, x NOT IN (SELECT y FROM r WHERE r.g = l.g) AS not_in_result FROM l ORDER BY id;
