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

-- Migrated from: dev/test/sql/test_map/T/test_map (case: testEmptyMap)
-- Test Objective:
-- 1. Validate map_concat with empty maps (map() literal as left/right operand).
-- 2. Cover FE errors for invalid map key types (MAP, ARRAY, and STRUCT as key).
-- 3. Validate map_entries on a table column and unnest(map_entries(...)).
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_map (
    col_int INT,
    col_map MAP<VARCHAR(50), INT>
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.test_map VALUES
    (1, map{"a":1,"b":2}),
    (2, map{"c":3}),
    (3, map{"d":4,"e":5});

-- query 2
-- map_concat: non-empty map on left, empty map on right
WITH t0 AS (SELECT c1 FROM (VALUES(map())) AS t(c1))
SELECT map_concat(map('a', 1, 'b', 2), c1) FROM t0;

-- query 3
-- map_concat: empty map on left, non-empty map on right
WITH t0 AS (SELECT c1 FROM (VALUES(map())) AS t(c1))
SELECT map_concat(c1, map('a', 1, 'b', 2)) FROM t0;

-- query 4
-- map_concat: both empty → empty map
SELECT map_concat(c1, map())
FROM (SELECT c1 FROM (VALUES(map())) AS t(c1)) t;

-- query 5
-- map_concat: empty + int-keyed map
SELECT map_concat(c1, map(1, 2))
FROM (SELECT c1 FROM (VALUES(map())) AS t(c1)) t;

-- query 6
-- @expect_error=Map key don't supported type: MAP
-- FE error: MAP type used as a map key
SELECT map_concat(c1, map(map(), map()))
FROM (SELECT c1 FROM (VALUES(map())) AS t(c1)) t;

-- query 7
-- @expect_error=Map key don't supported type: ARRAY
-- FE error: ARRAY type used as a map key
SELECT map_concat(c1, map([], map()))
FROM (SELECT c1 FROM (VALUES(map())) AS t(c1)) t;

-- query 8
-- @expect_error=Map key don't supported type: struct
-- FE error: STRUCT type used as a map key
SELECT map_concat(c1, map(named_struct('1', 2), named_struct('A', [map()])))
FROM (SELECT c1 FROM (VALUES(map())) AS t(c1)) t;

-- query 9
-- map_entries on a table column; order by pk for determinism
SELECT col_int, map_entries(col_map) FROM ${case_db}.test_map ORDER BY col_int;

-- query 10
-- unnest(map_entries(...)) on a table column; entries emitted in insertion order
SELECT col_int, map_entries(col_map), unnest
FROM ${case_db}.test_map, unnest(map_entries(col_map)) AS unnest(unnest)
ORDER BY col_int;
