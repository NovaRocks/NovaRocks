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
-- 2. Preserve MAP, ARRAY, and ROW keys when the other map is empty.
-- 3. Validate map_entries on a table column and unnest(map_entries(...)).
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_map (
    col_int INT,
    col_map MAP<VARCHAR(50), INT>
) WITH (format_version = 3);

INSERT INTO ${case_db}.test_map VALUES
    ROW(1, MAP(ARRAY['a', 'b'], ARRAY[1, 2])),
    ROW(2, MAP(ARRAY['c'], ARRAY[3])),
    ROW(3, MAP(ARRAY['d', 'e'], ARRAY[4, 5]));

-- query 2
-- map_concat: non-empty map on left, empty map on right
WITH t0 AS (SELECT c1 FROM (VALUES ROW(map())) AS t(c1))
SELECT map_concat(MAP(ARRAY['a', 'b'], ARRAY[1, 2]), c1) FROM t0;

-- query 3
-- map_concat: empty map on left, non-empty map on right
WITH t0 AS (SELECT c1 FROM (VALUES ROW(map())) AS t(c1))
SELECT map_concat(c1, MAP(ARRAY['a', 'b'], ARRAY[1, 2])) FROM t0;

-- query 4
-- map_concat: both empty → empty map
SELECT map_concat(c1, map())
FROM (SELECT c1 FROM (VALUES ROW(map())) AS t(c1)) t;

-- query 5
-- map_concat: empty + int-keyed map
SELECT map_concat(c1, MAP(ARRAY[1], ARRAY[2]))
FROM (SELECT c1 FROM (VALUES ROW(map())) AS t(c1)) t;

-- query 6
-- Verify the nested MAP key through the declared type.
WITH merged AS (
    SELECT map_concat(c1, MAP(ARRAY[map()], ARRAY[map()])) AS m
    FROM (VALUES ROW(map())) AS t(c1)
), entries AS (
    SELECT cardinality(m) AS merged_entries, element_at(map_entries(m), 1) AS e
    FROM merged
)
SELECT merged_entries, cardinality(e[1]) AS key_entries, cardinality(e[2]) AS value_entries
FROM entries;

-- query 7
-- Verify the ARRAY key and MAP value through their declared types.
WITH merged AS (
    SELECT map_concat(c1, MAP(ARRAY[[]], ARRAY[map()])) AS m
    FROM (VALUES ROW(map())) AS t(c1)
), entries AS (
    SELECT cardinality(m) AS merged_entries, element_at(map_entries(m), 1) AS e
    FROM merged
)
SELECT merged_entries, cardinality(e[1]) AS key_entries, cardinality(e[2]) AS value_entries
FROM entries;

-- query 8
-- Read the ROW key and the empty MAP nested in its ROW value by position.
WITH merged AS (
    SELECT map_concat(c1, MAP(ARRAY[CAST(ROW(2) AS ROW("1" INTEGER))],
                              ARRAY[CAST(ROW(ARRAY[MAP()]) AS ROW("A" ARRAY(MAP(INTEGER, INTEGER))))])) AS m
    FROM (VALUES ROW(map())) AS t(c1)
), entries AS (
    SELECT cardinality(m) AS merged_entries, element_at(map_entries(m), 1) AS e
    FROM merged
)
SELECT merged_entries, e[1][1] AS key_value, cardinality(e[2][1]) AS value_array_entries,
       cardinality(element_at(e[2][1], 1)) AS nested_map_entries
FROM entries;

-- query 9
-- map_entries on a table column; order by pk for determinism
SELECT col_int, map_entries(col_map) FROM ${case_db}.test_map ORDER BY col_int;

-- query 10
-- unnest(map_entries(...)) on a table column; entries emitted in insertion order
SELECT col_int, map_entries(col_map), "unnest"
FROM ${case_db}.test_map, unnest(map_entries(col_map)) AS "unnest"("unnest")
ORDER BY col_int;
