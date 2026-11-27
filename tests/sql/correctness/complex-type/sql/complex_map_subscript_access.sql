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

-- Migrated from: dev/test/sql/test_map/T/test_map (case: testMapTopN)
-- Test Objective:
-- 1. Validate MAP subscript access via [] operator and element_at() on literals and table columns.
-- 2. Validate ORDER BY on a MAP subscript value with LIMIT/OFFSET.
-- 3. Cover NULL values, empty maps, column probes, NULL probes, and rejected NULL keys.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.map_top_n (
    c1 INT,
    c2 MAP<VARCHAR(8), INT>
) WITH (format_version = 3);

INSERT INTO ${case_db}.map_top_n VALUES
    ROW(1, MAP(ARRAY['key1'], ARRAY[1])),
    ROW(2, MAP(ARRAY['key1', 'key2'], ARRAY[5, 6])),
    ROW(3, MAP(ARRAY['key1', 'key2', 'key4'], ARRAY[2, 3, 4])),
    ROW(4, MAP(ARRAY['key1', 'key2', 'key3', 'key4'], ARRAY[12, 13, 14, 15])),
    ROW(5, MAP(ARRAY['key1', 'key2', 'key3', 'key4', 'key5'], ARRAY[7, 8, 9, 10, 11]));

-- query 2
-- Full result ordered by map subscript value
SELECT * FROM ${case_db}.map_top_n ORDER BY c2['key1'];

-- query 3
-- Skip 1, take rest
SELECT * FROM ${case_db}.map_top_n ORDER BY c2['key1'] OFFSET 1
LIMIT  10;

-- query 4
-- Skip 2, take rest
SELECT * FROM ${case_db}.map_top_n ORDER BY c2['key1'] OFFSET 2
LIMIT  10;

-- query 5
-- Skip 3, take rest
SELECT * FROM ${case_db}.map_top_n ORDER BY c2['key1'] OFFSET 3
LIMIT  10;

-- query 6
-- Skip 4, take rest (last row only)
SELECT * FROM ${case_db}.map_top_n ORDER BY c2['key1'] OFFSET 4
LIMIT  10;

-- query 7
-- Skip 5 → empty result
SELECT * FROM ${case_db}.map_top_n ORDER BY c2['key1'] OFFSET 5
LIMIT  10;

-- query 8
-- element_at() on a map literal
SELECT element_at(MAP(ARRAY['a', 'b'], ARRAY[1, 2]), 'a');

-- query 9
-- [] subscript on a map literal
SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2])['a'];

-- query 10
-- Null-value lookup: key exists but maps to NULL
SELECT MAP(ARRAY['a', 'b'], ARRAY[NULL, 2])['a'];

-- query 11
-- Empty map lookup → NULL
SELECT element_at(map(), 'a');

-- query 12
-- Subscript access on a table column; order by pk for determinism
SELECT c2['key1'] FROM ${case_db}.map_top_n ORDER BY c1;

-- query 13
-- Map literal indexed by row column: c1=1→"a", c1=2→"b", c1>=3→NULL
SELECT element_at(MAP(ARRAY[1, 2], ARRAY['a', 'b']), c1) FROM ${case_db}.map_top_n ORDER BY c1;

-- query 14
-- The strict NULL probe suppresses the undemanded constructor error.
SELECT MAP(ARRAY[null, 2], ARRAY['a', 'b'])[null] AS skipped_constructor;

-- query 15
-- A NULL probe into a valid Map remains a successful NULL result.
SELECT MAP(ARRAY[1, 2], ARRAY['a', 'b'])[null] AS null_probe;

-- query 16
-- A non-NULL probe demands the constructor, which rejects the NULL key.
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT MAP(ARRAY[null, 2], ARRAY['a', 'b'])[2];
