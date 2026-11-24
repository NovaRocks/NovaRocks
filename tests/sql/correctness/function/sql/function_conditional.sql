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

-- Migrated from dev/test/sql/test_function/T/test_conditional_fn
-- Test Objective:
-- 1. Validate conditional functions (CASE/WHEN, COALESCE, IF, NULLIF) on nested types (ARRAY, MAP).
-- 2. Reject incompatible ARRAY and MAP element domains during common-type binding.
-- 3. Cover NULL propagation for each conditional function with nested-type columns.

-- query 1
-- Setup: create table with array and map columns
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE tc (
    "i" INTEGER,
    "ai" array<INTEGER>,
    "ass" array<varchar(100)>,
    "mi" map<INTEGER,int>,
    "ms" map<INTEGER,varchar(100)>
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO tc VALUES ROW(4, null, null, null, null);

-- query 3
-- @skip_result_check=true
-- iceberg: map keys must be non-null; dropped the null-key entry from ms (kept the non-null 1:null entry)
USE ${case_db};
INSERT INTO tc VALUES ROW(3, null, ['a','b'], null, MAP(ARRAY[1], ARRAY[CAST(NULL AS VARCHAR)]));

-- query 4
-- @skip_result_check=true
-- iceberg: map keys must be non-null; dropped the null-key entry from mi (kept the non-null 1:1 entry)
USE ${case_db};
INSERT INTO tc VALUES ROW(1, [1,2], null, MAP(ARRAY[1], ARRAY[1]), null);

-- query 5
-- @skip_result_check=true
-- iceberg: map keys must be non-null; dropped the null-key entries from mi and ms (kept the non-null entries)
USE ${case_db};
INSERT INTO tc VALUES ROW(2, [1,2], ['a','b'], MAP(ARRAY[1], ARRAY[1]), MAP(ARRAY[1], ARRAY[CAST('b' AS VARCHAR)]));

-- query 6
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- CASE WHEN with array columns (with else)
SELECT case i when 1 then ai when 3 then ass else ai end FROM ${case_db}.tc ORDER BY i;

-- query 7
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- CASE WHEN with array columns (without else)
SELECT case i when 1 then ai when 3 then ass end FROM ${case_db}.tc ORDER BY i;

-- query 8
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- CASE WHEN with map columns (with else)
SELECT case i when 1 then mi when 3 then ms else mi end FROM ${case_db}.tc ORDER BY i;

-- query 9
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- CASE WHEN with map columns (without else)
SELECT case i when 1 then mi when 3 then ms end FROM ${case_db}.tc ORDER BY i;

-- query 10
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- Searched CASE with array columns (without else)
SELECT case when i = 1 then ai when i=3 then ass end FROM ${case_db}.tc ORDER BY i;

-- query 11
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- Searched CASE with array columns (with else)
SELECT case when i = 1 then ai when i=3 then ass else ai end FROM ${case_db}.tc ORDER BY i;

-- query 12
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- Searched CASE with map columns (without else)
SELECT case when i = 1 then mi when i=3 then ms end FROM ${case_db}.tc ORDER BY i;

-- query 13
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- Searched CASE with map columns (with else)
SELECT case when i = 1 then mi when i=3 then ms else ms end FROM ${case_db}.tc ORDER BY i;

-- query 14
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- COALESCE on array columns
SELECT coalesce(ai, ass) FROM ${case_db}.tc ORDER BY i;

-- query 15
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- COALESCE on map columns
SELECT coalesce(mi, ms) FROM ${case_db}.tc ORDER BY i;

-- query 16
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- COALESCE with leading NULL literal on map columns
SELECT coalesce(null, ms, mi) FROM ${case_db}.tc ORDER BY i;

-- query 17
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- Two-argument COALESCE on map columns
SELECT coalesce(ms, mi), ms, mi FROM ${case_db}.tc ORDER BY i;

-- query 18
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- Two-argument COALESCE on array columns
SELECT coalesce(ass, ai), ass, ai FROM ${case_db}.tc ORDER BY i;

-- query 19
-- COALESCE with NULL literal as second arg
USE ${case_db};
SELECT coalesce(ai, null) FROM tc ORDER BY i;

-- query 20
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
-- @expect_error=[sql.analyze.unknown_function] no matching declared overload
-- IF on map columns
SELECT if(i>2, ms, mi) FROM ${case_db}.tc ORDER BY i;

-- query 21
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
-- @expect_error=[sql.analyze.unknown_function] no matching declared overload
-- IF on array columns
SELECT if(i>2, ass, ai) FROM ${case_db}.tc ORDER BY i;

-- query 22
-- IF with map_from_arrays and NULL
USE ${case_db};
SELECT if(ai is not null, map(['a', 'b'], [1, 2]), null) FROM tc ORDER BY i;

-- query 23
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
-- @expect_error=[sql.analyze.unknown_function] no matching declared overload
-- NULLIF on array columns
SELECT nullif(ai, ass), ai, ass FROM ${case_db}.tc ORDER BY i;

-- query 24
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
-- @expect_error=[sql.analyze.unknown_function] no matching declared overload
-- NULLIF on map columns
SELECT nullif(ms, mi), ms, mi FROM ${case_db}.tc ORDER BY i;

-- query 25
-- IF returning NULL when condition is false
USE ${case_db};
SELECT if(i > 5, i, null) FROM tc;

-- query 26
-- @order_sensitive=true
-- Freeze the migrated source maps, including parent NULL and NULL values.
USE ${case_db};
SELECT i, mi, ms, mi IS NULL AS mi_parent_null, ms IS NULL AS ms_parent_null FROM tc ORDER BY i;

-- query 27
USE ${case_db};
SELECT COUNT(*) AS rows_total, COUNT(mi) AS mi_present, COUNT(ms) AS ms_present, COUNT(ms[1]) AS ms_nonnull_values FROM tc;

-- query 28
SELECT IF(TRUE, SMALLINT '7') AS yes,
       IF(FALSE, SMALLINT '7') AS no,
       IF(NULL, SMALLINT '7') AS unknown;

-- query 29
SELECT IF(FALSE, 1 / 0) AS skipped,
       IF(NULL, 1 / 0) AS skipped_null;
