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

-- Migrated from dev/test/sql/test_function/T/test_math
-- Test Objective:
-- 1. Validate cosine_similarity() on ARRAY<FLOAT> columns and literals.
-- 2. Validate l2_distance() on ARRAY<FLOAT> columns and literals.
-- 3. Verify vector math functions work correctly with table data and cross joins.
-- 4. Cover ordering by similarity/distance results.

-- query 1
-- Setup: create t1 with array<float> column
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (id int, data array<REAL>) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES ROW(1, CAST(ARRAY[DECIMAL '0.1', DECIMAL '0.2', DECIMAL '0.3'] AS array(REAL))), ROW(2, CAST(ARRAY[DECIMAL '0.2', DECIMAL '0.1', DECIMAL '0.3'] AS array(REAL))), ROW(3, CAST(ARRAY[DECIMAL '0.3', DECIMAL '0.2', DECIMAL '0.1'] AS array(REAL)));

-- query 3
-- cosine_similarity with literal array vs table column
USE ${case_db};
SELECT round(cosine_similarity(CAST([0.1, 0.2, 0.3] AS array<REAL>), data), 3) AS dist, id FROM t1 ORDER BY dist desc, id;

-- query 4
-- cosine_similarity on two literal arrays
USE ${case_db};
SELECT round(cosine_similarity(CAST([0.1, 0.2, 0.3] AS array<REAL>), CAST([0.1, 0.2, 0.3] AS array<REAL>)), 3) AS dist;

-- query 5
-- l2_distance with literal array vs table column
USE ${case_db};
SELECT round(l2_distance(CAST([0.1, 0.2, 0.3] AS array<REAL>), data), 3) AS dist, id FROM t1 ORDER BY dist desc, id;

-- query 6
-- l2_distance on two literal arrays
USE ${case_db};
SELECT round(l2_distance(CAST([0.1, 0.2, 0.3] AS array<REAL>), CAST([0.1, 0.2, 0.3] AS array<REAL>)), 3) AS dist;

-- query 7
-- Setup: create test_vector table for cross join tests
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE test_vector (id int, data array<REAL>) WITH (format_version = 3);

-- query 8
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_vector VALUES ROW(1, CAST(ARRAY[DECIMAL '0.1', DECIMAL '0.2', DECIMAL '0.3'] AS array(REAL))), ROW(2, CAST(ARRAY[DECIMAL '0.2', DECIMAL '0.1', DECIMAL '0.3'] AS array(REAL)));

-- query 9
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_vector VALUES ROW(3, CAST(ARRAY[DECIMAL '0.15', DECIMAL '0.25', DECIMAL '0.32'] AS array(REAL))), ROW(4, CAST(ARRAY[DECIMAL '0.12', DECIMAL '0.11', DECIMAL '0.32'] AS array(REAL)));

-- query 10
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_vector VALUES ROW(5, CAST(ARRAY[DECIMAL '0.25', DECIMAL '0.12', DECIMAL '0.13'] AS array(REAL))), ROW(6, CAST(ARRAY[DECIMAL '0.22', DECIMAL '0.01', DECIMAL '0.39'] AS array(REAL)));

-- query 11
-- cosine_similarity with literal vs table column, ordered by similarity
USE ${case_db};
SELECT id, data, round(cosine_similarity(CAST([0.1, 0.2, 0.3] AS array<REAL>), data), 3) AS sim FROM test_vector ORDER BY sim desc, id;

-- query 12
-- cosine_similarity cross join (first execution)
USE ${case_db};
SELECT a.id, b.id, a.data, b.data, round(cosine_similarity(a.data, b.data), 3) AS sim FROM test_vector AS a CROSS JOIN test_vector AS b ORDER BY sim desc, a.id, b.id;

-- query 13
-- cosine_similarity cross join (repeated for consistency)
USE ${case_db};
SELECT a.id, b.id, a.data, b.data, round(cosine_similarity(a.data, b.data), 3) AS sim FROM test_vector AS a CROSS JOIN test_vector AS b ORDER BY sim desc, a.id, b.id;

-- query 14
-- l2_distance with literal vs table column, ordered by distance
USE ${case_db};
SELECT id, data, round(l2_distance(CAST([0.1, 0.2, 0.3] AS array<REAL>), data), 3) AS sim FROM test_vector ORDER BY sim desc, id;
