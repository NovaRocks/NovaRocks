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
-- @tags=filter,subquery
-- Test Objective:
-- 1. Multiple subqueries in the same SELECT (WHERE, HAVING, projection) must
--    all be rewritten. This was previously broken because
--    `infer_scalar_subquery_data_type` re-entered the analyzer and drained
--    the outer query's `collected_subqueries`, so any subquery seen BEFORE
--    a scalar one ended up un-rewritten and reached codegen as a stray
--    `SubqueryPlaceholder`. TPC-DS q14 (IN + HAVING-scalar) was the
--    canonical reproduction.
-- 2. Cover the three trigger shapes: IN-then-scalar in WHERE,
--    scalar-then-scalar in WHERE, multiple scalars in projection.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_subq;
CREATE TABLE ${case_db}.t_subq (
    id BIGINT ,
    v BIGINT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_subq VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30), ROW(4, 40), ROW(5, 50);

-- query 2
-- IN-then-scalar in WHERE — q14 shape. id IN {1,2,3} and v greater than min v (=10).
SELECT id, v FROM ${case_db}.t_subq
WHERE id IN (SELECT id FROM ${case_db}.t_subq WHERE id <= 3)
  AND v > (SELECT MIN(v) FROM ${case_db}.t_subq)
ORDER BY id;

-- query 3
-- scalar-then-IN in WHERE — opposite order (was already working pre-fix, included for parity).
SELECT id, v FROM ${case_db}.t_subq
WHERE v > (SELECT MIN(v) FROM ${case_db}.t_subq)
  AND id IN (SELECT id FROM ${case_db}.t_subq WHERE id <= 3)
ORDER BY id;

-- query 4
-- Two scalar subqueries in WHERE.
SELECT id, v FROM ${case_db}.t_subq
WHERE v > (SELECT MIN(v) FROM ${case_db}.t_subq)
  AND v < (SELECT MAX(v) FROM ${case_db}.t_subq)
ORDER BY id;

-- query 5
-- Two scalar subqueries in projection.
SELECT (SELECT MIN(v) FROM ${case_db}.t_subq) AS min_v,
       (SELECT MAX(v) FROM ${case_db}.t_subq) AS max_v;

-- query 6
-- IN in WHERE + scalar in HAVING (TPC-DS q14 step 1 shape).
SELECT id, sum(v) AS sv FROM ${case_db}.t_subq
WHERE id IN (SELECT id FROM ${case_db}.t_subq WHERE id <= 4)
GROUP BY id
HAVING sum(v) > (SELECT MIN(v) FROM ${case_db}.t_subq)
ORDER BY id;
