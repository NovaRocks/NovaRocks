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

-- @tags=optimizer,cse
-- Test Objective:
-- A repeated single-side scalar subexpression inside a join condition is
-- materialized once below that side of the Join while visible results stay stable.
DROP TABLE IF EXISTS ${case_db}.cse_join_l;
DROP TABLE IF EXISTS ${case_db}.cse_join_r;
CREATE TABLE ${case_db}.cse_join_l (a BIGINT);
CREATE TABLE ${case_db}.cse_join_r (b BIGINT);
INSERT INTO ${case_db}.cse_join_l VALUES ROW(2), ROW(5), ROW(9);
INSERT INTO ${case_db}.cse_join_r VALUES ROW(3), ROW(8), ROW(20), ROW(NULL);

SET SESSION disable_optimizer_rules = 'JoinCommutativity';

SELECT l.a, r.b
FROM ${case_db}.cse_join_l l
INNER JOIN ${case_db}.cse_join_r r
    ON (l.a * 2) > r.b AND (l.a * 2) < r.b + 10
ORDER BY l.a, r.b;

-- @explain_contains=NEST LOOP JOIN
-- @explain_contains=on: __cse_
-- @result_not_contains=__cse_
SELECT l.a, r.b
FROM ${case_db}.cse_join_l l
INNER JOIN ${case_db}.cse_join_r r
    ON (CAST(l.a AS DOUBLE) * 2e0) > CAST(r.b AS DOUBLE)
   AND (CAST(l.a AS DOUBLE) * 2e0) < CAST(r.b AS DOUBLE) + 10e0
ORDER BY l.a, r.b;

-- The shared expression reads the NULL-extended right output.
-- @explain_contains=__cse_0
-- @result_not_contains=__cse_
SELECT l.a, COALESCE(CAST(r.b AS DOUBLE), 0e0) AS x,
       COALESCE(CAST(r.b AS DOUBLE), 0e0) + 1e0 AS y
FROM ${case_db}.cse_join_l l
LEFT JOIN ${case_db}.cse_join_r r ON l.a = r.b - 1
ORDER BY l.a;

SET SESSION enable_common_subexpr_reuse = false;

-- @explain_not_contains=__cse_
-- @result_not_contains=__cse_
SELECT l.a, r.b
FROM ${case_db}.cse_join_l l
INNER JOIN ${case_db}.cse_join_r r
    ON (CAST(l.a AS DOUBLE) * 2e0) > CAST(r.b AS DOUBLE)
   AND (CAST(l.a AS DOUBLE) * 2e0) < CAST(r.b AS DOUBLE) + 10e0
ORDER BY l.a, r.b;

-- @explain_not_contains=__cse_
-- @result_not_contains=__cse_
SELECT l.a, COALESCE(CAST(r.b AS DOUBLE), 0e0) AS x,
       COALESCE(CAST(r.b AS DOUBLE), 0e0) + 1e0 AS y
FROM ${case_db}.cse_join_l l
LEFT JOIN ${case_db}.cse_join_r r ON l.a = r.b - 1
ORDER BY l.a;

SET SESSION enable_common_subexpr_reuse = true;
SET SESSION disable_optimizer_rules = '';
