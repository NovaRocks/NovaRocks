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
INSERT INTO ${case_db}.cse_join_l VALUES (2), (5), (9);
INSERT INTO ${case_db}.cse_join_r VALUES (3), (8), (20);

SET disable_optimizer_rules = 'JoinCommutativity';

SELECT l.a, r.b
FROM ${case_db}.cse_join_l l
INNER JOIN ${case_db}.cse_join_r r
    ON l.a * 2 > r.b AND l.a * 2 < r.b + 10
ORDER BY l.a, r.b;

-- @explain_contains=NEST LOOP JOIN
-- @explain_contains=on: __cse_
-- @result_not_contains=__cse_
SELECT l.a, r.b
FROM ${case_db}.cse_join_l l
INNER JOIN ${case_db}.cse_join_r r
    ON l.a * 2 > r.b AND l.a * 2 < r.b + 10
ORDER BY l.a, r.b;

SET disable_optimizer_rules = '';
