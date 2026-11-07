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

-- @tags=optimizer,subquery_apply,in_to_join
-- Test Objective:
-- Apply-mode IN / NOT IN subqueries are eliminated into semi/null-aware anti joins.
-- The nullable NOT IN case includes a NULL inner value so NAAJ semantics matter.
-- query 1
DROP TABLE IF EXISTS ${case_db}.sq_t1;
-- query 2
DROP TABLE IF EXISTS ${case_db}.sq_t2;
-- query 3
CREATE TABLE ${case_db}.sq_t1 (k INT, v INT);
-- query 4
CREATE TABLE ${case_db}.sq_t2 (k INT, v INT);
-- query 5
INSERT INTO ${case_db}.sq_t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40);
-- query 6
INSERT INTO ${case_db}.sq_t2 VALUES (1, 100), (3, 300), (5, NULL);
-- query 7
ANALYZE TABLE ${case_db}.sq_t1;
-- query 8
ANALYZE TABLE ${case_db}.sq_t2;
-- query 9

-- query 10

-- IN rewrites to a LEFT SEMI join and leaves no Apply node.
-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_not_contains=APPLY
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE t1.k IN (
    SELECT t2.k
    FROM ${case_db}.sq_t2 t2
)
ORDER BY k;
-- query 11

-- Nullable NOT IN rewrites to a NULL AWARE LEFT ANTI join and leaves no Apply node.
-- @explain_contains=HASH JOIN (BROADCAST, NULL AWARE LEFT ANTI
-- @explain_not_contains=APPLY
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE t1.v NOT IN (
    SELECT t2.v
    FROM ${case_db}.sq_t2 t2
)
ORDER BY k;
-- query 12

SET disable_optimizer_rules='QuantifiedApplyToJoin';
-- query 13

-- Disabling QuantifiedApplyToJoin should make the Apply backstop reject the
-- still-quantified IN instead of silently changing shape.
-- @expect_error=subquery decorrelation failed
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE t1.k IN (
    SELECT t2.k
    FROM ${case_db}.sq_t2 t2
)
ORDER BY k;
-- query 14

SET disable_optimizer_rules='';
-- query 15
