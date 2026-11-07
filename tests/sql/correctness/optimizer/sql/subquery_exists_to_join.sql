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

-- @tags=optimizer,subquery_apply,exists_to_join
-- Test Objective:
-- Apply-mode EXISTS / NOT EXISTS subqueries are eliminated into semi/anti joins.
-- Disabling ExistentialApplyToJoin is covered by an explicit sanity query.
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
INSERT INTO ${case_db}.sq_t2 VALUES (1, 100), (1, 101), (3, 300), (5, 500);
-- query 7
ANALYZE TABLE ${case_db}.sq_t1;
-- query 8
ANALYZE TABLE ${case_db}.sq_t2;
-- query 9

-- query 10

-- Correlated EXISTS rewrites to a LEFT SEMI join and leaves no Apply node.
-- @explain_contains=HASH JOIN (BROADCAST, LEFT SEMI
-- @explain_not_contains=APPLY
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE EXISTS (
    SELECT 1
    FROM ${case_db}.sq_t2 t2
    WHERE t2.k = t1.k
)
ORDER BY k;
-- query 11

-- Correlated NOT EXISTS rewrites to a LEFT ANTI join and leaves no Apply node.
-- @explain_contains=HASH JOIN (BROADCAST, LEFT ANTI
-- @explain_not_contains=APPLY
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE NOT EXISTS (
    SELECT 1
    FROM ${case_db}.sq_t2 t2
    WHERE t2.k = t1.k
)
ORDER BY k;
-- query 12

SET disable_optimizer_rules='ExistentialApplyToJoin';
-- query 13

-- Disabling ExistentialApplyToJoin should make the Apply backstop reject the
-- still-correlated EXISTS instead of silently changing shape.
-- @expect_error=subquery decorrelation failed
SELECT k, v
FROM ${case_db}.sq_t1 t1
WHERE EXISTS (
    SELECT 1
    FROM ${case_db}.sq_t2 t2
    WHERE t2.k = t1.k
)
ORDER BY k;
-- query 14

SET disable_optimizer_rules='';
-- query 15
