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

-- @tags=optimizer,oq13,subquery_to_window_rejected
-- Test Objective: ApplyToWindow is NOT applied to shapes that violate its
-- preconditions. Each sub-case asserts @explain_not_contains=WINDOW [ and
-- verifies the result rows are correct (ScalarApplyToJoin fallback is sound).
--
-- Three rejection guards:
--   1. Self-join: outer has two instances of the subquery's table.
--   2. Predicate mismatch: subquery has an extra residual filter absent from outer.
--   3. Distinct aggregate: avg(DISTINCT ...) is not window-eligible.
-- query 1
DROP TABLE IF EXISTS ${case_db}.wm_line;
-- query 2
DROP TABLE IF EXISTS ${case_db}.wm_part;
-- query 3
CREATE TABLE ${case_db}.wm_line (l_partkey INT, l_quantity INT, l_ext INT);
-- query 4
CREATE TABLE ${case_db}.wm_part (p_partkey INT, p_brand VARCHAR(16));
-- query 5
INSERT INTO ${case_db}.wm_line VALUES (1,5,100),(1,50,200),(2,7,300),(2,8,150),(3,9,90);
-- query 6
INSERT INTO ${case_db}.wm_part VALUES (1,'B1'),(2,'B1'),(3,'B2');
-- query 7
ANALYZE TABLE ${case_db}.wm_line;
-- query 8
ANALYZE TABLE ${case_db}.wm_part;
-- query 9

-- query 10

-- Guard 1: self-join — outer has two instances of wm_line (table-set check fails).
-- @explain_not_contains=WINDOW [
-- @explain_contains=OUTER
SELECT sum(a.l_ext)
FROM ${case_db}.wm_line a, ${case_db}.wm_line b
WHERE a.l_partkey = b.l_partkey
  AND a.l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = a.l_partkey);
-- query 11

-- Correctness for self-join rejection.
SELECT sum(a.l_ext)
FROM ${case_db}.wm_line a, ${case_db}.wm_line b
WHERE a.l_partkey = b.l_partkey
  AND a.l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = a.l_partkey);
-- query 12

-- Guard 2: predicate mismatch — subquery has extra residual filter (l_quantity > 0)
-- with no matching twin in the outer WHERE block.
-- @explain_not_contains=WINDOW [
-- @explain_contains=OUTER
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey AND l_quantity > 0);
-- query 13

-- Correctness for predicate mismatch rejection.
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey AND l_quantity > 0);
-- query 14

-- Guard 3: distinct aggregate — avg(DISTINCT ...) has no window analogue.
-- @explain_not_contains=WINDOW [
-- @explain_contains=OUTER
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(DISTINCT l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);
-- query 15

-- Correctness for distinct agg rejection.
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND l_quantity < (SELECT avg(DISTINCT l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);
-- query 16
