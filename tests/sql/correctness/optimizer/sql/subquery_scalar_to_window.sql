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

-- @tags=optimizer,oq13,subquery_to_window
-- Test Objective: a correlated scalar-aggregate subquery in a WHERE comparison
-- is rewritten to an analytic WINDOW over the outer relation (StarRocks WinMagic),
-- not a re-scan + LEFT OUTER JOIN. apply mode only.
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

-- Apply mode rewrites the scalar aggregate subquery to a window function.
-- @explain_contains=WINDOW [
-- @explain_not_contains=APPLY
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);
-- query 11

-- Correctness: apply-mode result rows (golden captures them; they must equal legacy).
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);
-- query 12

SET disable_optimizer_rules='ApplyToWindow';
-- query 13

-- With ApplyToWindow disabled, falls back to the outer JOIN form.
-- @explain_contains=OUTER
-- @explain_not_contains=WINDOW [
SELECT sum(l_ext)
FROM ${case_db}.wm_line, ${case_db}.wm_part
WHERE p_partkey = l_partkey
  AND p_brand = 'B1'
  AND l_quantity < (SELECT 2 * avg(l_quantity) FROM ${case_db}.wm_line WHERE l_partkey = p_partkey);
-- query 14

SET disable_optimizer_rules='';
