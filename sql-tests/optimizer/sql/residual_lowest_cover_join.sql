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

-- @tags=optimizer,oq9,residual,lowest_cover
-- Test Objective:
-- Keep a cross-side OR residual at the lowest join that covers its columns.
DROP TABLE IF EXISTS ${case_db}.residual_lcj_a;
DROP TABLE IF EXISTS ${case_db}.residual_lcj_b;
DROP TABLE IF EXISTS ${case_db}.residual_lcj_c;
CREATE TABLE ${case_db}.residual_lcj_a (k INT, bucket INT, payload INT);
CREATE TABLE ${case_db}.residual_lcj_b (k INT, bucket INT, payload INT);
CREATE TABLE ${case_db}.residual_lcj_c (k INT, flag INT, payload INT);

SET disable_optimizer_rules = 'JoinReorder,JoinCommutativity';

-- @skip_result_check=true
-- @result_contains=INNER, eq: [a.k = b.k]
-- @result_contains=other: a.bucket = b.bucket OR a.payload = b.payload
-- @result_contains=INNER, eq: [b.k = c.k]
-- @result_contains=predicates: CAST(c.flag AS Int64) = 1 AND c.k IS NOT NULL
EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM ${case_db}.residual_lcj_a a
JOIN ${case_db}.residual_lcj_b b
  ON a.k = b.k AND (a.bucket = b.bucket OR a.payload = b.payload)
JOIN ${case_db}.residual_lcj_c c
  ON b.k = c.k
WHERE c.flag = 1;

SET disable_optimizer_rules = '';
