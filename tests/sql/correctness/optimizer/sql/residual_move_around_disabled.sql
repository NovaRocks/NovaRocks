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

-- @tags=optimizer,oq9,residual,session_rule_disable
-- Test Objective:
-- SET disable_optimizer_rules='JoinPredicateMoveAround' suppresses derived
-- predicates that move from an already-filtered join child to a parent join sibling.
-- Keep the input predicate inside the original child. Disable the independent
-- PushDownPredicateJoin derivation in both controls to isolate MoveAround.
DROP TABLE IF EXISTS ${case_db}.residual_mad_a;
DROP TABLE IF EXISTS ${case_db}.residual_mad_b;
DROP TABLE IF EXISTS ${case_db}.residual_mad_c;
CREATE TABLE ${case_db}.residual_mad_a (k INT, payload INT);
CREATE TABLE ${case_db}.residual_mad_b (k INT, payload INT);
CREATE TABLE ${case_db}.residual_mad_c (k INT, payload INT);

SET disable_optimizer_rules = 'PushDownPredicateJoin,JoinReorder,JoinCommutativity';

-- @skip_result_check=true
-- @result_contains=INNER, eq: [k = b.k]
-- @result_contains=INNER, eq: [b.k = c.k]
-- @result_contains=predicates: CAST(k AS Int64) = 7 AND k IS NOT NULL
-- Positive control: a.k=b.k=c.k and a.k=7 imply both derived predicates
-- while JoinPredicateMoveAround is enabled. The disabled control below
-- continues to require their absence.
-- @result_contains=CAST(b.k AS Int64) = 7
-- @result_contains=CAST(c.k AS Int64) = 7
EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM (SELECT k, payload FROM ${case_db}.residual_mad_a WHERE k = 7) a
JOIN ${case_db}.residual_mad_b b ON a.k = b.k
JOIN ${case_db}.residual_mad_c c ON b.k = c.k;

SET disable_optimizer_rules = 'JoinPredicateMoveAround,PushDownPredicateJoin,JoinReorder,JoinCommutativity';

-- @skip_result_check=true
-- @result_contains=INNER, eq: [k = b.k]
-- @result_contains=INNER, eq: [b.k = c.k]
-- @result_contains=predicates: CAST(k AS Int64) = 7 AND k IS NOT NULL
-- @result_not_contains=CAST(b.k AS Int64) = 7
-- @result_not_contains=CAST(c.k AS Int64) = 7
EXPLAIN VERBOSE
SELECT a.payload, b.payload, c.payload
FROM (SELECT k, payload FROM ${case_db}.residual_mad_a WHERE k = 7) a
JOIN ${case_db}.residual_mad_b b ON a.k = b.k
JOIN ${case_db}.residual_mad_c c ON b.k = c.k;

SET disable_optimizer_rules = '';
