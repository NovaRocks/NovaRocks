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

-- @tags=optimizer,g7,equivalence_predicate,negative
-- Test Objective:
-- G7 (InnerJoinEquivalencePredicateRule) must NEVER fire for non-INNER joins.
-- For a LEFT OUTER JOIN, propagating `l.lk = 10` to the right (nullable) side
-- as `r.rk = 10` would silently drop the null-extended rows that
-- LEFT JOIN preserves.
-- This golden locks in the plan for the LEFT OUTER form so any future change
-- that accidentally relaxes the join-kind guard will produce a diff.
DROP TABLE IF EXISTS ${case_db}.g7_outer_l;
DROP TABLE IF EXISTS ${case_db}.g7_outer_r;
CREATE TABLE ${case_db}.g7_outer_l (lk BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.g7_outer_r (rk BIGINT, payload BIGINT);
-- @skip_result_check=true
-- @result_contains=LEFT OUTER, eq: [l.lk = r.rk]
-- @result_contains=other: l.lk = 10
-- @result_not_contains=r.rk = 10
-- @result_not_contains=predicates: l.lk = 10
EXPLAIN VERBOSE
SELECT l.lk, r.rk
FROM ${case_db}.g7_outer_l l
LEFT JOIN ${case_db}.g7_outer_r r ON l.lk = r.rk AND l.lk = 10;
