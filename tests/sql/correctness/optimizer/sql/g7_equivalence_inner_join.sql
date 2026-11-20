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

-- @tags=optimizer,g7,equivalence_predicate
-- Test Objective:
-- Lock in the current EXPLAIN VERBOSE plan shape for an INNER JOIN whose
-- ON-clause carries a literal equality on one side of the equi-join.
--
-- G7 (InnerJoinEquivalencePredicateRule) inserts a propagated
-- `r.rk = 10` filter alternative into the right child's memo group.
-- Whether that alternative is finally chosen by the cost search depends on
-- two unrelated pre-existing optimizer behaviors:
--   1. DP join reorder drops single-side literal conjuncts from JOIN
--      conditions (see `collect_join_predicates` in
--      `rewrite/rules/join_reorder/reorder.rs`).
--   2. PushDownPredicate only walks INTO `Filter(Join)`, not into a Join's
--      own ON-condition conjuncts.
-- Once either of those is improved, the propagated predicate will surface in
-- this golden and the diff will signal that G7 is now end-to-end visible.
-- Until then, this case serves as a plan-shape regression guard.
DROP TABLE IF EXISTS ${case_db}.g7_l;
DROP TABLE IF EXISTS ${case_db}.g7_r;
CREATE TABLE ${case_db}.g7_l (lk BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.g7_r (rk BIGINT, payload BIGINT);
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.lk, r.rk
FROM ${case_db}.g7_l l
JOIN ${case_db}.g7_r r ON l.lk = r.rk AND l.lk = 10;
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.lk, r.rk
FROM ${case_db}.g7_l l
JOIN ${case_db}.g7_r r ON l.lk = r.rk AND r.rk = 20;
