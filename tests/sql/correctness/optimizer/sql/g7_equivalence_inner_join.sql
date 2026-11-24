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
-- An equality on either join key must reach the scans of both inputs.
-- The selected distribution may vary with statistics and broadcast costs.
DROP TABLE IF EXISTS ${case_db}.g7_l;
DROP TABLE IF EXISTS ${case_db}.g7_r;
CREATE TABLE ${case_db}.g7_l (lk BIGINT, payload BIGINT);
CREATE TABLE ${case_db}.g7_r (rk BIGINT, payload BIGINT);
-- @result_contains=HASH JOIN (
-- @result_contains=INNER, eq: [l.lk = r.rk]
-- @result_contains=predicates: l.lk = 10
-- @result_contains=predicates: r.rk = 10
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.lk, r.rk
FROM ${case_db}.g7_l l
JOIN ${case_db}.g7_r r ON l.lk = r.rk AND l.lk = 10;
-- @result_contains=HASH JOIN (
-- @result_contains=INNER, eq: [l.lk = r.rk]
-- @result_contains=predicates: l.lk = 20
-- @result_contains=predicates: r.rk = 20
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.lk, r.rk
FROM ${case_db}.g7_l l
JOIN ${case_db}.g7_r r ON l.lk = r.rk AND r.rk = 20;
