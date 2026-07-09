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

-- @tags=optimizer,oq9,residual,semi_anti_guard
-- Test Objective:
-- SEMI/ANTI join conditions stay guarded from the inner/cross-only
-- predicate move-around derivation path.
DROP TABLE IF EXISTS ${case_db}.residual_sa_l;
DROP TABLE IF EXISTS ${case_db}.residual_sa_r;
CREATE TABLE ${case_db}.residual_sa_l (k INT, flag INT, payload INT);
CREATE TABLE ${case_db}.residual_sa_r (k INT, flag INT, payload INT);

-- @skip_result_check=true
-- @result_contains=LEFT SEMI, eq: [l.k = r.k, l.flag = r.flag]
-- @result_contains=other: CAST(l.flag AS Int64) = 1
-- @result_not_contains=predicates: CAST(l.flag AS Int64) = 1
EXPLAIN VERBOSE
SELECT l.k
FROM ${case_db}.residual_sa_l l
LEFT SEMI JOIN ${case_db}.residual_sa_r r
  ON l.k = r.k AND l.flag = r.flag AND l.flag = 1;

-- @skip_result_check=true
-- @result_contains=LEFT ANTI, eq: [l.k = r.k, l.flag = r.flag]
-- @result_contains=other: CAST(l.flag AS Int64) = 1
-- @result_not_contains=predicates: CAST(l.flag AS Int64) = 1
EXPLAIN VERBOSE
SELECT l.k
FROM ${case_db}.residual_sa_l l
LEFT ANTI JOIN ${case_db}.residual_sa_r r
  ON l.k = r.k AND l.flag = r.flag AND l.flag = 1;
