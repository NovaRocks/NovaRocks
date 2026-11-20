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

-- @tags=optimizer,baseline
-- Test Objective:
-- 1. Lock in the current EXPLAIN VERBOSE shape of a plain inner-equi-join.
-- 2. Failure of this case in a future PR signals a plan-shape change
--    that must be intentional and acknowledged via record mode.
DROP TABLE IF EXISTS ${case_db}.t_optimizer_baseline_a;
DROP TABLE IF EXISTS ${case_db}.t_optimizer_baseline_b;
CREATE TABLE ${case_db}.t_optimizer_baseline_a (k INT, v INT);
CREATE TABLE ${case_db}.t_optimizer_baseline_b (k INT, w INT);
INSERT INTO ${case_db}.t_optimizer_baseline_a VALUES ROW(1, 10), ROW(2, 20);
INSERT INTO ${case_db}.t_optimizer_baseline_b VALUES ROW(1, 100), ROW(2, 200);
ANALYZE ${case_db}.t_optimizer_baseline_a;
ANALYZE ${case_db}.t_optimizer_baseline_b;
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT a.k, a.v, b.w
FROM ${case_db}.t_optimizer_baseline_a a
INNER JOIN ${case_db}.t_optimizer_baseline_b b ON a.k = b.k;
