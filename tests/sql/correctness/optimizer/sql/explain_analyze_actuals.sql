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

-- @tags=optimizer,explain_analyze,actuals
-- Test Objective:
-- 1. EXPLAIN ANALYZE executes the distributed plan and renders actual metrics.
DROP TABLE IF EXISTS ${case_db}.explain_analyze_actuals_l;
DROP TABLE IF EXISTS ${case_db}.explain_analyze_actuals_r;
CREATE TABLE ${case_db}.explain_analyze_actuals_l (k INT, v INT);
CREATE TABLE ${case_db}.explain_analyze_actuals_r (k INT, v INT);
INSERT INTO ${case_db}.explain_analyze_actuals_l VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO ${case_db}.explain_analyze_actuals_r VALUES (1, 100), (2, 200), (4, 400);

-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Rows: 1
-- @result_contains=Profile: tasks=
-- @result_contains=HASH JOIN (BROADCAST, INNER
-- @result_contains=act={rows=
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM ${case_db}.explain_analyze_actuals_l l
INNER JOIN ${case_db}.explain_analyze_actuals_r r ON l.k = r.k;
