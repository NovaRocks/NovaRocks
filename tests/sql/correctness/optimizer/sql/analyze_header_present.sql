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

-- @tags=optimizer,explain_analyze
-- Test Objective:
-- 1. EXPLAIN ANALYZE executes the distributed plan and renders the summary header.
DROP TABLE IF EXISTS ${case_db}.t_analyze_header;
CREATE TABLE ${case_db}.t_analyze_header (k INT);
INSERT INTO ${case_db}.t_analyze_header VALUES (1), (2), (3);
ANALYZE TABLE ${case_db}.t_analyze_header;

-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Rows: 1
-- @result_contains=Profile: tasks=
-- @result_contains=PLAN FRAGMENT 0
-- @result_contains=HASH AGGREGATE
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ${case_db}.t_analyze_header;
