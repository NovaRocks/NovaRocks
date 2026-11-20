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

-- @tags=optimizer,explain,logical
-- Test Objective:
-- 1. Preserve the StarRocks-style EXPLAIN LOGICAL interface.
-- 2. EXPLAIN LOGICAL renders the non-distributed logical plan, not the
--    DistributedPlan fragment form used by ordinary EXPLAIN.
DROP TABLE IF EXISTS ${case_db}.explain_logical_l;
DROP TABLE IF EXISTS ${case_db}.explain_logical_r;
CREATE TABLE ${case_db}.explain_logical_l (k INT, v INT);
CREATE TABLE ${case_db}.explain_logical_r (k INT, v INT);
INSERT INTO ${case_db}.explain_logical_l VALUES ROW(1, 10), ROW(2, 20);
INSERT INTO ${case_db}.explain_logical_r VALUES ROW(1, 100), ROW(3, 300);

-- @skip_result_check=true
-- @result_contains=PROJECT [
-- @result_contains=INNER JOIN
-- @result_contains=on: l.k = r.k
-- @result_contains=0:SCAN
-- @result_not_contains=PLAN FRAGMENT
-- @result_not_contains=HASH JOIN
EXPLAIN WITH SESSION explain_detail = 'LOGICAL'
SELECT l.k, r.v
FROM ${case_db}.explain_logical_l l
INNER JOIN ${case_db}.explain_logical_r r ON l.k = r.k;
