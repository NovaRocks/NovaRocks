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

-- @tags=optimizer,predicate_pushdown,session_rule_disable,dist-only
-- Test Objective:
-- PushDownPredicateScan is the only owner of Filter-to-Scan pushdown. The
-- distributed builder must preserve a Filter when the rule is disabled.
DROP TABLE IF EXISTS ${case_db}.predicate_pushdown_scan_disabled_dist_t;
CREATE TABLE ${case_db}.predicate_pushdown_scan_disabled_dist_t (k INT, payload INT);
INSERT INTO ${case_db}.predicate_pushdown_scan_disabled_dist_t VALUES
    ROW(5, 50),
    ROW(20, 200),
    ROW(30, 300);

SET SESSION disable_optimizer_rules = '';

-- @skip_result_check=true
-- @result_not_contains=:FILTER
-- @result_contains=predicates:
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT payload
FROM ${case_db}.predicate_pushdown_scan_disabled_dist_t
WHERE k > 10;

SET SESSION disable_optimizer_rules = 'PushDownPredicateScan';

-- @skip_result_check=true
-- @result_contains=:FILTER
-- @result_contains=predicate:
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT payload
FROM ${case_db}.predicate_pushdown_scan_disabled_dist_t
WHERE k > 10;

SELECT payload
FROM ${case_db}.predicate_pushdown_scan_disabled_dist_t
WHERE k > 10
ORDER BY payload;

SET SESSION disable_optimizer_rules = '';
