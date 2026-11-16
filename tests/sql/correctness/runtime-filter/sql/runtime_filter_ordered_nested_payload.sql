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

-- @order_sensitive=true
-- @tags=runtime_filter,ordered_domain,nested_payload
-- Test Objective:
-- 1. A nested scan payload must not install an optional ordered filter.
-- 2. The grouped TopN result must match the same query with RF disabled.
-- COUNT(a) keeps the array in the scan output; the two smallest groups contain
-- two and one non-null arrays respectively. The suite owns table cleanup.
DROP TABLE IF EXISTS ${case_db}.rf_ordered_nested_payload;
CREATE TABLE ${case_db}.rf_ordered_nested_payload (k INT, a ARRAY<INT>)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.rf_ordered_nested_payload VALUES
    (1, [10]), (1, [20]), (2, [30]), (3, [40]);

-- Keep the complete-group ordered-filter candidate instead of preagg pruning.
SET disable_optimizer_rules = 'PushDownTopNToPreAgg';
SET enable_global_runtime_filter = true;
-- @explain_contains=LOCAL TOP-N (limit=2, offset=0)
-- @explain_contains=HASH AGGREGATE (GLOBAL, group by: [k])
-- @explain_contains=columns: k, a
-- @explain_not_contains=domain = OrderedBound
SELECT k, COUNT(a) AS array_count
FROM ${case_db}.rf_ordered_nested_payload
GROUP BY k ORDER BY k LIMIT 2;

SET enable_global_runtime_filter = false;
SELECT k, COUNT(a) AS array_count
FROM ${case_db}.rf_ordered_nested_payload
GROUP BY k ORDER BY k LIMIT 2;

SET enable_global_runtime_filter = true;
SET disable_optimizer_rules = '';
DROP TABLE ${case_db}.rf_ordered_nested_payload;
