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

-- OQ-4: disabling SplitAggregateRule keeps ordinary aggregate lowering single-phase.

CREATE TABLE ${case_db}.t_split_agg_disabled (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_disabled VALUES
    ROW(1, 10), ROW(1, 20), ROW(2, 30), ROW(2, 40), ROW(3, 50), ROW(3, 60);
ANALYZE ${case_db}.t_split_agg_disabled;

SET SESSION disable_optimizer_rules = 'SplitAggregateRule';

-- @result_not_contains=HASH AGGREGATE (LOCAL
-- @result_not_contains=HASH AGGREGATE (GLOBAL
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT k, SUM(v) AS s
FROM ${case_db}.t_split_agg_disabled
GROUP BY k
ORDER BY k;

SET SESSION disable_optimizer_rules = '';
