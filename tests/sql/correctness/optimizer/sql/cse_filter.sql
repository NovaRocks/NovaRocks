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

-- @tags=optimizer,cse
-- Test Objective:
-- Repeated filter predicate scalar subexpressions are materialized once below
-- the Filter as an internal CSE Project item while visible results stay stable.
DROP TABLE IF EXISTS ${case_db}.cse_filter_t;
CREATE TABLE ${case_db}.cse_filter_t (a BIGINT, b BIGINT);
INSERT INTO ${case_db}.cse_filter_t VALUES (1, 2), (3, 4), (8, 9), (12, 13);

SET disable_optimizer_rules = 'PushDownPredicateScan';

SELECT a, b
FROM ${case_db}.cse_filter_t
WHERE a + b > 6 AND a + b < 20
ORDER BY a;

SELECT *
FROM ${case_db}.cse_filter_t
WHERE a + b > 6 AND a + b < 20
ORDER BY a;

-- @explain_contains=__cse_0
-- @result_not_contains=__cse_
SELECT a, b
FROM ${case_db}.cse_filter_t
WHERE a + b > 6 AND a + b < 20
ORDER BY a;

SET disable_optimizer_rules = '';
