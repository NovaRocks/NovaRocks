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

-- @tags=optimizer,p8,distributed_ir_explain
EXPLAIN VERBOSE
SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;

-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Rows: 1
-- @result_contains=Profile: tasks=
-- @result_contains=HASH AGGREGATE
-- @result_contains=UNION ALL
EXPLAIN ANALYZE SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;

-- @explain_contains=PLAN FRAGMENT
-- @explain_contains=EXCHANGE ID:
-- @explain_not_contains=Boundary Schemas:
SELECT k, SUM(v) AS total_v
FROM (
    SELECT 1 AS k, 10 AS v
    UNION ALL
    SELECT 1 AS k, 20 AS v
) t
GROUP BY k;
