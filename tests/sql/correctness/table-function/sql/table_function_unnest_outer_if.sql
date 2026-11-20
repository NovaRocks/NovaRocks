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
-- @tags=table_function,unnest
-- Test Objective:
-- 1. Validate UNNEST with outer-column-dependent array expressions.
-- 2. Prevent regressions in correlated lateral table-function execution.
-- Test Flow:
-- 1. Build deterministic outer rows.
-- 2. Derive per-row arrays with IF expression.
-- 3. Expand with UNNEST and assert ordered output.
SELECT b.id, x
FROM (
    SELECT CAST(1 AS BIGINT) AS id
    UNION ALL
    SELECT CAST(2 AS BIGINT)
) b
CROSS JOIN  UNNEST(IF(b.id = 1, [10, 20], [30])) AS u(x)
ORDER BY b.id, x;
