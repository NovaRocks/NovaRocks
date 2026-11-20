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
-- @tags=aggregate,empty_set
-- Test Objective:
-- 1. Validate EMPTY_SET_NODE lowering for global aggregate queries.
-- 2. Prevent regressions where EMPTY_SET_NODE fails at plan lowering.
-- Test Flow:
-- 1. Build a deterministic inline relation with one non-null and one null row.
-- 2. Force an empty input with a constant-false predicate.
-- 3. Assert aggregate null/count semantics on zero rows.
SELECT
    COUNT() AS c_all,
    COUNT(v) AS c_not_null,
    SUM(v) AS s_v,
    AVG(v) AS avg_v,
    MIN(v) AS min_v,
    MAX(v) AS max_v
FROM (
    SELECT 1 AS v
    UNION ALL
    SELECT NULL AS v
) t
WHERE 1 = 0;
