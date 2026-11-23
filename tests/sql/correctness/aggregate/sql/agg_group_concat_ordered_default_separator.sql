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
-- @tags=aggregate,group_concat
-- Test Objective:
-- 1. Validate ordered group_concat with default separator under two-phase finalize execution.
-- 2. Prevent regressions where merge-stage input typing breaks intermediate ARRAY decoding.
-- Test Flow:
-- 1. Build a deterministic inline input with unordered integer values.
-- 2. Run ordered group_concat without explicit separator.
-- 3. Assert deterministic ordered output.
WITH t AS (
    SELECT 3 AS c1
    UNION ALL
    SELECT 1
    UNION ALL
    SELECT 2
)
SELECT LISTAGG(CAST(c1 AS VARCHAR), ',') WITHIN GROUP (ORDER BY c1) AS gc
FROM t;
