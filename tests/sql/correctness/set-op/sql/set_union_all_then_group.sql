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
-- @tags=set_op,union_all,group
-- Test Objective:
-- 1. Validate downstream grouping over UNION ALL output.
-- 2. Prevent regressions where upstream multiplicity is lost before GROUP BY.
-- Test Flow:
-- 1. Build UNION ALL branch output with duplicates.
-- 2. Aggregate counts by value.
-- 3. Assert ordered grouped result.
SELECT
    x,
    COUNT() AS c
FROM (
    SELECT CAST(1 AS BIGINT) AS x
    UNION ALL
    SELECT CAST(1 AS BIGINT)
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(2 AS BIGINT)
    UNION ALL
    SELECT CAST(2 AS BIGINT)
) t
GROUP BY x
ORDER BY x;
