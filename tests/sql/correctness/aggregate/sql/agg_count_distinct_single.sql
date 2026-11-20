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
-- @tags=aggregate,count_distinct
-- Test Objective:
-- 1. Validate COUNT(DISTINCT) on grouped string keys.
-- 2. Prevent regressions in distinct-state aggregation across groups.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert duplicate and NULL-contained rows.
-- 3. Compute grouped COUNT(DISTINCT) and assert deterministic order.
CREATE TABLE ${case_db}.t_agg_count_distinct_single (
    g INT,
    s VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_count_distinct_single VALUES
    ROW(1, 'a'),
    ROW(1, 'a'),
    ROW(1, 'b'),
    ROW(2, 'a'),
    ROW(2, NULL),
    ROW(2, 'c');

SELECT
    g,
    COUNT(DISTINCT CAST(s AS VARCHAR)) AS cd_s
FROM ${case_db}.t_agg_count_distinct_single
GROUP BY g
ORDER BY g;
