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
-- 1. Validate DISTINCT + ORDER BY group_concat behavior per group.
-- 2. Prevent regressions in group_concat merge semantics for distributed aggregation.
-- Test Flow:
-- 1. Create/reset grouped source table.
-- 2. Insert deterministic rows including duplicates and NULL output values.
-- 3. Group by key and assert ordered group_concat outputs.
CREATE TABLE ${case_db}.t_agg_group_concat_distinct_grouped (
    k INT,
    s VARCHAR
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_group_concat_distinct_grouped VALUES
    ROW(1, 'b'),
    ROW(2, 'a'),
    ROW(3, 'b'),
    ROW(4, NULL),
    ROW(5, 'c');

SELECT
    MOD(k, 2) AS g,
    LISTAGG(DISTINCT CAST(s AS VARCHAR), ',') WITHIN GROUP (ORDER BY CAST(s AS VARCHAR)) AS gc
FROM ${case_db}.t_agg_group_concat_distinct_grouped
GROUP BY g
ORDER BY g;
