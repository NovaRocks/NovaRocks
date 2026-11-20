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
-- @tags=aggregate,bool_or
-- Test Objective:
-- 1. Validate BOOL_OR aggregation over grouped predicates.
-- 2. Prevent regressions in boolean aggregation with NULL rows.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows including NULL.
-- 3. Aggregate predicate truth values by group and assert output.
CREATE TABLE ${case_db}.t_agg_bool_or_grouped (
    g INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_bool_or_grouped VALUES
    ROW(1, 10),
    ROW(1, 20),
    ROW(2, NULL),
    ROW(2, 5),
    ROW(3, NULL);

SELECT
    g,
    BOOL_OR(v > 15) AS has_gt_15,
    BOOL_OR(v IS NULL) AS has_null
FROM ${case_db}.t_agg_bool_or_grouped
GROUP BY g
ORDER BY g;
