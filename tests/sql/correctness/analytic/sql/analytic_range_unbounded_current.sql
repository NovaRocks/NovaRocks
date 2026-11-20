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
-- @tags=analytic,range_frame
-- Test Objective:
-- 1. Validate RANGE UNBOUNDED PRECEDING TO CURRENT ROW semantics.
-- 2. Prevent regressions in RANGE frame ordering behavior.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate order keys.
-- 3. Compute RANGE-frame SUM and assert output.
CREATE TABLE ${case_db}.t_analytic_range_unbounded_current (
    grp VARCHAR(10),
    ord_key INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_range_unbounded_current VALUES
    ROW('A', 1, 10),
    ROW('A', 2, 20),
    ROW('A', 2, 30),
    ROW('A', 3, 40);

SELECT
    grp,
    ord_key,
    v,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ord_key
        RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS range_sum
FROM ${case_db}.t_analytic_range_unbounded_current
ORDER BY grp, ord_key, v;
