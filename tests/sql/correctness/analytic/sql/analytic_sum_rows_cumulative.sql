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
-- @tags=analytic,sum,rows_frame
-- Test Objective:
-- 1. Validate cumulative SUM over ROWS frame.
-- 2. Prevent regressions in running-aggregate window updates.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows per partition.
-- 3. Compute cumulative SUM and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_sum_rows_cumulative (
    grp VARCHAR(10),
    ts INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_sum_rows_cumulative VALUES
    ROW('A', 1, 2),
    ROW('A', 2, 3),
    ROW('A', 3, 5),
    ROW('B', 1, 4),
    ROW('B', 2, 6);

SELECT
    grp,
    ts,
    v,
    SUM(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_sum
FROM ${case_db}.t_analytic_sum_rows_cumulative
ORDER BY grp, ts;
