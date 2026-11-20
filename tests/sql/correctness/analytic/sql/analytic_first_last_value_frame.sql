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
-- @tags=analytic,first_value,last_value
-- Test Objective:
-- 1. Validate FIRST_VALUE/LAST_VALUE under full window frame.
-- 2. Prevent regressions where LAST_VALUE incorrectly uses current-row frame.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic ordered rows.
-- 3. Compute FIRST_VALUE/LAST_VALUE with full frame and assert output.
CREATE TABLE ${case_db}.t_analytic_first_last_value_frame (
    grp VARCHAR(10),
    ts INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_first_last_value_frame VALUES
    ROW('A', 1, 5),
    ROW('A', 2, 6),
    ROW('A', 3, 7),
    ROW('B', 1, NULL),
    ROW('B', 2, 9);

SELECT
    grp,
    ts,
    v,
    FIRST_VALUE(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_v,
    LAST_VALUE(v) OVER (
        PARTITION BY grp ORDER BY ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_v
FROM ${case_db}.t_analytic_first_last_value_frame
ORDER BY grp, ts;
