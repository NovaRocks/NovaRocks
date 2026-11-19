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
-- @tags=analytic,session_number
-- Test Objective:
-- 1. Validate session_number gap-splitting semantics under ordered partitions.
-- 2. Prevent regressions where session boundaries are miscomputed across large gaps.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic timestamp-like integer sequences by group.
-- 3. Compute session_number with fixed gap threshold and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_session_number_gap (
    grp VARCHAR(10),
    ts INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_session_number_gap VALUES
    ROW('A', 1),
    ROW('A', 3),
    ROW('A', 10),
    ROW('A', 11),
    ROW('A', 20),
    ROW('B', 5),
    ROW('B', 7),
    ROW('B', 30);

SELECT
    grp,
    ts,
    session_number(ts, 2) OVER (PARTITION BY grp ORDER BY ts) AS sess_id
FROM ${case_db}.t_analytic_session_number_gap
ORDER BY grp, ts;
