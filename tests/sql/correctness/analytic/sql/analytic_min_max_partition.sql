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
-- @tags=analytic,min,max
-- Test Objective:
-- 1. Validate MIN/MAX as partition windows.
-- 2. Prevent regressions in partition-scoped extrema propagation.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic partitioned rows.
-- 3. Compute partition MIN/MAX and assert ordered output.
CREATE TABLE ${case_db}.t_analytic_min_max_partition (
    grp VARCHAR(10),
    id INT,
    v INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_analytic_min_max_partition VALUES
    ROW('A', 1, 9),
    ROW('A', 2, 3),
    ROW('A', 3, 7),
    ROW('B', 4, NULL),
    ROW('B', 5, 8);

SELECT
    grp,
    id,
    v,
    MIN(v) OVER (PARTITION BY grp) AS min_v,
    MAX(v) OVER (PARTITION BY grp) AS max_v
FROM ${case_db}.t_analytic_min_max_partition
ORDER BY grp, id;
