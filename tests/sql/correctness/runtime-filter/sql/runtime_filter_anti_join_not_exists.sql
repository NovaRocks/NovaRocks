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
-- @tags=runtime_filter,anti_join,not_exists
-- Test Objective:
-- 1. Validate NOT EXISTS anti-join semantics.
-- 2. Prevent regressions where anti-join rows are incorrectly filtered out.
-- Test Flow:
-- 1. Create/reset source tables.
-- 2. Insert deterministic overlapping keys.
-- 3. Filter left rows via NOT EXISTS and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_rf_anti_join_not_exists_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_anti_join_not_exists_r;
CREATE TABLE ${case_db}.t_rf_anti_join_not_exists_l (
    id INT,
    k INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_anti_join_not_exists_r (
    k INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_anti_join_not_exists_l VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, 30),
    ROW(4, NULL);

INSERT INTO ${case_db}.t_rf_anti_join_not_exists_r VALUES
    ROW(20),
    ROW(40);

SELECT l.id, l.k
FROM ${case_db}.t_rf_anti_join_not_exists_l l
WHERE NOT EXISTS (
    SELECT 1
    FROM ${case_db}.t_rf_anti_join_not_exists_r r
    WHERE r.k = l.k
)
ORDER BY l.id;
