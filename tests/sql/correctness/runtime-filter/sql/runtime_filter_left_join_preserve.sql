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
-- @tags=runtime_filter,left_join
-- Test Objective:
-- 1. Validate LEFT JOIN unmatched-row preservation with runtime filter enabled.
-- 2. Prevent regressions where probe-side rows are incorrectly dropped.
-- Test Flow:
-- 1. Create/reset left/right tables.
-- 2. Insert partially overlapping keys.
-- 3. Execute LEFT JOIN and assert ordered NULL-fill output.
DROP TABLE IF EXISTS ${case_db}.t_rf_left_join_preserve_l;
DROP TABLE IF EXISTS ${case_db}.t_rf_left_join_preserve_r;
CREATE TABLE ${case_db}.t_rf_left_join_preserve_l (
    id INT,
    k INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_rf_left_join_preserve_r (
    k INT,
    tag VARCHAR(20)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_rf_left_join_preserve_l VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, 30);

INSERT INTO ${case_db}.t_rf_left_join_preserve_r VALUES
    ROW(20, 'r20'),
    ROW(30, 'r30');

SELECT l.id, l.k, r.tag
FROM ${case_db}.t_rf_left_join_preserve_l l
LEFT JOIN ${case_db}.t_rf_left_join_preserve_r r
  ON l.k = r.k
ORDER BY l.id;
