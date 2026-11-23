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
-- @tags=join,left_outer
-- Test Objective:
-- 1. Validate LEFT OUTER JOIN null-fill semantics for unmatched right rows.
-- 2. Prevent regressions where unmatched left rows are dropped.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows with one unmatched left key.
-- 3. Execute LEFT OUTER JOIN and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_left_outer_l;
DROP TABLE IF EXISTS ${case_db}.t_join_left_outer_r;
CREATE TABLE ${case_db}.t_join_left_outer_l (
  id INT,
  lv VARCHAR
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_left_outer_r (
  id INT,
  rv VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_left_outer_l VALUES
  ROW(1, 'L1'),
  ROW(2, 'L2'),
  ROW(3, 'L3');
INSERT INTO ${case_db}.t_join_left_outer_r VALUES
  ROW(2, 'R2'),
  ROW(3, 'R3');
SELECT l.id, l.lv, r.rv
FROM ${case_db}.t_join_left_outer_l l
LEFT OUTER JOIN ${case_db}.t_join_left_outer_r r
  ON l.id = r.id
ORDER BY l.id;
