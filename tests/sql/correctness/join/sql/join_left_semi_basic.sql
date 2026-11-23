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
-- @tags=join,left_semi
-- Test Objective:
-- 1. Validate LEFT SEMI JOIN existence semantics.
-- 2. Prevent regressions where semi-join emits non-matching left rows.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic rows including duplicate right keys.
-- 3. Execute LEFT SEMI JOIN and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_left_semi_l;
DROP TABLE IF EXISTS ${case_db}.t_join_left_semi_r;
CREATE TABLE ${case_db}.t_join_left_semi_l (
  id INT,
  lv VARCHAR
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_left_semi_r (
  id INT,
  rv VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_left_semi_l VALUES
  ROW(1, 'L1'),
  ROW(2, 'L2'),
  ROW(3, 'L3'),
  ROW(4, 'L4');
INSERT INTO ${case_db}.t_join_left_semi_r VALUES
  ROW(2, 'R2a'),
  ROW(2, 'R2b'),
  ROW(4, 'R4');
SELECT l.id, l.lv
FROM ${case_db}.t_join_left_semi_l l
WHERE EXISTS (SELECT 1 FROM ${case_db}.t_join_left_semi_r r WHERE l.id = r.id)
ORDER BY l.id;
