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
-- @tags=join,right_semi
-- Test Objective:
-- 1. Validate RIGHT SEMI JOIN existence semantics on right-side output rows.
-- 2. Prevent regressions where unmatched right rows leak into output.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping and non-overlapping keys.
-- 3. Execute RIGHT SEMI JOIN and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_right_semi_l;
DROP TABLE IF EXISTS ${case_db}.t_join_right_semi_r;
CREATE TABLE ${case_db}.t_join_right_semi_l (
  id INT,
  lv STRING
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_right_semi_r (
  id INT,
  rv STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_right_semi_l VALUES
  ROW(2, 'L2'),
  ROW(4, 'L4');
INSERT INTO ${case_db}.t_join_right_semi_r VALUES
  ROW(1, 'R1'),
  ROW(2, 'R2'),
  ROW(3, 'R3'),
  ROW(4, 'R4');
SELECT r.id, r.rv
FROM ${case_db}.t_join_right_semi_r r
WHERE EXISTS (SELECT 1 FROM ${case_db}.t_join_right_semi_l l WHERE l.id = r.id)
ORDER BY r.id;
