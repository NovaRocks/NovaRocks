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
-- @tags=join,left_outer,residual,null
-- Test Objective:
-- 1. Validate LEFT OUTER JOIN semantics when residual predicate evaluates to NULL.
-- 2. Prevent regressions where residual-NULL candidates are treated as matched rows.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows that produce TRUE/FALSE/NULL outcomes for residual predicates.
-- 3. Execute LEFT OUTER JOIN with residual predicate and assert unmatched null-fill output.
DROP TABLE IF EXISTS ${case_db}.t_join_left_outer_residual_nullable_l;
DROP TABLE IF EXISTS ${case_db}.t_join_left_outer_residual_nullable_r;
CREATE TABLE ${case_db}.t_join_left_outer_residual_nullable_l (
  id INT,
  lv INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_left_outer_residual_nullable_r (
  id INT,
  rv INT,
  flag STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_left_outer_residual_nullable_l VALUES
  ROW(1, 5),
  ROW(2, 8),
  ROW(3, 10);
INSERT INTO ${case_db}.t_join_left_outer_residual_nullable_r VALUES
  ROW(1, 7, 'Y'),
  ROW(1, 9, NULL),
  ROW(2, 6, NULL),
  ROW(4, 1, 'Y');
SELECT l.id, l.lv, r.rv, r.flag
FROM ${case_db}.t_join_left_outer_residual_nullable_l l
LEFT OUTER JOIN ${case_db}.t_join_left_outer_residual_nullable_r r
  ON l.id = r.id AND l.lv < r.rv AND r.flag = 'Y'
ORDER BY l.id, r.rv;
