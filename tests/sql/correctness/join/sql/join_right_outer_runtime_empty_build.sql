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
-- @tags=join,right_outer,residual
-- Test Objective:
-- 1. Validate RIGHT OUTER JOIN keeps only right-side rows when right side is preserved.
-- 2. Prevent regression where probe-side rows leak when build side becomes empty at runtime.
-- Test Flow:
-- 1. Create/reset left and right tables with deterministic data.
-- 2. Build a right-side subquery filtered by a non-matching predicate to produce runtime-empty build input.
-- 3. Execute RIGHT OUTER JOIN and assert empty output.
DROP TABLE IF EXISTS ${case_db}.t_join_right_outer_runtime_empty_build_l;
DROP TABLE IF EXISTS ${case_db}.t_join_right_outer_runtime_empty_build_r;
CREATE TABLE ${case_db}.t_join_right_outer_runtime_empty_build_l (
  id INT,
  lv VARCHAR
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_right_outer_runtime_empty_build_r (
  id INT,
  rv VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_right_outer_runtime_empty_build_l VALUES
  ROW(1, 'L1'),
  ROW(2, 'L2');
INSERT INTO ${case_db}.t_join_right_outer_runtime_empty_build_r VALUES
  ROW(1, 'R1'),
  ROW(2, 'R2');
SELECT l.id AS lid, l.lv, r.id AS rid, r.rv
FROM ${case_db}.t_join_right_outer_runtime_empty_build_l l
RIGHT OUTER JOIN (
  SELECT id, rv
  FROM ${case_db}.t_join_right_outer_runtime_empty_build_r
  WHERE rv = 'NO_MATCH'
) r
  ON l.id = r.id
ORDER BY rid;
