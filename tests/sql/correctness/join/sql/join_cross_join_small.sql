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
-- @tags=join,cross,nestloop
-- Test Objective:
-- 1. Validate CROSS JOIN cartesian semantics.
-- 2. Prevent regressions in nested-loop style cross product generation.
-- Test Flow:
-- 1. Create/reset two tiny tables.
-- 2. Insert deterministic rows on each side.
-- 3. Execute CROSS JOIN and assert ordered cartesian output.
DROP TABLE IF EXISTS ${case_db}.t_join_cross_a;
DROP TABLE IF EXISTS ${case_db}.t_join_cross_b;
CREATE TABLE ${case_db}.t_join_cross_a (
  id INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_cross_b (
  c VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_cross_a VALUES
  ROW(1),
  ROW(2);
INSERT INTO ${case_db}.t_join_cross_b VALUES
  ROW('x'),
  ROW('y');
SELECT a.id, b.c
FROM ${case_db}.t_join_cross_a a
CROSS JOIN ${case_db}.t_join_cross_b b
ORDER BY a.id, b.c;
