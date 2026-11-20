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
-- @tags=join,semi,subquery
-- Test Objective:
-- 1. Validate EXISTS subquery semantics equivalent to semi-join behavior.
-- 2. Prevent regressions in correlated subquery key matching.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic overlapping keys.
-- 3. Query with EXISTS and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_exists_l;
DROP TABLE IF EXISTS ${case_db}.t_join_exists_r;
CREATE TABLE ${case_db}.t_join_exists_l (
  id INT,
  v STRING
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_exists_r (
  id INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_exists_l VALUES
  ROW(1, 'a'),
  ROW(2, 'b'),
  ROW(3, 'c'),
  ROW(4, 'd');
INSERT INTO ${case_db}.t_join_exists_r VALUES
  ROW(2),
  ROW(4);
SELECT l.id, l.v
FROM ${case_db}.t_join_exists_l l
WHERE EXISTS (
  SELECT 1
  FROM ${case_db}.t_join_exists_r r
  WHERE r.id = l.id
)
ORDER BY l.id;
