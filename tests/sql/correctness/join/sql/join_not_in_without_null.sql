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
-- @tags=join,anti,not_in
-- Test Objective:
-- 1. Validate NOT IN semantics when subquery set has no NULLs.
-- 2. Prevent regressions in anti-set filtering for scalar keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic values without NULL on subquery side.
-- 3. Query with NOT IN and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_join_not_in_no_null_l;
DROP TABLE IF EXISTS ${case_db}.t_join_not_in_no_null_r;
CREATE TABLE ${case_db}.t_join_not_in_no_null_l (
  id INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_not_in_no_null_r (
  id INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_not_in_no_null_l VALUES
  ROW(1),
  ROW(2),
  ROW(3),
  ROW(NULL);
INSERT INTO ${case_db}.t_join_not_in_no_null_r VALUES
  ROW(2);
SELECT id
FROM ${case_db}.t_join_not_in_no_null_l
WHERE id NOT IN (
  SELECT id FROM ${case_db}.t_join_not_in_no_null_r
)
ORDER BY id;
