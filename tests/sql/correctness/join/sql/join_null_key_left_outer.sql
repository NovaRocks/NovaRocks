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
-- @tags=join,left_outer,null
-- Test Objective:
-- 1. Validate NULL-key behavior under LEFT OUTER JOIN.
-- 2. Prevent regressions where NULL-key left rows incorrectly match right NULL keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows including NULL and non-NULL keys.
-- 3. Execute LEFT OUTER JOIN and assert null-fill behavior.
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_left_outer_l;
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_left_outer_r;
CREATE TABLE ${case_db}.t_join_null_key_left_outer_l (
  k INT,
  vl VARCHAR
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_null_key_left_outer_r (
  k INT,
  vr VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_null_key_left_outer_l VALUES
  ROW(NULL, 'LN'),
  ROW(2, 'L2');
INSERT INTO ${case_db}.t_join_null_key_left_outer_r VALUES
  ROW(NULL, 'RN'),
  ROW(2, 'R2');
SELECT
  COALESCE(CAST(l.k AS VARCHAR), 'NULL') AS lk,
  l.vl,
  r.vr
FROM ${case_db}.t_join_null_key_left_outer_l l
LEFT OUTER JOIN ${case_db}.t_join_null_key_left_outer_r r
  ON l.k = r.k
ORDER BY (l.k IS NULL) ASC, l.k ASC;
