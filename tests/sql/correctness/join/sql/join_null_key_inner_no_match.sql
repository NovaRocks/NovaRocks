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
-- @tags=join,inner,null
-- Test Objective:
-- 1. Validate that NULL join keys do not match under equality INNER JOIN.
-- 2. Prevent regressions where NULL=NULL is treated as a match in join keys.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert rows including NULL and non-NULL keys.
-- 3. Execute INNER JOIN and assert only non-NULL matched rows remain.
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_inner_l;
DROP TABLE IF EXISTS ${case_db}.t_join_null_key_inner_r;
CREATE TABLE ${case_db}.t_join_null_key_inner_l (
  k INT,
  vl STRING
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_null_key_inner_r (
  k INT,
  vr STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_null_key_inner_l VALUES
  ROW(NULL, 'LN'),
  ROW(2, 'L2');
INSERT INTO ${case_db}.t_join_null_key_inner_r VALUES
  ROW(NULL, 'RN'),
  ROW(2, 'R2');
SELECT l.k, l.vl, r.vr
FROM ${case_db}.t_join_null_key_inner_l l
INNER JOIN ${case_db}.t_join_null_key_inner_r r
  ON l.k = r.k
ORDER BY l.k;
