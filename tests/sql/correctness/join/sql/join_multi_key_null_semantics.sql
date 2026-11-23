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
-- @tags=join,inner,multi_key,null
-- Test Objective:
-- 1. Validate multi-column INNER JOIN key matching semantics with NULL keys.
-- 2. Prevent regressions where rows with any NULL join key are incorrectly matched.
-- Test Flow:
-- 1. Create/reset left and right tables with two join-key columns.
-- 2. Insert deterministic rows including NULL-containing keys on both sides.
-- 3. Execute multi-key INNER JOIN and assert only fully non-NULL key matches.
DROP TABLE IF EXISTS ${case_db}.t_join_multi_key_null_l;
DROP TABLE IF EXISTS ${case_db}.t_join_multi_key_null_r;
CREATE TABLE ${case_db}.t_join_multi_key_null_l (
  k1 INT,
  k2 INT,
  lv VARCHAR
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_join_multi_key_null_r (
  k1 INT,
  k2 INT,
  rv VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_join_multi_key_null_l VALUES
  ROW(1, 1, 'l11'),
  ROW(1, NULL, 'l1n'),
  ROW(NULL, 1, 'ln1'),
  ROW(2, 2, 'l22'),
  ROW(2, 3, 'l23');
INSERT INTO ${case_db}.t_join_multi_key_null_r VALUES
  ROW(1, 1, 'r11'),
  ROW(1, NULL, 'r1n'),
  ROW(NULL, 1, 'rn1'),
  ROW(2, 2, 'r22'),
  ROW(2, 4, 'r24');
SELECT l.k1, l.k2, l.lv, r.rv
FROM ${case_db}.t_join_multi_key_null_l l
INNER JOIN ${case_db}.t_join_multi_key_null_r r
  ON l.k1 = r.k1 AND l.k2 = r.k2
ORDER BY l.lv;
