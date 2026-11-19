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
-- @tags=join,m1,semi,anti,subquery
-- Test Objective:
-- 1. Validate M1 semi/anti selection paths for correlated EXISTS and NOT EXISTS.
-- 2. Ensure semi/anti output keeps only left-side slots.
-- 3. Prevent matched and unmatched probe rows from crossing between the two paths.
-- Test Flow:
-- 1. Create/reset left and right tables.
-- 2. Insert deterministic matching and non-matching keys.
-- 3. Query with EXISTS and NOT EXISTS and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.m1_left_side;
DROP TABLE IF EXISTS ${case_db}.m1_right_side;
CREATE TABLE ${case_db}.m1_left_side (
  k INT,
  v INT
) WITH (format_version = 3);
CREATE TABLE ${case_db}.m1_right_side (
  k INT,
  payload INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.m1_left_side VALUES
  ROW(1, 10),
  ROW(2, 20),
  ROW(3, 30);
INSERT INTO ${case_db}.m1_right_side VALUES
  ROW(1, 100),
  ROW(3, 300);
SELECT v
FROM ${case_db}.m1_left_side
WHERE EXISTS (
  SELECT 1
  FROM ${case_db}.m1_right_side
  WHERE m1_left_side.k = m1_right_side.k
)
ORDER BY v;
SELECT v
FROM ${case_db}.m1_left_side
WHERE NOT EXISTS (
  SELECT 1
  FROM ${case_db}.m1_right_side
  WHERE m1_left_side.k = m1_right_side.k
)
ORDER BY v;
