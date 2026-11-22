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
-- @tags=filter
-- Test Objective:
-- 1. Validate basic comparison filtering with nullable numeric columns.
-- 2. Prevent regressions where NULL rows are incorrectly included in range predicates.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with NULL and non-NULL values.
-- 3. Filter by numeric threshold and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_filter_basic_comparison;
CREATE TABLE ${case_db}.t_filter_basic_comparison (
  id INT,
  v INT,
  name VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_filter_basic_comparison VALUES
  ROW(1, 10, 'a'),
  ROW(2, 20, 'b'),
  ROW(3, NULL, 'c'),
  ROW(4, 30, NULL);
-- @explain_contains=stats={rows=
-- @explain_contains=SCAN
SELECT id, v, name
FROM ${case_db}.t_filter_basic_comparison
WHERE v >= 20
ORDER BY id;
