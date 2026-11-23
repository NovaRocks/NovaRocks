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
-- @tags=filter,null
-- Test Objective:
-- 1. Validate OR predicate behavior when one side depends on IS NULL checks.
-- 2. Prevent regressions in boolean short-circuit style evaluation for nullable columns.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows covering NULL and non-NULL combinations.
-- 3. Filter with OR predicate and assert deterministic ordering.
DROP TABLE IF EXISTS ${case_db}.t_filter_null_or_predicate;
CREATE TABLE ${case_db}.t_filter_null_or_predicate (
  id INT,
  a INT,
  b VARCHAR
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_filter_null_or_predicate VALUES
  ROW(1, 1, 'x'),
  ROW(2, NULL, 'x'),
  ROW(3, 2, 'y'),
  ROW(4, NULL, NULL),
  ROW(5, 5, 'z');
SELECT id, a, b
FROM ${case_db}.t_filter_null_or_predicate
WHERE a IS NULL OR b = 'y'
ORDER BY id;
