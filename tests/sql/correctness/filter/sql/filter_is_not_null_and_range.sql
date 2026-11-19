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
-- 1. Validate IS NOT NULL combined with range predicates.
-- 2. Prevent regressions where nullable rows leak into bounded filters.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows with NULL and boundary numeric values.
-- 3. Filter with IS NOT NULL and range boundaries, then assert output.
DROP TABLE IF EXISTS ${case_db}.t_filter_is_not_null_and_range;
CREATE TABLE ${case_db}.t_filter_is_not_null_and_range (
  id INT,
  k INT,
  payload STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_filter_is_not_null_and_range VALUES
  ROW(1, NULL, 'n1'),
  ROW(2, 5, 'p2'),
  ROW(3, 10, 'p3'),
  ROW(4, 11, 'p4'),
  ROW(5, 20, 'p5');
SELECT id, k, payload
FROM ${case_db}.t_filter_is_not_null_and_range
WHERE k IS NOT NULL AND k >= 10 AND k < 20
ORDER BY id;
