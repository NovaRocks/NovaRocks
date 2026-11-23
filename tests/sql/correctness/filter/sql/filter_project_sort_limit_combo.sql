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
-- @tags=filter,project,sort,limit
-- Test Objective:
-- 1. Validate combined Filter->Project->Sort->Limit pipeline behavior.
-- 2. Prevent regressions in end-to-end row selection after expression projection.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with NULL and non-NULL metrics.
-- 3. Execute combined query and assert ordered top rows.
DROP TABLE IF EXISTS ${case_db}.t_filter_project_sort_limit_combo;
CREATE TABLE ${case_db}.t_filter_project_sort_limit_combo (
  id INT,
  name VARCHAR,
  qty INT,
  price INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_filter_project_sort_limit_combo VALUES
  ROW(1, 'apple', 2, 5),
  ROW(2, 'banana', NULL, 3),
  ROW(3, 'carrot', 7, 2),
  ROW(4, 'apple', 6, 4),
  ROW(5, 'durian', 10, 1);
SELECT
  name,
  qty * price AS revenue
FROM ${case_db}.t_filter_project_sort_limit_combo
WHERE qty IS NOT NULL AND qty >= 5
ORDER BY revenue DESC, name ASC
LIMIT 3;
