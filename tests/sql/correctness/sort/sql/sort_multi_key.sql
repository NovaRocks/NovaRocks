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
-- @tags=sort
-- Test Objective:
-- 1. Validate ORDER BY on multiple keys with mixed directions.
-- 2. Prevent regressions in tie-breaking behavior for same primary sort key.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic duplicate-key rows.
-- 3. Sort by multiple keys and assert exact row order.
DROP TABLE IF EXISTS ${case_db}.t_sort_multi_key;
CREATE TABLE ${case_db}.t_sort_multi_key (
  grp STRING,
  v INT,
  id INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_sort_multi_key VALUES
  ROW('B', 2, 1),
  ROW('A', 3, 2),
  ROW('A', 1, 3),
  ROW('B', 1, 4),
  ROW('A', 1, 5);
SELECT grp, v, id
FROM ${case_db}.t_sort_multi_key
ORDER BY grp ASC, v ASC, id DESC;
