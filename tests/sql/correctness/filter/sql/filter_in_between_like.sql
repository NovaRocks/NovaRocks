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
-- @tags=filter,string
-- Test Objective:
-- 1. Validate combined LIKE and BETWEEN predicates.
-- 2. Prevent regressions in mixed string+numeric predicate conjunctions.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic string and score rows.
-- 3. Apply LIKE + BETWEEN filter and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_filter_in_between_like;
CREATE TABLE ${case_db}.t_filter_in_between_like (
  id INT,
  name VARCHAR,
  score INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_filter_in_between_like VALUES
  ROW(1, 'apple', 12),
  ROW(2, 'apricot', 20),
  ROW(3, 'banana', 15),
  ROW(4, 'azure', 9),
  ROW(5, 'avocado', 21);
SELECT id, name, score
FROM ${case_db}.t_filter_in_between_like
WHERE name LIKE 'a%' AND score BETWEEN 10 AND 20
ORDER BY id;
