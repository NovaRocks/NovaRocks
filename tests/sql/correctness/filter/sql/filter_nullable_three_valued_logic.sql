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
-- @tags=filter,null,three_valued_logic
-- Test Objective:
-- 1. Validate three-valued boolean predicate behavior in WHERE filtering.
-- 2. Prevent regressions where NULL predicate results are treated as TRUE.
-- Test Flow:
-- 1. Create/reset source table with nullable numeric columns.
-- 2. Insert rows that produce TRUE/FALSE/NULL predicate outcomes.
-- 3. Filter with nullable predicate plus OR branch and assert deterministic output.
DROP TABLE IF EXISTS ${case_db}.t_filter_nullable_three_valued_logic;
CREATE TABLE ${case_db}.t_filter_nullable_three_valued_logic (
  id INT,
  a INT,
  b INT,
  tag STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_filter_nullable_three_valued_logic VALUES
  ROW(1, 3, 1, 'n'),
  ROW(2, 3, 0, 'n'),
  ROW(3, NULL, 2, 'n'),
  ROW(4, 5, NULL, 'n'),
  ROW(5, 1, 1, 'force'),
  ROW(6, NULL, NULL, 'force');
SELECT
  id,
  a,
  b,
  (a > b AND b > 0) AS pred
FROM ${case_db}.t_filter_nullable_three_valued_logic
WHERE (a > b AND b > 0) OR tag = 'force'
ORDER BY id;
