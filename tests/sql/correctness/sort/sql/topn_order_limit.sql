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
-- @tags=sort,limit,topn
-- Test Objective:
-- 1. Validate TopN behavior with ORDER BY + LIMIT.
-- 2. Prevent regressions in tie-break ordering for same score rows.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic score rows including ties.
-- 3. Query top-N rows with deterministic ordering.
DROP TABLE IF EXISTS ${case_db}.t_topn_order_limit;
CREATE TABLE ${case_db}.t_topn_order_limit (
  id INT,
  score INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_topn_order_limit VALUES
  ROW(1, 70),
  ROW(2, 95),
  ROW(3, 88),
  ROW(4, 95),
  ROW(5, 60);
SELECT id, score
FROM ${case_db}.t_topn_order_limit
ORDER BY score DESC, id ASC
LIMIT 3;
