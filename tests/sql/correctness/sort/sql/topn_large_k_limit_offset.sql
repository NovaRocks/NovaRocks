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
-- @tags=sort,topn,limit,offset,large_k
-- Test Objective:
-- 1. Validate ORDER BY + LIMIT/OFFSET semantics when requested top-k is much larger than heap threshold.
-- 2. Prevent regressions in large-k page slicing behavior.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with score ties.
-- 3. Query ORDER BY + LIMIT/OFFSET where limit+offset is large enough to hit large-k topn path.
DROP TABLE IF EXISTS ${case_db}.t_topn_large_k_limit_offset;
CREATE TABLE ${case_db}.t_topn_large_k_limit_offset (
  id INT,
  score INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_topn_large_k_limit_offset VALUES
  ROW(1, 100),
  ROW(2, 95),
  ROW(3, 95),
  ROW(4, 90),
  ROW(5, 85),
  ROW(6, 85),
  ROW(7, 80),
  ROW(8, 70);
SELECT id, score
FROM ${case_db}.t_topn_large_k_limit_offset
ORDER BY score DESC, id ASC
OFFSET 2 LIMIT 1500;
