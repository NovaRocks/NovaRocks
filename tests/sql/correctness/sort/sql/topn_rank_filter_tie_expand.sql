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
-- @tags=sort,topn,rank,tie
-- Test Objective:
-- 1. Validate FE rank-topn rewrite path with deterministic output.
-- 2. Cover rank-based topn behavior under duplicate order-by keys.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate scores.
-- 3. Compute RANK and filter with rk <= 2, then assert deterministic output order.
DROP TABLE IF EXISTS ${case_db}.t_topn_rank_filter_tie_expand;
CREATE TABLE ${case_db}.t_topn_rank_filter_tie_expand (
  id INT,
  score INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_topn_rank_filter_tie_expand VALUES
  ROW(1, 100),
  ROW(2, 95),
  ROW(3, 95),
  ROW(4, 90),
  ROW(5, 80);
SELECT id, score, rk
FROM (
  SELECT
    id,
    score,
    RANK() OVER (ORDER BY score DESC) AS rk
  FROM ${case_db}.t_topn_rank_filter_tie_expand
) t
WHERE rk <= 2
ORDER BY rk ASC, id ASC;
