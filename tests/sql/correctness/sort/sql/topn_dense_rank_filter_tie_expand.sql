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
-- @tags=sort,topn,dense_rank,tie
-- Test Objective:
-- 1. Validate dense_rank filter semantics with deterministic output order.
-- 2. Cover duplicate-key peer-group behavior for dense_rank queries.
-- 3. Document current FE behavior: StarRocks FE does not rewrite this pattern
--    to `TOP-N type: DENSE_RANK` yet, so this case validates semantic fallback.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows with duplicate scores in multiple peer groups.
-- 3. Compute DENSE_RANK and filter with drk <= 3, then assert deterministic output order.
DROP TABLE IF EXISTS ${case_db}.t_topn_dense_rank_filter_tie_expand;
CREATE TABLE ${case_db}.t_topn_dense_rank_filter_tie_expand (
  id INT,
  score INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_topn_dense_rank_filter_tie_expand VALUES
  ROW(1, 100),
  ROW(2, 95),
  ROW(3, 95),
  ROW(4, 90),
  ROW(5, 90),
  ROW(6, 80);
SELECT id, score, drk
FROM (
  SELECT
    id,
    score,
    DENSE_RANK() OVER (ORDER BY score DESC) AS drk
  FROM ${case_db}.t_topn_dense_rank_filter_tie_expand
) t
WHERE drk <= 3
ORDER BY drk ASC, id ASC;
