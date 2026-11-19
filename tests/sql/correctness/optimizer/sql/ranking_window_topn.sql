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

-- @tags=optimizer,oq13,ranking_window_topn
-- Test Objective: RankingWindowPredicatePushdown rewrites Filter(rk<=K) over
-- Window(rank/row_number/dense_rank OVER (PARTITION BY p ORDER BY o)) by
-- setting partition_limit=K + topn_type on the analytic Sort.  The Window and
-- Filter are preserved so results are identical to the rule-off path.
DROP TABLE IF EXISTS ${case_db}.rw_sales;
CREATE TABLE ${case_db}.rw_sales (region VARCHAR(20), amount INT);
INSERT INTO ${case_db}.rw_sales VALUES
    ROW('A',100),ROW('A',200),ROW('A',50),
    ROW('B',300),ROW('B',150),ROW('B',400),
    ROW('C',10),ROW('C',20);
ANALYZE ${case_db}.rw_sales;

-- Case 1a: rule fires for rank() <= 2.
-- EXPLAIN must show partition_limit=2 topn_type=RANK on the SORT line
-- and the WINDOW node must still be present.
-- @explain_contains=partition_limit=2 topn_type=RANK
-- @explain_contains=WINDOW
SELECT *
FROM (
    SELECT region, amount,
           rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rk
    FROM ${case_db}.rw_sales
) t
WHERE rk <= 2
ORDER BY region, amount DESC;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 1b: rule OFF — EXPLAIN must NOT show partition_limit token.
-- Result rows must be identical to Case 1a.
-- @explain_not_contains=partition_limit=
SELECT *
FROM (
    SELECT region, amount,
           rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rk
    FROM ${case_db}.rw_sales
) t
WHERE rk <= 2
ORDER BY region, amount DESC;

SET SESSION disable_optimizer_rules = '';

-- Case 2: row_number() <= 1 fires with topn_type=ROW_NUMBER.
-- @explain_contains=partition_limit=1 topn_type=ROW_NUMBER
-- @explain_contains=WINDOW
SELECT *
FROM (
    SELECT region, amount,
           row_number() OVER (PARTITION BY region ORDER BY amount DESC) AS rn
    FROM ${case_db}.rw_sales
) t
WHERE rn <= 1
ORDER BY region, amount DESC;
