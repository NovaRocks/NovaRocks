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

-- @tags=optimizer,oq13,ranking_window_topn_rejected
-- Test Objective: RankingWindowPredicatePushdown must NOT fire when:
--   (a) the window contains a non-ranking aggregate (avg) alongside rank,
--   (b) the filter only gives a lower bound (rk >= 2, no finite upper bound),
--   (c) PARTITION BY is empty (rank over the full set, no per-partition truncation).
-- Each sub-case asserts @explain_not_contains=partition_limit= and captures
-- result rows so future refactors stay correct.
DROP TABLE IF EXISTS ${case_db}.rw_sales;
CREATE TABLE ${case_db}.rw_sales (region VARCHAR(20), amount INT);
INSERT INTO ${case_db}.rw_sales VALUES
    ROW('A',100),ROW('A',200),ROW('A',50),
    ROW('B',300),ROW('B',150),ROW('B',400),
    ROW('C',10),ROW('C',20);
ANALYZE ${case_db}.rw_sales;

-- Guard (a): window has rank() AND avg() over the same partition.
-- Truncating the partition would corrupt avg's full-partition accumulation,
-- so the rule must stay silent even though the structural shape matches.
-- @explain_not_contains=partition_limit=
SELECT *
FROM (
    SELECT region, amount,
           rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rk,
           avg(amount) OVER (PARTITION BY region ORDER BY amount DESC) AS avg_amt
    FROM ${case_db}.rw_sales
) t
WHERE rk <= 2
ORDER BY region, amount DESC;

-- Guard (b): filter is a lower bound only (rk >= 2).
-- rank_upper_bound returns None, so the rule cannot set partition_limit.
-- @explain_not_contains=partition_limit=
SELECT *
FROM (
    SELECT region, amount,
           rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rk
    FROM ${case_db}.rw_sales
) t
WHERE rk >= 2
ORDER BY region, amount DESC;

-- Guard (c): PARTITION BY is empty — rank() over the entire dataset.
-- Without partitioning, truncating would change global rank semantics,
-- so the rule must not fire.
-- @explain_not_contains=partition_limit=
SELECT *
FROM (
    SELECT region, amount,
           rank() OVER (ORDER BY amount DESC) AS rk
    FROM ${case_db}.rw_sales
) t
WHERE rk <= 2
ORDER BY region, amount DESC;

-- Guard (d): Window has two ranking fns with DIFFERENT ORDER BY signatures
-- (rank() ORDER BY amount DESC and rank() ORDER BY region ASC).
-- The analytic Sort is keyed on the FIRST window's order; setting partition_limit
-- would truncate each partition by amount-order and corrupt the region-ordered rank.
-- group_win_exprs_by_sig returns 2 groups → rule must stay silent.
-- @explain_not_contains=partition_limit=
SELECT *
FROM (
    SELECT region, amount,
           rank() OVER (PARTITION BY region ORDER BY amount DESC) AS rk_amount,
           rank() OVER (PARTITION BY region ORDER BY region ASC)  AS rk_region
    FROM ${case_db}.rw_sales
) t
WHERE rk_region <= 2
ORDER BY region, amount DESC;
