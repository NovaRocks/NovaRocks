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

-- @tags=sort,oq13,ranking_window_topn_correctness
-- Test Objective: Verify RankingWindowPredicatePushdown correctness — results must be
-- IDENTICAL whether the rule is ON (default) or OFF (SET disable_optimizer_rules=...).
-- Covers: rank()=1, rank()<=k with tie at boundary, dense_rank()<=k with ties,
--         row_number()<=k, NULL ORDER BY values, single-row partition,
--         and a scalar subquery with a rank window.
-- The rule fires by inserting partition_limit=K into the analytic Sort; the Filter and
-- Window nodes are preserved, so per-partition truncation must not alter the result set.

CREATE SCHEMA IF NOT EXISTS ${case_db};
USE ${case_db};

DROP TABLE IF EXISTS ${case_db}.rw_c;
CREATE TABLE ${case_db}.rw_c (
    grp   VARCHAR(10),
    oid   INT,
    score INT
);
INSERT INTO ${case_db}.rw_c VALUES
    -- group A: 4 rows, top-1 is score=400
    ROW('A', 1, 400),
    ROW('A', 2, 300),
    ROW('A', 3, 300),
    ROW('A', 4, 100),
    -- group B: 3 rows, tie at rank 2 (scores 90,90); rank()<=2 must return all 3
    ROW('B', 5, 200),
    ROW('B', 6, 90),
    ROW('B', 7, 90),
    -- group C: 1 row only (single-row partition)
    ROW('C', 8, 50),
    -- group D: rows with NULL score
    ROW('D', 9,  NULL),
    ROW('D', 10, 500),
    ROW('D', 11, 400);

ANALYZE ${case_db}.rw_c;

-- ===========================================================================
-- Case 1: rank() = 1 (top-1 per partition, rule ON)
-- @explain_contains=partition_limit=
-- ===========================================================================
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
    FROM ${case_db}.rw_c
) t
WHERE rk = 1
ORDER BY grp, oid;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 1 rule OFF — must produce identical rows as Case 1 rule ON.
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
    FROM ${case_db}.rw_c
) t
WHERE rk = 1
ORDER BY grp, oid;

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 2: rank() <= 2 with tie AT boundary (group B has 2 rows at rank 2).
-- Rule ON: partition_limit must expand to cover ties.
-- ===========================================================================
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
    FROM ${case_db}.rw_c
) t
WHERE rk <= 2
ORDER BY grp, rk, oid;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 2 rule OFF — must produce identical rows as Case 2 rule ON.
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
    FROM ${case_db}.rw_c
) t
WHERE rk <= 2
ORDER BY grp, rk, oid;

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 3: dense_rank() <= 2 with ties (group B has two rows at dense_rank=2).
-- ===========================================================================
SELECT grp, oid, score, drk
FROM (
    SELECT grp, oid, score,
           dense_rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS drk
    FROM ${case_db}.rw_c
) t
WHERE drk <= 2
ORDER BY grp, drk, oid;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 3 rule OFF — must produce identical rows as Case 3 rule ON.
SELECT grp, oid, score, drk
FROM (
    SELECT grp, oid, score,
           dense_rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS drk
    FROM ${case_db}.rw_c
) t
WHERE drk <= 2
ORDER BY grp, drk, oid;

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 4: row_number() <= 2 (exactly 2 rows per partition, no tie expansion).
-- ===========================================================================
SELECT grp, oid, score, rn
FROM (
    SELECT grp, oid, score,
           row_number() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST, oid) AS rn
    FROM ${case_db}.rw_c
) t
WHERE rn <= 2
ORDER BY grp, rn;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 4 rule OFF — must produce identical rows as Case 4 rule ON.
SELECT grp, oid, score, rn
FROM (
    SELECT grp, oid, score,
           row_number() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST, oid) AS rn
    FROM ${case_db}.rw_c
) t
WHERE rn <= 2
ORDER BY grp, rn;

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 5: NULL in ORDER BY key — group D has one NULL score.
-- NULLS LAST puts NULL at the bottom; rank()<=1 should return the non-NULL top row.
-- ===========================================================================
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
    FROM ${case_db}.rw_c
    WHERE grp = 'D'
) t
WHERE rk <= 1
ORDER BY grp, oid;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 5 rule OFF — must produce identical rows as Case 5 rule ON.
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC NULLS LAST) AS rk
    FROM ${case_db}.rw_c
    WHERE grp = 'D'
) t
WHERE rk <= 1
ORDER BY grp, oid;

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 6: Single-row partition (group C). rank()<=1 must still return that row.
-- ===========================================================================
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
    FROM ${case_db}.rw_c
    WHERE grp = 'C'
) t
WHERE rk <= 1
ORDER BY grp, oid;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 6 rule OFF — must produce identical rows as Case 6 rule ON.
SELECT grp, oid, score, rk
FROM (
    SELECT grp, oid, score,
           rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
    FROM ${case_db}.rw_c
    WHERE grp = 'C'
) t
WHERE rk <= 1
ORDER BY grp, oid;

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 7: Scalar subquery with rank window — returns exactly 1 row (OK path).
-- Uses group C (single row) so the scalar always succeeds with the rule ON or OFF.
-- ===========================================================================
SELECT (
    SELECT score
    FROM (
        SELECT score,
               rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
        FROM ${case_db}.rw_c
        WHERE grp = 'C'
    ) sub
    WHERE rk <= 1
) AS top_c_score;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 7 rule OFF — scalar subquery still returns the same single value.
SELECT (
    SELECT score
    FROM (
        SELECT score,
               rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
        FROM ${case_db}.rw_c
        WHERE grp = 'C'
    ) sub
    WHERE rk <= 1
) AS top_c_score;

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 8: Scalar subquery that returns >1 row must error identically rule ON and OFF.
-- Groups A and B both have rank()=1 (one row each), so selecting across both groups
-- WITHOUT a partition filter makes the scalar return 2 rows → AssertOneRow error.
-- ===========================================================================
-- @expect_error=assert_num_rows failed
SELECT (
    SELECT score
    FROM (
        SELECT score,
               rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
        FROM ${case_db}.rw_c
        WHERE grp IN ('A', 'B')
    ) sub
    WHERE rk = 1
);

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 8 rule OFF — same error must fire.
-- @expect_error=assert_num_rows failed
SELECT (
    SELECT score
    FROM (
        SELECT score,
               rank() OVER (PARTITION BY grp ORDER BY score DESC) AS rk
        FROM ${case_db}.rw_c
        WHERE grp IN ('A', 'B')
    ) sub
    WHERE rk = 1
);

SET SESSION disable_optimizer_rules = '';

-- ===========================================================================
-- Case 9: Two ranking fns with DIFFERENT ORDER BY — rule must NOT fire.
-- rank() ORDER BY a DESC ranks by value; rank() ORDER BY b DESC ranks by row order.
-- Filter rkb <= 2 should return the 2 rows with the largest b (b=4 and b=3).
-- With the bug, rule ON would corrupt results; with the fix, rule ON is a no-op
-- for this shape and results must match rule OFF exactly.
-- ===========================================================================
DROP TABLE IF EXISTS ${case_db}.rw_c9;
CREATE TABLE ${case_db}.rw_c9 (grp INT, a INT, b INT);
INSERT INTO ${case_db}.rw_c9 VALUES ROW(1,40,1),ROW(1,30,2),ROW(1,20,3),ROW(1,10,4);

-- @explain_not_contains=partition_limit=
SELECT grp,a,b,rka,rkb
FROM (
    SELECT grp, a, b,
           rank() OVER (PARTITION BY grp ORDER BY a DESC) AS rka,
           rank() OVER (PARTITION BY grp ORDER BY b DESC) AS rkb
    FROM ${case_db}.rw_c9
) x
WHERE rkb <= 2
ORDER BY b DESC;

SET SESSION disable_optimizer_rules = 'RankingWindowPredicatePushdown';

-- Case 9 rule OFF — must produce identical rows as Case 9 rule ON.
SELECT grp,a,b,rka,rkb
FROM (
    SELECT grp, a, b,
           rank() OVER (PARTITION BY grp ORDER BY a DESC) AS rka,
           rank() OVER (PARTITION BY grp ORDER BY b DESC) AS rkb
    FROM ${case_db}.rw_c9
) x
WHERE rkb <= 2
ORDER BY b DESC;

SET SESSION disable_optimizer_rules = '';
