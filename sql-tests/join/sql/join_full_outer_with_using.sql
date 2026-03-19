-- @tags=join,full_outer,using
-- Test Objective:
-- Validate FULL OUTER JOIN with USING clause across single-key, multi-key, multi-table
-- chained joins, mixed join types, CTEs, subqueries, inline VALUES, and aggregations.
-- Covers: NULL coalescing in USING columns, ROLLUP/CUBE with FULL OUTER,
-- window functions over FULL OUTER results, and type promotion (TINYINT/SMALLINT/BIGINT).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t2;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t3;

-- query 4
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t4;

-- query 5
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 INT,
    k2 INT,
    v1 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- query 6
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
    k1 INT,
    k2 INT,
    v2 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (
    k1 INT,
    k2 INT,
    v3 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (
    k1 INT,
    k2 INT,
    v4 VARCHAR(10)
) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES
    (1, 10, 'a1'),
    (2, 20, 'a2'),
    (3, NULL, 'a3'),
    (NULL, 40, 'a4'),
    (NULL, NULL, 'a5');

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES
    (1, 10, 'b1'),
    (4, 40, 'b2'),
    (5, NULL, 'b3'),
    (NULL, 60, 'b4'),
    (NULL, NULL, 'b5');

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES
    (1, 10, 'c1'),
    (2, 20, 'c2'),
    (6, 60, 'c3'),
    (NULL, 70, 'c4'),
    (NULL, NULL, 'c5');

-- query 12
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES
    (1, 10, 'd1'),
    (7, 70, 'd2'),
    (8, NULL, 'd3'),
    (NULL, 80, 'd4'),
    (NULL, NULL, 'd5');

-- query 13
-- FULL OUTER JOIN single-key USING
-- @order_sensitive=true
SELECT k1, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
ORDER BY k1, v1, v2;

-- query 14
-- FULL OUTER JOIN multi-key USING
-- @order_sensitive=true
SELECT k1, k2, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
ORDER BY k1, k2, v1, v2;

-- query 15
-- FULL OUTER JOIN multi-key USING: SELECT *
-- @order_sensitive=true
SELECT *
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
ORDER BY k1, k2, v1, v2;

-- query 16
-- FULL OUTER JOIN with WHERE filter on USING column
-- @order_sensitive=true
SELECT k1, k2, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE k1 IS NOT NULL
ORDER BY k1, k2;

-- query 17
-- FULL OUTER JOIN single-key USING + aggregation
-- @order_sensitive=true
SELECT k1, COUNT(*) as cnt, COUNT(v1) as cnt_v1, COUNT(v2) as cnt_v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
GROUP BY k1
ORDER BY k1;

-- query 18
-- FULL OUTER JOIN single-key USING + HAVING
-- @order_sensitive=true
SELECT k1, COUNT(*) as cnt
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
GROUP BY k1
HAVING k1 > 1 OR k1 IS NULL
ORDER BY k1;

-- query 19
-- Three-table chained FULL OUTER JOIN USING
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
         FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- query 20
-- Mixed: LEFT + FULL OUTER JOIN USING
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM ${case_db}.t1 LEFT JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- query 21
-- Mixed: FULL OUTER + LEFT JOIN USING
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        LEFT JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- query 22
-- Mixed: FULL OUTER + INNER JOIN USING
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- query 23
-- Mixed: INNER + FULL OUTER JOIN USING
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- query 24
-- Four-table chained FULL OUTER JOIN USING
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 25
-- LEFT + FULL OUTER + FULL OUTER + FULL OUTER
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 LEFT JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 26
-- FULL OUTER + INNER + FULL OUTER
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        INNER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 27
-- FULL OUTER + FULL OUTER + RIGHT
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        RIGHT JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 28
-- LEFT + RIGHT + FULL OUTER
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 LEFT JOIN ${case_db}.t2 USING(k1, k2)
        RIGHT JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 29
-- FULL OUTER JOIN multi-key USING + GROUP BY aggregation
-- @order_sensitive=true
SELECT k1, k2, SUM(CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END) as t1_count,
       SUM(CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END) as t2_count
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
GROUP BY k1, k2
ORDER BY k1, k2;

-- query 30
-- Four-table FULL OUTER + WHERE filter
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
WHERE (k1 = 1 AND k2 = 10) OR (v1 IS NULL AND v2 IS NULL)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 31
-- Four-table FULL OUTER single-key USING + aggregation HAVING
-- @order_sensitive=true
SELECT k1,
       COUNT(*) as total_rows,
       COUNT(v1) as t1_rows,
       COUNT(v2) as t2_rows,
       COUNT(v3) as t3_rows,
       COUNT(v4) as t4_rows
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
        FULL OUTER JOIN ${case_db}.t3 USING(k1)
        FULL OUTER JOIN ${case_db}.t4 USING(k1)
GROUP BY k1
HAVING COUNT(*) > 1
ORDER BY k1;

-- query 32
-- Four-table FULL OUTER: DESC + ASC ordering
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1 DESC, k2 ASC, v1, v2;

-- query 33
-- Three-table FULL OUTER USING + DISTINCT
-- @order_sensitive=true
SELECT DISTINCT k1, k2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2;

-- query 34
-- Four-table FULL OUTER USING + IN subquery filter
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM ${case_db}.t1 WHERE k1 IS NOT NULL)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 35
-- Three-table FULL OUTER USING + CASE expression
-- @order_sensitive=false
SELECT k1, k2,
       CASE WHEN k1 IS NULL THEN 'NULL_K1'
            WHEN k1 < 3 THEN 'SMALL'
            ELSE 'LARGE' END as k1_category,
       v1, v2, v3
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2;

-- query 36
-- Four-table FULL OUTER USING + aggregation with COUNT DISTINCT
-- @order_sensitive=true
SELECT k1, k2,
       COUNT(*) as cnt,
       COUNT(DISTINCT v1) as dist_v1,
       COUNT(DISTINCT v2) as dist_v2,
       MAX(v3) as max_v3,
       MIN(v4) as min_v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
GROUP BY k1, k2
ORDER BY k1, k2;

-- query 37
-- Four-table FULL OUTER USING + LIMIT
-- @order_sensitive=false
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2
LIMIT 10;

-- query 38
-- Four-table FULL OUTER USING + COALESCE
-- @order_sensitive=false
SELECT k1, k2,
       COALESCE(v1, v2, v3, v4) as value
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2;

-- query 39
-- Four-table FULL OUTER USING + complex WHERE
-- @order_sensitive=false
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
WHERE (k1 > 0 OR k1 IS NULL) AND (k2 < 50 OR k2 IS NULL)
ORDER BY k1, k2;

-- query 40
-- Four-table FULL OUTER USING + GROUP BY ROLLUP
-- @order_sensitive=false
SELECT k1, k2, COUNT(*) as cnt
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
GROUP BY ROLLUP(k1, k2)
ORDER BY k1, k2;

-- query 41
-- Four-table single-key FULL OUTER USING
-- @order_sensitive=true
SELECT k1, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
        FULL OUTER JOIN ${case_db}.t3 USING(k1)
        FULL OUTER JOIN ${case_db}.t4 USING(k1)
ORDER BY k1, v1, v2, v3, v4;

-- query 42
-- Four-table single-key FULL OUTER USING + non_null_values HAVING
-- @order_sensitive=true
SELECT k1,
       COUNT(*) as total,
       SUM(CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END) +
       SUM(CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END) +
       SUM(CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END) +
       SUM(CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END) as non_null_values
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
        FULL OUTER JOIN ${case_db}.t3 USING(k1)
        FULL OUTER JOIN ${case_db}.t4 USING(k1)
GROUP BY k1
HAVING non_null_values >= 2
ORDER BY k1;

-- query 43
-- INNER + FULL OUTER + LEFT chain
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        LEFT JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 44
-- RIGHT + FULL OUTER + INNER chain
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 RIGHT JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        INNER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3, v4;

-- query 45
-- Subquery with NOT NULL filter + FULL OUTER chain
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM (SELECT k1, k2, v1 FROM ${case_db}.t1 WHERE k1 IS NOT NULL) as sub1
     FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
     FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- query 46
-- FULL OUTER + subquery with WHERE filter
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN (SELECT k1, k2, v3 FROM ${case_db}.t3 WHERE k2 > 0) as sub3 USING(k1, k2)
ORDER BY k1, k2, v1, v2, v3;

-- query 47
-- CTE + FULL OUTER JOIN USING
-- @order_sensitive=true
WITH cte1 AS (
    SELECT k1, k2, v1 FROM ${case_db}.t1 WHERE k1 <= 3 OR k1 IS NULL
),
cte2 AS (
    SELECT k1, k2, v2 FROM ${case_db}.t2 WHERE k1 >= 3 OR k1 IS NULL
)
SELECT k1, k2, v1, v2
FROM cte1 FULL OUTER JOIN cte2 USING(k1, k2)
ORDER BY k1, k2;

-- query 48
-- CTE wrapping FULL OUTER as base + another FULL OUTER
-- @order_sensitive=true
WITH base_join AS (
    SELECT k1, k2, v1, v2
    FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
)
SELECT k1, k2, v1, v2, v3
FROM base_join FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY k1, k2;

-- query 49
-- Three-table FULL OUTER USING + aggregation HAVING
-- @order_sensitive=true
SELECT k1,
       MAX(v1) as max_v1,
       MIN(v2) as min_v2,
       AVG(CAST(k2 AS DOUBLE)) as avg_k2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
GROUP BY k1
HAVING MAX(v1) IS NOT NULL OR MIN(v2) IS NOT NULL
ORDER BY k1;

-- query 50
-- FULL OUTER USING + UNION ALL
-- @order_sensitive=true
SELECT k1, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
WHERE k1 = 1
UNION ALL
SELECT k1, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
WHERE k1 = 2
ORDER BY k1, v1, v2;

-- query 51
-- Mixed LEFT + FULL OUTER + RIGHT + FULL OUTER with subquery
-- @order_sensitive=true
SELECT k1, v1, v2, v3, v4
FROM ${case_db}.t1 LEFT JOIN ${case_db}.t2 USING(k1)
        FULL OUTER JOIN ${case_db}.t3 USING(k1)
        RIGHT JOIN ${case_db}.t4 USING(k1)
        FULL OUTER JOIN (SELECT k1, 'extra' as v_extra FROM ${case_db}.t1 WHERE k1 < 5) sub USING(k1)
ORDER BY k1;

-- query 52
-- FULL OUTER USING + window functions
-- @order_sensitive=true
SELECT k1, k2, v1, v2,
       ROW_NUMBER() OVER (PARTITION BY k1 ORDER BY k2) as rn,
       COUNT(*) OVER (PARTITION BY k1) as cnt_per_k1
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
ORDER BY k1, k2;

-- query 53
-- Four-table FULL OUTER USING + BETWEEN + complex OR filter
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
WHERE (k1 BETWEEN 1 AND 5) OR
      (k1 IS NULL AND k2 IS NOT NULL) OR
      (k2 > 50 AND k1 IS NOT NULL)
ORDER BY k1, k2;

-- query 54
-- FULL OUTER USING + EXISTS subquery
-- @order_sensitive=true
SELECT k1, k2, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE EXISTS (
    SELECT 1 FROM ${case_db}.t3 WHERE t3.k1 = k1 AND t3.k2 = k2
)
ORDER BY k1, k2;

-- query 55
-- FULL OUTER USING + NOT EXISTS (empty result)
-- @order_sensitive=true
SELECT k1, k2, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE NOT EXISTS (
    SELECT 1 FROM ${case_db}.t3 WHERE t3.k1 = k1
)
ORDER BY k1, k2;

-- query 56
-- FULL OUTER USING + CASE match_status
-- @order_sensitive=true
SELECT k1, k2,
       CASE
         WHEN v1 IS NOT NULL AND v2 IS NOT NULL THEN 'BOTH'
         WHEN v1 IS NOT NULL THEN 'T1_ONLY'
         WHEN v2 IS NOT NULL THEN 'T2_ONLY'
         ELSE 'NEITHER'
       END as match_status,
       COALESCE(v1, v2) as value
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
ORDER BY k1, k2;

-- query 57
-- INNER + FULL OUTER + INNER + FULL OUTER chain
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        INNER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2;

-- query 58
-- FULL OUTER USING + IN subquery + IS NULL
-- @order_sensitive=true
SELECT k1, k2, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM ${case_db}.t3 WHERE k1 IS NOT NULL)
ORDER BY k1, k2;

-- query 59
-- FULL OUTER USING + ORDER BY COALESCE
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
ORDER BY COALESCE(k1, 999), COALESCE(k2, 999), v1;

-- query 60
-- FULL OUTER + LEFT + FULL OUTER + RIGHT chain with dup subquery
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        LEFT JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
        RIGHT JOIN (SELECT k1, k2, v1 as v1_dup FROM ${case_db}.t1) dup USING(k1, k2)
ORDER BY k1, k2
LIMIT 15;

-- query 61
-- FULL OUTER USING with WHERE on both USING columns
-- @order_sensitive=true
SELECT k1, k2, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE (k1 = 1 OR k1 IS NULL) AND (k2 = 10 OR k2 IS NULL)
ORDER BY k1, k2;

-- query 62
-- FULL OUTER single-key USING + match_score aggregation
-- @order_sensitive=true
SELECT k1,
       SUM(CASE
           WHEN v1 IS NOT NULL AND v2 IS NOT NULL THEN 2
           WHEN v1 IS NOT NULL OR v2 IS NOT NULL THEN 1
           ELSE 0
       END) as match_score,
       COUNT(*) as total
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1)
GROUP BY k1
ORDER BY match_score DESC, k1;

-- query 63
-- Four-table FULL OUTER USING + COALESCE + tables_present count
-- @order_sensitive=true
SELECT DISTINCT
       COALESCE(k1, -1) as k1_coalesced,
       COALESCE(k2, -1) as k2_coalesced,
       CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END as tables_present
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1_coalesced, k2_coalesced;

-- query 64
-- FULL OUTER USING + IN subquery with v3 filter
-- @order_sensitive=true
SELECT k1, k2, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE k1 IN (SELECT k1 FROM ${case_db}.t3 WHERE v3 IS NOT NULL)
   OR k1 IS NULL
ORDER BY k1, k2;

-- query 65
-- Inline VALUES: single-key FULL OUTER JOIN USING
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 'a')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b')) AS u(k, v2) USING (k)
ORDER BY k;

-- query 66
-- Inline VALUES: multi-key FULL OUTER JOIN USING
-- @order_sensitive=true
SELECT * FROM
    (VALUES (0, 1, 2), (3, 6, 5)) a(x1, y, z1) FULL OUTER JOIN
    (VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)
ORDER BY y, x1, z1, x2, z2;

-- query 67
-- Inline VALUES: FULL OUTER with NULLs
-- @order_sensitive=true
SELECT y, x1, x2 FROM
    (VALUES (0, 1, 2), (3, 6, 5), (3, NULL, 5)) a(x1, y, z1) FULL OUTER JOIN
    (VALUES (3, 1, 5), (0, 4, 2)) b(x2, y, z2) USING (y)
ORDER BY y, x1, x2;

-- query 68
-- Inline VALUES: type promotion TINYINT vs INT
-- @order_sensitive=true
SELECT * FROM
    (VALUES (CAST(1 AS TINYINT), 'a')) AS t(k, v1) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);

-- query 69
-- Inline VALUES: type promotion SMALLINT vs BIGINT
-- @order_sensitive=true
SELECT * FROM
    (VALUES (CAST(1 AS SMALLINT), 'a')) AS t(k, v1) JOIN
    (VALUES (CAST(1 AS BIGINT), 'b')) AS u(k, v2) USING (k);

-- query 70
-- Inline VALUES: USING column not first position
-- @order_sensitive=true
SELECT * FROM
    (VALUES ('a', 1)) AS t(v1, k) JOIN
    (VALUES (1, 'b')) AS u(k, v2) USING (k);

-- query 71
-- Inline VALUES + persistent table USING
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 'inline')) AS inline_t(k1, val) LEFT JOIN ${case_db}.t1 USING(k1)
ORDER BY k1, val;

-- query 72
-- Persistent table + Inline VALUES FULL OUTER USING
-- @order_sensitive=true
SELECT * FROM
    ${case_db}.t1 FULL OUTER JOIN
    (VALUES (10, 20, 'inline')) AS inline_t(k1, k2, val) USING(k1, k2)
ORDER BY k1, k2;

-- query 73
-- Persistent + Inline VALUES FULL OUTER + LEFT chain
-- @order_sensitive=true
SELECT k1, k2, v1, val, v2 FROM
    ${case_db}.t1 FULL OUTER JOIN
    (VALUES (1, 10, 'inline1'), (2, 20, 'inline2')) AS inline_t(k1, k2, val) USING(k1, k2)
    LEFT JOIN ${case_db}.t2 USING(k1, k2)
ORDER BY k1, k2;

-- query 74
-- Three inline VALUES FULL OUTER chained
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 'a')) AS t1_val(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b')) AS t2_val(k, v2) USING (k) FULL OUTER JOIN
    (VALUES (1, 'c'), (3, 'd')) AS t3_val(k, v3) USING (k)
ORDER BY k;

-- query 75
-- Inline VALUES FULL OUTER + aggregation
-- @order_sensitive=true
SELECT k, COUNT(*) as cnt, COUNT(v1) as cnt_v1, COUNT(v2) as cnt_v2
FROM
    (VALUES (1, 'a'), (1, 'aa'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (3, 'y')) AS u(k, v2) USING (k)
GROUP BY k
ORDER BY k;

-- query 76
-- Inline VALUES FULL OUTER as subquery
-- @order_sensitive=true
SELECT k, v1, v2 FROM (
    SELECT * FROM
        (VALUES (1, 'a')) AS t(k, v1) FULL OUTER JOIN
        (VALUES (1, 'b'), (2, 'c')) AS u(k, v2) USING (k)
) sub
ORDER BY k;

-- query 77
-- Inline VALUES FULL OUTER + WHERE filter
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'x'), (3, 'y'), (4, 'z')) AS u(k, v2) USING (k)
WHERE k >= 2
ORDER BY k;

-- query 78
-- Inline VALUES FULL OUTER + expression columns
-- @order_sensitive=true
SELECT k, k * 10 as k_scaled, CONCAT('K', CAST(k AS STRING)) as k_str, v1, v2
FROM
    (VALUES (1, 'a'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'x'), (3, 'y')) AS u(k, v2) USING (k)
WHERE k IS NOT NULL
ORDER BY k;

-- query 79
-- Inline VALUES FULL OUTER + CASE + COALESCE
-- @order_sensitive=true
SELECT k,
       CASE WHEN k < 2 THEN 'SMALL' ELSE 'LARGE' END as size_cat,
       COALESCE(v1, 'NULL_V1') as v1_safe,
       COALESCE(v2, 'NULL_V2') as v2_safe
FROM
    (VALUES (1, 'a'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b'), (3, 'd')) AS u(k, v2) USING (k)
ORDER BY k;

-- query 80
-- Inline VALUES: larger overlapping sets
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (3, 'x'), (4, 'y'), (5, 'z'), (6, 'w'), (7, 'v')) AS u(k, v2) USING (k)
ORDER BY k;

-- query 81
-- Inline VALUES: NULLs in join keys
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 'a'), (NULL, 'b'), (2, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (NULL, 'y'), (3, 'z')) AS u(k, v2) USING (k)
ORDER BY k, v1, v2;

-- query 82
-- Three inline VALUES with increasing sizes
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 'a')) AS t1_val(k, v1) FULL OUTER JOIN
    (VALUES (1, 'b'), (2, 'bb')) AS t2_val(k, v2) USING (k) FULL OUTER JOIN
    (VALUES (1, 'c'), (2, 'cc'), (3, 'ccc')) AS t3_val(k, v3) USING (k)
ORDER BY k;

-- query 83
-- Persistent + Inline VALUES with TINYINT cast
-- @order_sensitive=true
SELECT k1, v1, val, v2 FROM
    ${case_db}.t1 FULL OUTER JOIN
    (VALUES (CAST(1 AS TINYINT), 100), (CAST(5 AS TINYINT), 500)) AS inline_t(k1, val) USING(k1)
    LEFT JOIN ${case_db}.t2 USING(k1)
ORDER BY k1;

-- query 84
-- Inline VALUES FULL OUTER + complex WHERE with BETWEEN
-- @order_sensitive=true
SELECT * FROM
    (VALUES (1, 10), (2, 20), (3, 30)) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 100), (3, 200), (4, 300)) AS u(k, v2) USING (k)
WHERE (k IS NULL) OR (k BETWEEN 2 AND 3 AND (v1 > 15 OR v2 > 150))
ORDER BY k;

-- query 85
-- Inline VALUES FULL OUTER + computed expressions + ORDER BY COALESCE
-- @order_sensitive=true
SELECT k, v1, v2,
       k + 100 as k_offset,
       CASE WHEN k IS NULL THEN -1 ELSE k END as k_safe
FROM
    (VALUES (1, 'a'), (3, 'c')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (2, 'b'), (3, 'd'), (4, 'e')) AS u(k, v2) USING (k)
ORDER BY COALESCE(k, 999), v1, v2;

-- query 86
-- Inline VALUES FULL OUTER + DISTINCT on USING column with duplicates
-- @order_sensitive=true
SELECT DISTINCT k
FROM
    (VALUES (1, 'a'), (1, 'aa'), (2, 'b')) AS t(k, v1) FULL OUTER JOIN
    (VALUES (1, 'x'), (1, 'xx'), (2, 'y')) AS u(k, v2) USING (k)
ORDER BY k;

-- query 87
-- Inline VALUES FULL OUTER with STRING keys
-- @order_sensitive=true
SELECT * FROM
    (VALUES ('apple', 1), ('banana', 2)) AS t(name, v1) FULL OUTER JOIN
    (VALUES ('banana', 10), ('cherry', 20)) AS u(name, v2) USING (name)
ORDER BY name;

-- query 88
-- Persistent + two Inline VALUES: FULL OUTER + LEFT + FULL OUTER chain
-- @order_sensitive=true
SELECT k1, v1, val1, v2, val2 FROM
    ${case_db}.t1 FULL OUTER JOIN
    (VALUES (1, 'V1'), (2, 'V2')) AS v1_t(k1, val1) USING(k1) LEFT JOIN
    ${case_db}.t2 USING(k1) FULL OUTER JOIN
    (VALUES (1, 'V3'), (5, 'V5')) AS v2_t(k1, val2) USING(k1)
ORDER BY k1
LIMIT 20;

-- query 89
-- Derived table (LEFT JOIN) FULL OUTER derived table (RIGHT JOIN)
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM (
    SELECT k1, k2, v1, v2
    FROM ${case_db}.t1 LEFT JOIN ${case_db}.t2 USING(k1, k2)
) left_result
FULL OUTER JOIN (
    SELECT k1, k2, v3, v4
    FROM ${case_db}.t3 RIGHT JOIN ${case_db}.t4 USING(k1, k2)
) right_result USING(k1, k2)
ORDER BY k1, k2;

-- query 90
-- FULL OUTER USING + arithmetic on USING columns
-- @order_sensitive=true
SELECT k1, k2,
       k1 + k2 as sum_k,
       k1 * k2 as product_k,
       v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE k1 IS NOT NULL AND k2 IS NOT NULL
ORDER BY k1, k2;

-- query 91
-- FULL OUTER USING + string functions on values
-- @order_sensitive=true
SELECT k1, k2,
       CONCAT('K1:', CAST(k1 AS STRING)) as k1_str,
       LENGTH(v1) as len_v1,
       UPPER(v2) as v2_upper
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
WHERE k1 IS NOT NULL
ORDER BY k1, k2;

-- query 92
-- INNER + FULL OUTER + INNER + FULL OUTER (all tables)
-- @order_sensitive=true
SELECT k1, k2, v1, v2, v3, v4
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
        INNER JOIN ${case_db}.t4 USING(k1, k2)
ORDER BY k1, k2;

-- query 93
-- Three-table FULL OUTER USING + aggregation HAVING with AVG
-- @order_sensitive=true
SELECT k1, COUNT(*) as cnt, AVG(k2) as avg_k2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(k1, k2)
        FULL OUTER JOIN ${case_db}.t3 USING(k1, k2)
GROUP BY k1
HAVING k1 > 0 AND AVG(k2) > 10
ORDER BY k1;

-- query 94
-- @skip_result_check=true
DROP DATABASE IF EXISTS ${case_db};
