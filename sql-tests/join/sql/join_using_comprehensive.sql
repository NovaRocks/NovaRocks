-- @tags=join,using,comprehensive,multi_table
-- Test Objective:
-- Comprehensive validation of USING-clause joins with many tables (up to 8) and
-- mixed join types (FULL OUTER, INNER, LEFT, RIGHT). Tables use different integer
-- key widths (TINYINT, SMALLINT, INT, BIGINT) to stress type promotion.
-- Covers: 6-table and 8-table join chains, mixed join type permutations, aggregation
-- over multi-table FULL OUTER results, CTE-based complex queries, inline VALUES
-- with persistent tables, window functions, and correlated subqueries.

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
DROP TABLE IF EXISTS ${case_db}.t5;

-- query 6
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t6;

-- query 7
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t7;

-- query 8
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t8;

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    id TINYINT,
    v1 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
    id SMALLINT,
    v2 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 11
-- @skip_result_check=true
CREATE TABLE ${case_db}.t3 (
    id INT,
    v3 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.t4 (
    id BIGINT,
    v4 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.t5 (
    id TINYINT,
    v5 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 14
-- @skip_result_check=true
CREATE TABLE ${case_db}.t6 (
    id INT,
    v6 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 15
-- @skip_result_check=true
CREATE TABLE ${case_db}.t7 (
    id BIGINT,
    v7 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 16
-- @skip_result_check=true
CREATE TABLE ${case_db}.t8 (
    id SMALLINT,
    v8 VARCHAR(20)
) ENGINE=OLAP
DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES
(1, 't1_1'),
(2, 't1_2'),
(3, 't1_3'),
(NULL, 't1_null'),
(10, 't1_10');

-- query 18
-- @skip_result_check=true
INSERT INTO ${case_db}.t2 VALUES
(1, 't2_1'),
(2, 't2_2'),
(4, 't2_4'),
(NULL, 't2_null'),
(20, 't2_20');

-- query 19
-- @skip_result_check=true
INSERT INTO ${case_db}.t3 VALUES
(1, 't3_1'),
(3, 't3_3'),
(5, 't3_5'),
(NULL, 't3_null'),
(30, 't3_30');

-- query 20
-- @skip_result_check=true
INSERT INTO ${case_db}.t4 VALUES
(2, 't4_2'),
(4, 't4_4'),
(6, 't4_6'),
(NULL, 't4_null'),
(40, 't4_40');

-- query 21
-- @skip_result_check=true
INSERT INTO ${case_db}.t5 VALUES
(1, 't5_1'),
(3, 't5_3'),
(7, 't5_7'),
(NULL, 't5_null'),
(50, 't5_50');

-- query 22
-- @skip_result_check=true
INSERT INTO ${case_db}.t6 VALUES
(2, 't6_2'),
(5, 't6_5'),
(8, 't6_8'),
(NULL, 't6_null'),
(60, 't6_60');

-- query 23
-- @skip_result_check=true
INSERT INTO ${case_db}.t7 VALUES
(1, 't7_1'),
(4, 't7_4'),
(9, 't7_9'),
(NULL, 't7_null'),
(70, 't7_70');

-- query 24
-- @skip_result_check=true
INSERT INTO ${case_db}.t8 VALUES
(2, 't8_2'),
(6, 't8_6'),
(3, 't8_3'),
(NULL, 't8_null'),
(80, 't8_80');

-- query 25
-- FULL OUTER with inline scalar subquery
-- @order_sensitive=true
SELECT id, v1
FROM ${case_db}.t1 FULL OUTER JOIN (SELECT 1 AS id) s1 USING(id)
ORDER BY id, v1;

-- query 26
-- FULL OUTER chain with two inline scalar subqueries
-- @order_sensitive=true
SELECT id, v1, v2
FROM ${case_db}.t1 FULL OUTER JOIN (SELECT 1 AS id) s1 USING(id)
        FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN (SELECT 9 AS id) s2 USING(id)
ORDER BY id, v1, v2;

-- query 27
-- 6-table FULL OUTER chain (all FULL OUTER)
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 100;

-- query 28
-- 6-table mixed: FULL OUTER + INNER alternating
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        INNER JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
WHERE id > 0
ORDER BY id;

-- query 29
-- 6-table mixed: INNER + FULL OUTER + LEFT + FULL OUTER + RIGHT
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        LEFT JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        RIGHT JOIN ${case_db}.t6 USING(id)
WHERE id > 0
ORDER BY id;

-- query 30
-- 6-table mixed: LEFT + FULL OUTER + INNER + FULL OUTER + LEFT
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6
FROM ${case_db}.t1 LEFT JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        INNER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        LEFT JOIN ${case_db}.t6 USING(id)
WHERE id IS NOT NULL
ORDER BY id;

-- query 31
-- 7-table FULL OUTER chain
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
        FULL OUTER JOIN ${case_db}.t7 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 100;

-- query 32
-- 7-table all INNER (empty result expected - no id present in all)
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        INNER JOIN ${case_db}.t4 USING(id)
        INNER JOIN ${case_db}.t5 USING(id)
        INNER JOIN ${case_db}.t6 USING(id)
        INNER JOIN ${case_db}.t7 USING(id)
WHERE id > 0
ORDER BY id;

-- query 33
-- 7-table mixed: FULL OUTER + INNER alternating (single match)
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        INNER JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
        INNER JOIN ${case_db}.t7 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;

-- query 34
-- 7-table mixed: LEFT + FULL OUTER + INNER + FULL OUTER + LEFT + FULL OUTER
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7
FROM ${case_db}.t1 LEFT JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        INNER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        LEFT JOIN ${case_db}.t6 USING(id)
        FULL OUTER JOIN ${case_db}.t7 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;

-- query 35
-- 8-table FULL OUTER chain + table_count expression
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8,
       CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v2 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v4 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v5 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v6 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v7 IS NOT NULL THEN 1 ELSE 0 END +
       CASE WHEN v8 IS NOT NULL THEN 1 ELSE 0 END as table_count
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
        FULL OUTER JOIN ${case_db}.t7 USING(id)
        FULL OUTER JOIN ${case_db}.t8 USING(id)
WHERE id > 0 AND id <= 10
ORDER BY id
LIMIT 100;

-- query 36
-- 8-table all INNER (empty result)
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        INNER JOIN ${case_db}.t4 USING(id)
        INNER JOIN ${case_db}.t5 USING(id)
        INNER JOIN ${case_db}.t6 USING(id)
        INNER JOIN ${case_db}.t7 USING(id)
        INNER JOIN ${case_db}.t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;

-- query 37
-- 8-table mixed: FULL OUTER + INNER alternating
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        INNER JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
        INNER JOIN ${case_db}.t7 USING(id)
        FULL OUTER JOIN ${case_db}.t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;

-- query 38
-- 8-table mixed: INNER + FULL OUTER + LEFT + FULL OUTER + INNER + FULL OUTER + RIGHT
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        LEFT JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        INNER JOIN ${case_db}.t6 USING(id)
        FULL OUTER JOIN ${case_db}.t7 USING(id)
        RIGHT JOIN ${case_db}.t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 100;

-- query 39
-- 8-table mixed: FULL OUTER + LEFT + INNER + FULL OUTER + LEFT + FULL OUTER + INNER
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        LEFT JOIN ${case_db}.t3 USING(id)
        INNER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        LEFT JOIN ${case_db}.t6 USING(id)
        FULL OUTER JOIN ${case_db}.t7 USING(id)
        INNER JOIN ${case_db}.t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 100;

-- query 40
-- 6-table mixed: aggregation COUNT per source
-- @order_sensitive=true
SELECT id,
       COUNT(*) as row_count,
       COUNT(v1) as t1_count,
       COUNT(v2) as t2_count,
       COUNT(v3) as t3_count,
       COUNT(v4) as t4_count,
       COUNT(v5) as t5_count,
       COUNT(v6) as t6_count
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        LEFT JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
WHERE id > 0
GROUP BY id
ORDER BY id;

-- query 41
-- 6-table mixed: CASE status + COALESCE first value
-- @order_sensitive=true
SELECT id,
       CASE WHEN v1 IS NOT NULL THEN 'HAS_V1' ELSE 'NO_V1' END as v1_status,
       CASE WHEN v2 IS NOT NULL THEN 'HAS_V2' ELSE 'NO_V2' END as v2_status,
       CASE WHEN v3 IS NOT NULL THEN 'HAS_V3' ELSE 'NO_V3' END as v3_status,
       COALESCE(v1, v2, v3, v4, v5, 'NONE') as first_valuea
FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        LEFT JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        INNER JOIN ${case_db}.t6 USING(id)
        FULL OUTER JOIN ${case_db}.t7 USING(id)
WHERE id > 0
ORDER BY id;

-- query 42
-- 8-table mixed: expression columns + BETWEEN filter
-- @order_sensitive=true
SELECT id,
       id * 2 as id_double,
       id + 100 as id_offset,
       CONCAT('ID_', CAST(id AS STRING)) as id_str,
       v1, v2, v3, v4, v5
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        LEFT JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
        INNER JOIN ${case_db}.t7 USING(id)
        FULL OUTER JOIN ${case_db}.t8 USING(id)
WHERE id BETWEEN 1 AND 10
ORDER BY id;

-- query 43
-- 6-table mixed: aggregation with GROUP BY + HAVING
-- @order_sensitive=true
SELECT id,
       COUNT(*) as cnt,
       MAX(v1) as max_v1,
       MIN(v2) as min_v2
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        LEFT JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
WHERE id > 0
GROUP BY id
HAVING COUNT(*) > 0
ORDER BY id;

-- query 44
-- 8-table mixed: qualified table names
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4, v5, v6, v7, v8
FROM ${case_db}.t1
        FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        LEFT JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
        INNER JOIN ${case_db}.t7 USING(id)
        FULL OUTER JOIN ${case_db}.t8 USING(id)
WHERE id > 0
ORDER BY id
LIMIT 50;

-- query 45
-- CTE with 8-table join + aggregation + window
-- @order_sensitive=true
WITH base_join AS (
    SELECT id AS join_key,
           v1 AS val_from_t1,
           v2 AS val_from_t2,
           v3 AS val_from_t3,
           v4 AS val_from_t4,
           v5 AS val_from_t5,
           v6 AS val_from_t6,
           v7 AS val_from_t7,
           v8 AS val_from_t8
    FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
            INNER JOIN ${case_db}.t3 USING(id)
            FULL OUTER JOIN ${case_db}.t4 USING(id)
            LEFT JOIN ${case_db}.t5 USING(id)
            FULL OUTER JOIN ${case_db}.t6 USING(id)
            INNER JOIN ${case_db}.t7 USING(id)
            FULL OUTER JOIN ${case_db}.t8 USING(id)
    WHERE id > 0
),
aggregated AS (
    SELECT join_key,
           CONCAT(COALESCE(val_from_t1, ''), '-', COALESCE(val_from_t2, '')) AS combined_val,
           CASE WHEN val_from_t3 IS NOT NULL THEN 'HAS_T3' ELSE 'NO_T3' END AS t3_status,
           CASE WHEN val_from_t5 IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN val_from_t6 IS NOT NULL THEN 1 ELSE 0 END +
           CASE WHEN val_from_t7 IS NOT NULL THEN 1 ELSE 0 END AS table_count
    FROM base_join
    WHERE join_key BETWEEN 1 AND 10
)
SELECT join_key AS final_id,
       combined_val AS final_combined,
       t3_status AS final_status,
       table_count AS final_count
FROM aggregated
WHERE table_count > 0
ORDER BY final_id, final_combined
LIMIT 50;

-- query 46
-- 6-table mixed: CASE join_result + COALESCE first_non_null
-- @order_sensitive=true
SELECT id AS key_id,
       CONCAT('T1:', COALESCE(v1, 'NULL')) AS t1_value,
       CONCAT('T2:', COALESCE(v2, 'NULL')) AS t2_value,
       CONCAT('T3:', COALESCE(v3, 'NULL')) AS t3_value,
       CASE
           WHEN v4 IS NOT NULL AND v5 IS NOT NULL THEN 'BOTH'
           WHEN v4 IS NOT NULL THEN 'T4_ONLY'
           WHEN v5 IS NOT NULL THEN 'T5_ONLY'
           ELSE 'NEITHER'
       END AS join_result,
       COALESCE(v1, v2, v3, v4, v5, v6, 'NONE') AS first_non_null
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        LEFT JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
WHERE id IS NOT NULL
ORDER BY key_id, join_result
LIMIT 50;

-- query 47
-- 3-table FULL OUTER + IN subquery + AND filter
-- @order_sensitive=true
SELECT id, v1, v2, v3
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
WHERE id IN (
    SELECT id
    FROM ${case_db}.t5
    WHERE id IS NOT NULL
    GROUP BY id
    HAVING COUNT(*) > 0
)
  AND (v1 IS NOT NULL OR v2 IS NOT NULL OR v3 IS NOT NULL)
ORDER BY id
LIMIT 50;

-- query 48
-- CROSS JOIN with multi-table FULL OUTER subqueries
-- @order_sensitive=true
SELECT main.id,
       main.v1,
       main.v2,
       stats.total_matches,
       stats.avg_len
FROM (
    SELECT id, v1, v2
    FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
    WHERE id > 0
) main
CROSS JOIN (
    SELECT COUNT(*) AS total_matches,
           AVG(LENGTH(COALESCE(v3, v4, ''))) AS avg_len
    FROM ${case_db}.t3 FULL OUTER JOIN ${case_db}.t4 USING(id)
    WHERE id > 0
) stats
WHERE main.id IN (
    SELECT id
    FROM ${case_db}.t5 FULL OUTER JOIN ${case_db}.t6 USING(id)
    WHERE id IS NOT NULL
    GROUP BY id
    HAVING COUNT(*) > 0
)
ORDER BY main.id
LIMIT 50;

-- query 49
-- 4-table FULL OUTER + complex OR WHERE filter
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
WHERE (id > 0 AND id < 10)
   OR (v1 IS NOT NULL AND v2 IS NOT NULL)
   OR (v3 LIKE 't3%' AND v4 LIKE 't4%')
ORDER BY id
LIMIT 50;

-- query 50
-- CTE + 6-table FULL OUTER + aggregation
-- @order_sensitive=true
WITH join_result AS (
    SELECT id, v1, v2, v3, v4, v5, v6
    FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
            FULL OUTER JOIN ${case_db}.t3 USING(id)
            FULL OUTER JOIN ${case_db}.t4 USING(id)
            FULL OUTER JOIN ${case_db}.t5 USING(id)
            FULL OUTER JOIN ${case_db}.t6 USING(id)
    WHERE id IS NOT NULL
)
SELECT id,
       COUNT(*) AS row_count,
       MAX(CONCAT(COALESCE(v1, ''), COALESCE(v2, ''))) AS concat_result,
       SUM(CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END) AS v3_non_null
FROM join_result
GROUP BY id
HAVING COUNT(*) > 0
ORDER BY id
LIMIT 50;

-- query 51
-- DISTINCT + 5-table FULL OUTER + BETWEEN + value filter
-- @order_sensitive=true
SELECT DISTINCT id, v1, v3, v5
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
WHERE id BETWEEN 1 AND 20
  AND (v1 IS NOT NULL OR v3 IS NOT NULL OR v5 IS NOT NULL)
ORDER BY id, v1, v3
LIMIT 50;

-- query 52
-- 2-table FULL OUTER + presence status + COALESCE defaults
-- @order_sensitive=true
SELECT id,
       CASE
           WHEN v1 IS NOT NULL AND v2 IS NOT NULL THEN 'BOTH'
           WHEN v1 IS NOT NULL THEN 'ONLY_T1'
           WHEN v2 IS NOT NULL THEN 'ONLY_T2'
           ELSE 'NEITHER'
       END AS presence_status,
       COALESCE(v1, 'DEFAULT_1') AS v1_with_default,
       COALESCE(v2, 'DEFAULT_2') AS v2_with_default
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
WHERE id > 0
ORDER BY id, presence_status
LIMIT 50;

-- query 53
-- 4-table FULL OUTER + IN + NOT IN subquery filter
-- @order_sensitive=true
SELECT id, v1, v2, v3, v4
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
WHERE id IN (SELECT id FROM ${case_db}.t5 WHERE id > 0)
  AND id NOT IN (SELECT id FROM ${case_db}.t6 WHERE id > 10 AND id IS NOT NULL)
  AND (v1 IS NOT NULL OR v2 IS NOT NULL)
ORDER BY id
LIMIT 50;

-- query 54
-- 7-table FULL OUTER: COALESCE combined values + first_available
-- @order_sensitive=true
SELECT id,
       CONCAT(COALESCE(v1, 'NULL'), '-', COALESCE(v2, 'NULL')) AS combined_1_2,
       CONCAT(COALESCE(v3, 'NULL'), '-', COALESCE(v4, 'NULL')) AS combined_3_4,
       COALESCE(v5, v6, v7, 'NO_VALUE') AS first_available
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        FULL OUTER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        FULL OUTER JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
        FULL OUTER JOIN ${case_db}.t7 USING(id)
WHERE id > 0
  AND (v1 IS NOT NULL OR v2 IS NOT NULL OR v3 IS NOT NULL)
ORDER BY id
LIMIT 50;

-- query 55
-- Inline VALUES + persistent tables: FULL OUTER chain
-- @order_sensitive=true
SELECT id, val, v1, v2
FROM (VALUES (1, 'val1'), (2, 'val2'), (3, 'val3'), (NULL, 'valN')) AS vals(id, val)
     FULL OUTER JOIN ${case_db}.t1 USING(id)
     FULL OUTER JOIN ${case_db}.t2 USING(id)
WHERE id IS NOT NULL OR val IS NOT NULL
ORDER BY id
LIMIT 50;

-- query 56
-- Two inline VALUES + three persistent tables: FULL OUTER chain
-- @order_sensitive=true
SELECT id, val1, val2, v1, v2, v3
FROM (VALUES (1, 'A'), (2, 'B'), (5, 'E'), (NULL, 'N1')) AS v1(id, val1)
     FULL OUTER JOIN (VALUES (1, 'X'), (3, 'Y'), (5, 'Z'), (NULL, 'N2')) AS v2(id, val2) USING(id)
     FULL OUTER JOIN ${case_db}.t1 USING(id)
     FULL OUTER JOIN ${case_db}.t2 USING(id)
     FULL OUTER JOIN ${case_db}.t3 USING(id)
WHERE id > 0 OR val1 IS NOT NULL OR val2 IS NOT NULL
ORDER BY id
LIMIT 50;

-- query 57
-- CTE self-join FULL OUTER + INNER + LEFT
-- @order_sensitive=true
WITH t1_early AS (
    SELECT id, v1 AS early_value
    FROM ${case_db}.t1
    WHERE id <= 5
),
t1_late AS (
    SELECT id, v1 AS late_value
    FROM ${case_db}.t1
    WHERE id >= 3
)
SELECT id,
       early_value,
       late_value,
       t2.v2,
       t3.v3,
       CASE
           WHEN early_value IS NOT NULL AND late_value IS NOT NULL THEN 'OVERLAP'
           WHEN early_value IS NOT NULL THEN 'EARLY_ONLY'
           WHEN late_value IS NOT NULL THEN 'LATE_ONLY'
           ELSE 'NEITHER'
       END AS period_status
FROM t1_early
     FULL OUTER JOIN t1_late USING(id)
     INNER JOIN ${case_db}.t2 USING(id)
     LEFT JOIN ${case_db}.t3 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;

-- query 58
-- Nested CTE: base branches + combined + center data
-- @order_sensitive=true
WITH base_left AS (
    SELECT id, v1, v2
    FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(id)
    WHERE id > 0
),
base_right AS (
    SELECT id, v3, v4
    FROM ${case_db}.t3 LEFT JOIN ${case_db}.t4 USING(id)
    WHERE id > 0
),
combined_branches AS (
    SELECT id, bl.v1, bl.v2, br.v3, br.v4
    FROM base_left bl
         FULL OUTER JOIN base_right br USING(id)
),
with_center AS (
    SELECT id, cb.v1, cb.v2, cb.v3, cb.v4, t5.v5, t6.v6
    FROM combined_branches cb
         FULL OUTER JOIN ${case_db}.t5 USING(id)
         LEFT JOIN ${case_db}.t6 USING(id)
)
SELECT id,
       v1, v2, v3, v4, v5, v6,
       CASE
           WHEN v1 IS NOT NULL AND v3 IS NOT NULL THEN 'BOTH_SIDES'
           WHEN v1 IS NOT NULL THEN 'LEFT_ONLY'
           WHEN v3 IS NOT NULL THEN 'RIGHT_ONLY'
           ELSE 'CENTER_ONLY'
       END AS data_source
FROM with_center
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;

-- query 59
-- Nested CTE: multi-level + window functions
-- @order_sensitive=true
WITH base_data AS (
    SELECT id, v1, v2, v3, v4, v5
    FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
            INNER JOIN ${case_db}.t3 USING(id)
            LEFT JOIN ${case_db}.t4 USING(id)
            LEFT JOIN ${case_db}.t5 USING(id)
)
SELECT id,
       v1, v2, v3, v4, v5,
       COUNT(*) OVER (PARTITION BY
           CASE WHEN id <= 3 THEN 'LOW' WHEN id <= 6 THEN 'MID' ELSE 'HIGH' END
       ) AS range_count,
       ROW_NUMBER() OVER (
           PARTITION BY
               CASE WHEN v1 IS NOT NULL THEN 1 ELSE 0 END
           ORDER BY id
       ) AS rn_by_v1_presence
FROM base_data
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;

-- query 60
-- 6-table mixed: reordered SELECT columns (v3 first, id in middle)
-- @order_sensitive=true
SELECT v1, v2, id, v3, v4, v5, v6
FROM ${case_db}.t1 FULL OUTER JOIN ${case_db}.t2 USING(id)
        INNER JOIN ${case_db}.t3 USING(id)
        FULL OUTER JOIN ${case_db}.t4 USING(id)
        LEFT JOIN ${case_db}.t5 USING(id)
        FULL OUTER JOIN ${case_db}.t6 USING(id)
WHERE id IS NOT NULL
ORDER BY id
LIMIT 50;

-- query 61
-- FE error: agg alias used without re-alias in outer FULL OUTER (column resolution)
-- @expect_error=cannot be resolved
WITH agg_level1 AS (
    SELECT id,
           COUNT(*) AS l1_count,
           MAX(v1) AS l1_max_v1,
           MIN(v2) AS l1_min_v2,
           SUM(CASE WHEN v3 IS NOT NULL THEN 1 ELSE 0 END) AS l1_v3_count
    FROM ${case_db}.t1 INNER JOIN ${case_db}.t2 USING(id)
            FULL OUTER JOIN ${case_db}.t3 USING(id)
            LEFT JOIN ${case_db}.t4 USING(id)
    WHERE id > 0
    GROUP BY id
)
SELECT agg.id,
       agg.l1_count,
       agg.l1_max_v1,
       agg.l1_min_v2,
       agg.l1_v3_count,
       t5.v5,
       t6.v6,
       CASE
           WHEN agg.l1_v3_count > 2 THEN 'HIGH'
           WHEN agg.l1_v3_count > 0 THEN 'MEDIUM'
           ELSE 'LOW'
       END AS v3_presence_level
FROM agg_level1 agg
     LEFT JOIN ${case_db}.t5 USING(id)
     FULL OUTER JOIN ${case_db}.t6 USING(id)
WHERE agg.id IS NOT NULL
ORDER BY agg.l1_count DESC, agg.id
LIMIT 50;
