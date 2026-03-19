-- @order_sensitive=true
-- @tags=join,nljoin,non_equi,runtime_filter
-- Test Objective:
-- 1. Validate non-equality nested-loop join semantics across broadcast and shuffle paths.
-- 2. Cover empty-side handling for inner, outer, semi, and anti joins.
-- 3. Preserve chunk-boundary regressions with asymmetric large inputs.
-- 4. Keep runtime-filter correctness checks on scan and exchange paths.

-- query 1
DROP TABLE IF EXISTS ${case_db}.t1;
DROP TABLE IF EXISTS ${case_db}.t2;
DROP TABLE IF EXISTS ${case_db}.t3;
DROP TABLE IF EXISTS ${case_db}.t4;

CREATE TABLE ${case_db}.t1 (
  c1 INT NULL,
  c2 INT NULL
)
ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE ${case_db}.t2 (
  c1 INT NULL,
  c2 INT NULL
)
ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE ${case_db}.t3 (
  c1 INT NULL,
  c2 INT NULL
)
ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

CREATE TABLE ${case_db}.t4 (
  c1 INT NULL,
  c2 INT NULL
)
ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES (
  "replication_num" = "1"
);

INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 5));

INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 2));

INSERT INTO ${case_db}.t3
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 9));

INSERT INTO ${case_db}.t4
SELECT generate_series, generate_series
FROM TABLE(generate_series(2, 8));

-- Base semantics across all non-equality NL join types.
SELECT scenario, row_count, left_non_null, right_non_null, sum_left, sum_right
FROM (
  SELECT '01_inner' AS scenario,
         COUNT(*) AS row_count,
         COUNT(t1.c2) AS left_non_null,
         COUNT(t2.c2) AS right_non_null,
         COALESCE(SUM(t1.c2), 0) AS sum_left,
         COALESCE(SUM(t2.c2), 0) AS sum_right
  FROM ${case_db}.t1
  JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
  UNION ALL
  SELECT '02_left_outer',
         COUNT(*),
         COUNT(t1.c2),
         COUNT(t2.c2),
         COALESCE(SUM(t1.c2), 0),
         COALESCE(SUM(t2.c2), 0)
  FROM ${case_db}.t1
  LEFT JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
  UNION ALL
  SELECT '03_left_semi',
         COUNT(*),
         COUNT(t1.c2),
         0,
         COALESCE(SUM(t1.c2), 0),
         0
  FROM ${case_db}.t1
  LEFT SEMI JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
  UNION ALL
  SELECT '04_left_anti',
         COUNT(*),
         COUNT(t1.c2),
         0,
         COALESCE(SUM(t1.c2), 0),
         0
  FROM ${case_db}.t1
  LEFT ANTI JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
  UNION ALL
  SELECT '05_right_outer',
         COUNT(*),
         COUNT(t1.c2),
         COUNT(t3.c2),
         COALESCE(SUM(t1.c2), 0),
         COALESCE(SUM(t3.c2), 0)
  FROM ${case_db}.t1
  RIGHT OUTER JOIN [shuffle] ${case_db}.t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '06_full_outer',
         COUNT(*),
         COUNT(t1.c2),
         COUNT(t4.c2),
         COALESCE(SUM(t1.c2), 0),
         COALESCE(SUM(t4.c2), 0)
  FROM ${case_db}.t1
  FULL OUTER JOIN [shuffle] ${case_db}.t4 ON t1.c1 > t4.c1
  UNION ALL
  SELECT '07_right_semi',
         COUNT(*),
         0,
         COUNT(t3.c2),
         0,
         COALESCE(SUM(t3.c2), 0)
  FROM ${case_db}.t1
  RIGHT SEMI JOIN [shuffle] ${case_db}.t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '08_right_anti',
         COUNT(*),
         0,
         COUNT(t4.c2),
         0,
         COALESCE(SUM(t4.c2), 0)
  FROM ${case_db}.t1
  RIGHT ANTI JOIN [shuffle] ${case_db}.t4 ON t1.c1 > t4.c1
) s
ORDER BY scenario;

-- query 2
-- Right input empty.
SELECT scenario, row_count, left_non_null, right_non_null, sum_left, sum_right
FROM (
  SELECT '01_inner' AS scenario,
         COUNT(*) AS row_count,
         COUNT(t1.c2) AS left_non_null,
         COUNT(t3.c2) AS right_non_null,
         COALESCE(SUM(t1.c2), 0) AS sum_left,
         COALESCE(SUM(t3.c2), 0) AS sum_right
  FROM ${case_db}.t1
  JOIN [broadcast] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '02_left_outer',
         COUNT(*),
         COUNT(t1.c2),
         COUNT(t3.c2),
         COALESCE(SUM(t1.c2), 0),
         COALESCE(SUM(t3.c2), 0)
  FROM ${case_db}.t1
  LEFT JOIN [broadcast] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '03_left_semi',
         COUNT(*),
         COUNT(t1.c2),
         0,
         COALESCE(SUM(t1.c2), 0),
         0
  FROM ${case_db}.t1
  LEFT SEMI JOIN [broadcast] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '04_left_anti',
         COUNT(*),
         COUNT(t1.c2),
         0,
         COALESCE(SUM(t1.c2), 0),
         0
  FROM ${case_db}.t1
  LEFT ANTI JOIN [broadcast] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '05_right_outer',
         COUNT(*),
         COUNT(t1.c2),
         COUNT(t3.c2),
         COALESCE(SUM(t1.c2), 0),
         COALESCE(SUM(t3.c2), 0)
  FROM ${case_db}.t1
  RIGHT OUTER JOIN [shuffle] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '06_full_outer',
         COUNT(*),
         COUNT(t1.c2),
         COUNT(t3.c2),
         COALESCE(SUM(t1.c2), 0),
         COALESCE(SUM(t3.c2), 0)
  FROM ${case_db}.t1
  FULL OUTER JOIN [shuffle] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '07_right_semi',
         COUNT(*),
         0,
         COUNT(t3.c2),
         0,
         COALESCE(SUM(t3.c2), 0)
  FROM ${case_db}.t1
  RIGHT SEMI JOIN [shuffle] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
  UNION ALL
  SELECT '08_right_anti',
         COUNT(*),
         0,
         COUNT(t3.c2),
         0,
         COALESCE(SUM(t3.c2), 0)
  FROM ${case_db}.t1
  RIGHT ANTI JOIN [shuffle] (SELECT * FROM ${case_db}.t2 WHERE c1 < 1) t3 ON t1.c1 > t3.c1
) s
ORDER BY scenario;

-- query 3
-- Left input empty.
SELECT scenario, row_count, left_non_null, right_non_null, sum_left, sum_right
FROM (
  SELECT '01_inner' AS scenario,
         COUNT(*) AS row_count,
         COUNT(t3.c2) AS left_non_null,
         COUNT(t2.c2) AS right_non_null,
         COALESCE(SUM(t3.c2), 0) AS sum_left,
         COALESCE(SUM(t2.c2), 0) AS sum_right
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  JOIN [broadcast] ${case_db}.t2 ON t3.c1 > t2.c1
  UNION ALL
  SELECT '02_left_outer',
         COUNT(*),
         COUNT(t3.c2),
         COUNT(t2.c2),
         COALESCE(SUM(t3.c2), 0),
         COALESCE(SUM(t2.c2), 0)
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  LEFT JOIN [broadcast] ${case_db}.t2 ON t3.c1 > t2.c1
  UNION ALL
  SELECT '03_left_semi',
         COUNT(*),
         COUNT(t3.c2),
         0,
         COALESCE(SUM(t3.c2), 0),
         0
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  LEFT SEMI JOIN [broadcast] ${case_db}.t2 ON t3.c1 > t2.c1
  UNION ALL
  SELECT '04_left_anti',
         COUNT(*),
         COUNT(t3.c2),
         0,
         COALESCE(SUM(t3.c2), 0),
         0
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  LEFT ANTI JOIN [broadcast] ${case_db}.t2 ON t3.c1 > t2.c1
  UNION ALL
  SELECT '05_right_outer',
         COUNT(*),
         COUNT(t3.c2),
         COUNT(t2.c2),
         COALESCE(SUM(t3.c2), 0),
         COALESCE(SUM(t2.c2), 0)
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  RIGHT OUTER JOIN [shuffle] ${case_db}.t2 ON t3.c1 > t2.c1
  UNION ALL
  SELECT '06_full_outer',
         COUNT(*),
         COUNT(t3.c2),
         COUNT(t2.c2),
         COALESCE(SUM(t3.c2), 0),
         COALESCE(SUM(t2.c2), 0)
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  FULL OUTER JOIN [shuffle] ${case_db}.t2 ON t3.c1 > t2.c1
  UNION ALL
  SELECT '07_right_semi',
         COUNT(*),
         0,
         COUNT(t2.c2),
         0,
         COALESCE(SUM(t2.c2), 0)
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  RIGHT SEMI JOIN [shuffle] ${case_db}.t2 ON t3.c1 > t2.c1
  UNION ALL
  SELECT '08_right_anti',
         COUNT(*),
         0,
         COUNT(t2.c2),
         0,
         COALESCE(SUM(t2.c2), 0)
  FROM (SELECT * FROM ${case_db}.t1 WHERE c1 < 1) t3
  RIGHT ANTI JOIN [shuffle] ${case_db}.t2 ON t3.c1 > t2.c1
) s
ORDER BY scenario;

-- query 4
TRUNCATE TABLE ${case_db}.t1;
TRUNCATE TABLE ${case_db}.t2;
INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4098));
INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(2, 8));

SELECT 'left_large' AS scenario,
       COUNT(*) AS row_count,
       COALESCE(SUM(t1.c2), 0) AS sum_left,
       COALESCE(SUM(t2.c2), 0) AS sum_right
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1;

-- query 5
-- Sample the low-key edge without sorting the full large join result.
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 <= 5
ORDER BY t1.c2, t2.c2
LIMIT 10;

-- query 6
-- Sample the high-key edge to preserve descending tail coverage cheaply.
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 >= 4094
ORDER BY t1.c2 DESC, t2.c2 DESC
LIMIT 10;

-- query 7
TRUNCATE TABLE ${case_db}.t1;
TRUNCATE TABLE ${case_db}.t2;
INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(2, 8));
INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4098));

SELECT 'right_large' AS scenario,
       COUNT(*) AS row_count,
       COALESCE(SUM(t1.c2), 0) AS sum_left,
       COALESCE(SUM(t2.c2), 0) AS sum_right
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1;

-- query 8
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
ORDER BY t1.c2, t2.c2
LIMIT 10;

-- query 9
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
ORDER BY t1.c2 DESC, t2.c2 DESC
LIMIT 10;

-- query 10
TRUNCATE TABLE ${case_db}.t1;
TRUNCATE TABLE ${case_db}.t2;
INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4098));
INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4098));

SELECT 'multi_chunk_equal' AS scenario,
       COUNT(*) AS row_count,
       COALESCE(SUM(t1.c2), 0) AS sum_left,
       COALESCE(SUM(t2.c2), 0) AS sum_right
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1;

-- query 11
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 <= 5
ORDER BY t1.c2, t2.c2
LIMIT 10;

-- query 12
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 >= 4094
ORDER BY t1.c2 DESC, t2.c2 DESC
LIMIT 10;

-- query 13
TRUNCATE TABLE ${case_db}.t1;
TRUNCATE TABLE ${case_db}.t2;
INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4096));
INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4098));

SELECT 'multi_chunk_left_boundary' AS scenario,
       COUNT(*) AS row_count,
       COALESCE(SUM(t1.c2), 0) AS sum_left,
       COALESCE(SUM(t2.c2), 0) AS sum_right
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1;

-- query 14
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 <= 5
ORDER BY t1.c2, t2.c2
LIMIT 10;

-- query 15
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 >= 4092
ORDER BY t1.c2 DESC, t2.c2 DESC
LIMIT 10;

-- query 16
TRUNCATE TABLE ${case_db}.t1;
TRUNCATE TABLE ${case_db}.t2;
INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4098));
INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 4096));

SELECT 'multi_chunk_right_boundary' AS scenario,
       COUNT(*) AS row_count,
       COALESCE(SUM(t1.c2), 0) AS sum_left,
       COALESCE(SUM(t2.c2), 0) AS sum_right
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1;

-- query 17
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 <= 5
ORDER BY t1.c2, t2.c2
LIMIT 10;

-- query 18
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
WHERE t1.c1 >= 4094
ORDER BY t1.c2 DESC, t2.c2 DESC
LIMIT 10;

-- query 19
TRUNCATE TABLE ${case_db}.t1;
TRUNCATE TABLE ${case_db}.t2;
INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 77));
INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 133));

SELECT 'multi_small_chunk' AS scenario,
       COUNT(*) AS row_count,
       COALESCE(SUM(t1.c2), 0) AS sum_left,
       COALESCE(SUM(t2.c2), 0) AS sum_right
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1;

-- query 20
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
ORDER BY t1.c2, t2.c2
LIMIT 10;

-- query 21
SELECT t1.c2, t2.c2
FROM ${case_db}.t1
JOIN [broadcast] ${case_db}.t2 ON t1.c1 > t2.c1
ORDER BY t1.c2 DESC, t2.c2 DESC
LIMIT 10;

-- query 22
TRUNCATE TABLE ${case_db}.t1;
TRUNCATE TABLE ${case_db}.t2;
INSERT INTO ${case_db}.t1
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.t2
SELECT generate_series, generate_series
FROM TABLE(generate_series(1, 1));

-- Runtime filter on scan path should preserve the single surviving row.
SELECT t1.c1
FROM ${case_db}.t1
JOIN ${case_db}.t2 ON t1.c1 <= t2.c1;

-- query 23
-- Runtime filter on exchange path should preserve the two qualifying keys.
WITH l AS (
  SELECT c1 AS lk
  FROM ${case_db}.t1
),
r AS (
  SELECT c1 AS rk
  FROM ${case_db}.t1
),
dim AS (
  SELECT 50000 AS mx_lo_orderkey, 49999 AS mn_lo_orderkey
)
SELECT COUNT(*)
FROM l
JOIN [shuffle] r ON lk = rk
JOIN dim
WHERE rk >= mn_lo_orderkey AND rk <= mx_lo_orderkey;
