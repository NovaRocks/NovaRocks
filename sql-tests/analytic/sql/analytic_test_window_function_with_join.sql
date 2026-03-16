-- Migrated from: dev/test/sql/test_window_function/T/test_window_function_with_join
-- Test Objective:
-- 1. Validate max() window function with CTEs when join type is bucket, broadcast, shuffle, and colocate.
-- 2. Confirm result is consistent regardless of join hint.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.`nt0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"colocate_with" = "nt0",
"replication_num" = "1"
);

INSERT INTO ${case_db}.nt0 SELECT generate_series %4096, 4096 - generate_series, generate_series %4096 FROM TABLE(generate_series(1, 40960));
INSERT INTO ${case_db}.nt0 SELECT * FROM ${case_db}.nt0;

CREATE TABLE ${case_db}.nt1 AS SELECT * FROM ${case_db}.nt0;
CREATE TABLE ${case_db}.nt2 AS SELECT * FROM ${case_db}.nt0;

CREATE TABLE ${case_db}.`nt3` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`) BUCKETS 48
PROPERTIES (
"colocate_with" = "nt0",
"replication_num" = "1"
);

INSERT INTO ${case_db}.nt3 SELECT * FROM ${case_db}.nt0;

-- query 2
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN [bucket] ${case_db}.nt1 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;

-- query 3
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN [broadcast] ${case_db}.nt1 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;

-- query 4
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN [shuffle] ${case_db}.nt1 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;

-- query 5
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN [colocate] ${case_db}.nt3 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;
