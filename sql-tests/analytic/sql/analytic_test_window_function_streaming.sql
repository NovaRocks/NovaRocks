-- Migrated from: dev/test/sql/test_window_function/T/test_window_function_streaming
-- Test Objective:
-- 1. Validate streaming execution of row_number, rank, dense_rank, count, min, max, sum, avg window functions
-- 2. Test cume_dist and percent_rank on large datasets (>10k rows) with partition-based filtering
-- 3. Verify correctness with partition key filtering (rk <= 1 and rk > 1 subsets)

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  `v1` int(11) NULL,
  `v2` int(11) NULL,
  `v3` int(11) NOT NULL,
  `v4` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);

INSERT INTO ${case_db}.t1 (v1, v2, v3, v4) values
    (1, 1, 1, NULL),
    (1, 1, 2, NULL),
    (1, NULL, 3, NULL),
    (1, NULL, 4, NULL),
    (1, 2, 5, NULL),
    (1, 2, 6, NULL),
    (1, NULL, 7, NULL),
    (1, NULL, 8, NULL),
    (2, 3, 9, NULL),
    (2, 3, 10, NULL),
    (2, NULL, 11, NULL),
    (2, NULL, 12, NULL),
    (2, 4, 13, NULL),
    (2, 4, 14, NULL),
    (2, NULL, 15, NULL),
    (2, NULL, 16, NULL),
    (NULL, 3, 17, NULL),
    (NULL, 3, 18, NULL),
    (NULL, NULL, 19, NULL),
    (NULL, NULL, 20, NULL),
    (NULL, 4, 21, NULL),
    (NULL, 4, 22, NULL),
    (NULL, NULL, 23, NULL),
    (NULL, NULL, 24, NULL);

INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;

INSERT INTO ${case_db}.t1 (v1, v2, v3, v4) values
    (101, 101, 101, NULL),
    (101, 101, 102, NULL),
    (101, NULL, 103, NULL),
    (101, NULL, 104, NULL),
    (101, 102, 105, NULL),
    (101, 102, 106, NULL),
    (101, NULL, 107, NULL),
    (101, NULL, 108, NULL),
    (102, 103, 109, NULL),
    (102, 103, 110, NULL),
    (102, NULL, 111, NULL),
    (102, NULL, 112, NULL),
    (102, 104, 113, NULL),
    (102, 104, 114, NULL),
    (102, NULL, 115, NULL),
    (102, NULL, 116, NULL),
    (NULL, 103, 117, NULL),
    (NULL, 103, 118, NULL),
    (NULL, NULL, 119, NULL),
    (NULL, NULL, 120, NULL),
    (NULL, 104, 121, NULL),
    (NULL, 104, 122, NULL),
    (NULL, NULL, 123, NULL),
    (NULL, NULL, 124, NULL);

-- query 2
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT row_number() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a WHERE rk <= 1;

-- query 3
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT row_number() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a WHERE rk > 1;

-- query 4
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT row_number() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a;

-- query 5
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT rank() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a WHERE rk <= 1;

-- query 6
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT rank() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a WHERE rk > 1;

-- query 7
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT rank() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a;

-- query 8
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT dense_rank() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a WHERE rk <= 1;

-- query 9
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT dense_rank() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a WHERE rk > 1;

-- query 10
SELECT COUNT(*), SUM(rk), MIN(rk), MAX(rk) FROM (SELECT dense_rank() OVER (PARTITION BY v2 ORDER BY v1) AS rk FROM ${case_db}.t1)a;

-- query 11
SELECT COUNT(*), SUM(wv), MIN(wv), MAX(wv) FROM (SELECT count(v3) OVER (PARTITION BY v2 ORDER BY v1) AS wv FROM ${case_db}.t1)a;

-- query 12
SELECT COUNT(*), SUM(wv), MIN(wv), MAX(wv) FROM (SELECT min(v3) OVER (PARTITION BY v2 ORDER BY v1) AS wv FROM ${case_db}.t1)a;

-- query 13
SELECT COUNT(*), SUM(wv), MIN(wv), MAX(wv) FROM (SELECT max(v3) OVER (PARTITION BY v2 ORDER BY v1) AS wv FROM ${case_db}.t1)a;

-- query 14
SELECT COUNT(*), SUM(wv), MIN(wv), MAX(wv) FROM (SELECT sum(v3) OVER (PARTITION BY v2 ORDER BY v1) AS wv FROM ${case_db}.t1)a;

-- query 15
SELECT COUNT(*), SUM(wv), MIN(wv), MAX(wv) FROM (SELECT avg(v3) OVER (PARTITION BY v2 ORDER BY v1) AS wv FROM ${case_db}.t1)a;

-- query 16
SELECT /*+ SET_VAR(pipeline_dop='1')*/ COUNT(*), SUM(wv), MIN(wv), MAX(wv) FROM (SELECT CAST(cume_dist() OVER (PARTITION BY v2 ORDER BY v1) AS decimal64(18,5)) AS wv FROM ${case_db}.t1)a;

-- query 17
SELECT /*+ SET_VAR(pipeline_dop='1')*/ COUNT(*), SUM(wv), MIN(wv), MAX(wv) FROM (SELECT CAST(percent_rank() OVER (PARTITION BY v2 ORDER BY v1) AS decimal64(18,5)) AS wv FROM ${case_db}.t1)a;
