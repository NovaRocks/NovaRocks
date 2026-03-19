-- Migrated from: dev/test/sql/test_window_function/T/test_window_function_streaming
-- Test Objective:
-- 1. Validate lead/lag window functions on large decimal64 datasets with and without partitioning
-- 2. Validate SUM with half-bounded ROWS frames (UNBOUNDED PRECEDING AND N PRECEDING/FOLLOWING)
-- 3. Test both global and partition-scoped half-bounded frame behavior

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

INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t1;

CREATE TABLE ${case_db}.t5 (
  `v1` decimal64(18,5) NULL,
  `v2` decimal64(18,5) NULL,
  `v3` decimal64(18,5) NOT NULL,
  `v4` decimal64(18,5) NOT NULL,
  `v5` decimal64(18,5) NOT NULL,
  `v6` decimal64(18,5) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);

INSERT INTO ${case_db}.t5 (v1, v2, v3, v4, v5, v6)
SELECT v1, v2, v3,
row_number() OVER (ORDER BY v1, v2, v3, v4) as v4,
row_number() OVER (PARTITION BY v1 ORDER BY v2, v3, v4) as v5,
1 as v6
FROM ${case_db}.t1;

-- query 2
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lead(v3, 1) OVER(ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 3
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lead(v3, 10) OVER(ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 4
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lag(v3, 1) OVER(ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 5
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lag(v3, 10) OVER(ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 6
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lead(v3, 1) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 7
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lead(v3, 10) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 8
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lag(v3, 1) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 9
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT lag(v3, 10) OVER(PARTITION BY v2 ORDER BY v4) AS wv FROM ${case_db}.t5) a;

-- query 10
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) AS wv FROM ${case_db}.t5) a;

-- query 11
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS wv FROM ${case_db}.t5) a;

-- query 12
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) AS wv FROM ${case_db}.t5) a;

-- query 13
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS wv FROM ${case_db}.t5) a;

-- query 14
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(PARTITION BY v2 ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 PRECEDING) AS wv FROM ${case_db}.t5) a;

-- query 15
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(PARTITION BY v2 ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS wv FROM ${case_db}.t5) a;

-- query 16
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(PARTITION BY v2 ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 10 FOLLOWING) AS wv FROM ${case_db}.t5) a;

-- query 17
SELECT SUM(wv), AVG(wv), MIN(wv), MAX(wv) FROM (SELECT SUM(v3) OVER(PARTITION BY v2 ORDER BY v4 ROWS BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING) AS wv FROM ${case_db}.t5) a;
