-- Migrated from: dev/test/sql/test_window_function/T/test_ignore_nulls
-- Test Objective:
-- 1. Validate IGNORE NULLS for first_value/last_value over ORDER BY and PARTITION BY windows.
-- 2. Validate IGNORE NULLS for lead/lag with various offsets and partitions.
-- 3. Validate lead/lag IGNORE NULLS when all values in a partition are NULL.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.`t0` (
  `v1` int(11) NULL,
  `v2` int(11) NULL,
  `v3` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);

INSERT INTO ${case_db}.`t0` (v1, v2, v3) values
    (1, 1, 1),
    (1, 2, NULL),
    (1, 3, 3),
    (1, 4, NULL),
    (1, 5, 5),
    (1, 6, NULL),
    (2, 1, NULL),
    (2, 2, 2),
    (2, 3, NULL),
    (2, 4, 4),
    (2, 5, NULL),
    (2, 6, 6),
    (3, 1, 1),
    (3, 2, 2),
    (3, 3, NULL),
    (3, 4, NULL),
    (3, 5, 5),
    (3, 6, 6),
    (4, 1, NULL),
    (4, 2, NULL),
    (4, 3, 3),
    (4, 4, 4),
    (4, 5, NULL),
    (4, 6, NULL);

-- query 2
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 IGNORE NULLS) OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 3
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 IGNORE NULLS) OVER(ORDER BY v1, v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 4
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 IGNORE NULLS) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 5
-- @order_sensitive=true
SELECT v1, v2, v3, first_value(v3 IGNORE NULLS) OVER(partition BY v1 ORDER BY v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 6
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 IGNORE NULLS) OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 7
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 IGNORE NULLS) OVER(ORDER BY v1, v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 8
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 IGNORE NULLS) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 9
-- @order_sensitive=true
SELECT v1, v2, v3, last_value(v3 IGNORE NULLS) OVER(partition BY v1 ORDER BY v2 rows between 1 preceding and 1 following) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 10
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 IGNORE NULLS, 1) OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 11
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 IGNORE NULLS, 1) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 12
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 IGNORE NULLS, 2) OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 13
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 IGNORE NULLS, 2) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 14
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 IGNORE NULLS, 1) OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 15
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 IGNORE NULLS, 1) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 16
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 IGNORE NULLS, 2) OVER(ORDER BY v1, v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 17
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 IGNORE NULLS, 2) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t0 ORDER BY v1, v2;

-- query 18
-- @skip_result_check=true
CREATE TABLE ${case_db}.`t_all_null` (
  `v1` int(11) NULL,
  `v2` int(11) NULL,
  `v3` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);

INSERT INTO ${case_db}.`t_all_null` (v1, v2, v3) values
    (1, 1, NULL),
    (1, 2, NULL),
    (1, 3, NULL),
    (1, 4, NULL),
    (1, 5, NULL),
    (1, 6, NULL),
    (2, 1, NULL),
    (2, 2, NULL),
    (2, 3, NULL),
    (2, 4, NULL),
    (2, 5, NULL),
    (2, 6, NULL),
    (3, 1, NULL),
    (3, 2, NULL),
    (3, 3, NULL),
    (3, 4, NULL),
    (3, 5, NULL),
    (3, 6, NULL),
    (4, 1, NULL),
    (4, 2, NULL),
    (4, 3, NULL),
    (4, 4, NULL),
    (4, 5, NULL),
    (4, 6, NULL);

-- query 19
-- @order_sensitive=true
SELECT v1, v2, v3, lead(v3 IGNORE NULLS, 1) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t_all_null ORDER BY v1, v2;

-- query 20
-- @order_sensitive=true
SELECT v1, v2, v3, lag(v3 IGNORE NULLS, 1) OVER(partition BY v1 ORDER BY v2) FROM ${case_db}.t_all_null ORDER BY v1, v2;
