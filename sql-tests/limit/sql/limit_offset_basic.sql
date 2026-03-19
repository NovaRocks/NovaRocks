-- Migrated from: dev/test/sql/test_limit/T/test_limit
-- Test Objective:
-- 1. Validate LIMIT offset,count correctness on a 33-row table across boundary cases
--    (within range, partial range, beyond range, nested limits).
-- 2. Validate LIMIT on generate_series with large offsets.
-- 3. Validate CTE + LIMIT correctness and EXPLAIN plan shape under
--    enable_multi_cast_limit_push_down=false/true.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
  region     VARCHAR(128) NOT NULL,
  order_date DATE         NOT NULL,
  income     DECIMAL(7, 0) NOT NULL,
  ship_mode  INT          NOT NULL,
  ship_code  INT
) ENGINE=OLAP
DUPLICATE KEY(region, order_date)
DISTRIBUTED BY HASH(region, order_date) BUCKETS 10
PROPERTIES (
  "replication_num"          = "1",
  "enable_persistent_index"  = "true",
  "replicated_storage"       = "false",
  "compression"              = "LZ4"
);

INSERT INTO ${case_db}.t0 (region, order_date, income, ship_mode, ship_code) VALUES
  ('USA',    '2022-01-01', 12345, 50,  1),
  ('CHINA',  '2022-01-02', 54321, 51,  4),
  ('JAPAN',  '2022-01-03', 67890, 610, 6),
  ('UK',     '2022-01-04', 98765, 75,  2),
  ('AUS',    '2022-01-01', 23456, 25,  18),
  ('AFRICA', '2022-01-02', 87654, 125, 7),
  ('USA',    '2022-01-03', 54321, 75,  null),
  ('CHINA',  '2022-01-04', 12345, 100, 3),
  ('JAPAN',  '2022-01-01', 67890, 64,  10),
  ('UK',     '2022-01-02', 54321, 25,  5),
  ('AUS',    '2022-01-03', 98765, 150, 15),
  ('AFRICA', '2022-01-04', 23456, 75,  null),
  ('USA',    '2022-01-01', 87654, 125, 2),
  ('CHINA',  '2022-01-02', 54321, 175, 12),
  ('JAPAN',  '2022-01-03', 12345, 100, 3),
  ('UK',     '2022-01-04', 67890, 50,  10),
  ('AUS',    '2022-01-01', 54321, 25,  5),
  ('AFRICA', '2022-01-02', 98765, 150, 15),
  ('USA',    '2022-01-03', 23456, 75,  18),
  ('CHINA',  '2022-01-04', 87654, 125, 7),
  ('JAPAN',  '2022-01-01', 54321, 175, 12),
  ('UK',     '2022-01-02', 12345, 86,  3),
  ('AUS',    '2022-01-03', 67890, 50,  10),
  ('AFRICA', '2022-01-04', 54321, 25,  95),
  ('USA',    '2022-01-01', 98765, 150, 55),
  ('CHINA',  '2022-01-02', 23456, 75,  88),
  ('JAPAN',  '2022-01-03', 87654, 125, 67),
  ('UK',     '2022-01-04', 54321, 82,  72),
  ('AUS',    '2022-01-01', 12345, 90,  35),
  ('AFRICA', '2022-01-02', 67890, 50,  100),
  ('USA',    '2022-01-03', 54321, 25,  5),
  ('CHINA',  '2022-01-04', 98765, 150, 15),
  ('JAPAN',  '2022-01-01', 23456, 75,  null);

-- query 2
-- t0 has 33 rows; skip 10, take 20 → 20 rows
SELECT count(*) FROM (SELECT * FROM ${case_db}.t0 LIMIT 10, 20) xx;

-- query 3
-- skip 20, take 20 → only 13 remaining
SELECT count(*) FROM (SELECT * FROM ${case_db}.t0 LIMIT 20, 20) xx;

-- query 4
-- skip 50 → past end, 0 rows
SELECT count(*) FROM (SELECT * FROM ${case_db}.t0 LIMIT 50, 20) xx;

-- query 5
-- outer LIMIT 10 → 10 rows; then skip 10 → 0 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 10) x LIMIT 10, 20) xx;

-- query 6
-- inner: skip 10, take 10 → 10 rows; outer: skip 1, take 2 → 2 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 10, 10) x LIMIT 1, 2) xx;

-- query 7
-- inner: skip 10, take 50 → 23 rows; outer: skip 10, take 30 → 13 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 10, 50) x LIMIT 10, 30) xx;

-- query 8
-- inner: skip 50, take 1 → 0 rows; outer: skip 1 → still 0
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 50, 1) x LIMIT 1, 1) xx;

-- query 9
-- inner: skip 40, take 10 → 0 rows (33 total); outer: take 2 → 0
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 40, 10) x LIMIT 2) xx;

-- query 10
-- inner: skip 40, take 2 → 0 rows; outer: take 1 → 0
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 40, 2) x LIMIT 1) xx;

-- query 11
-- inner: skip 30, take 10 → 3 rows; outer: take 2 → 2
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 30, 10) x LIMIT 2) xx;

-- query 12
-- inner: skip 30, take 2 → 2 rows; outer: take 1 → 1
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 30, 2) x LIMIT 1) xx;

-- query 13
-- inner: skip 30, take 2 → 2 rows; outer: take 5 → 2 (capped)
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 30, 2) x LIMIT 5) xx;

-- query 14
-- inner: take 20; outer: skip 10, take 40 → 10 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 20) x LIMIT 10, 40) xx;

-- query 15
-- inner: take 10; outer: skip 10 → 0 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 10) x LIMIT 10, 10) xx;

-- query 16
-- inner: take 30; outer: skip 50 → 0 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 30) x LIMIT 50, 10) xx;

-- query 17
-- generate_series 1..100000; skip 50000, take 10
SELECT count(*) FROM (SELECT * FROM TABLE(generate_series(1, 100000)) LIMIT 50000, 10) x;

-- query 18
-- generate_series 1..100000; skip 90000, take 20000 → only 10000 remaining
SELECT count(*) FROM (SELECT * FROM TABLE(generate_series(1, 100000)) LIMIT 90000, 20000) x;

-- query 19
-- generate_series 1..100000; skip 1, take 1000
SELECT count(*) FROM (SELECT * FROM TABLE(generate_series(1, 100000)) LIMIT 1, 1000) x;

-- query 20
-- @skip_result_check=true
-- Disable multi-cast limit push-down; both CTE branches get the more conservative limit
SET enable_multi_cast_limit_push_down = false;
SET cbo_enable_low_cardinality_optimize = false;
SET cbo_cte_force_reuse_limit_without_order_by = false;

-- query 21
-- push-down=false: count is still correct (7 = 3 + 4)
WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 22
-- push-down=false: EXPLAIN executes without error; both branches share the conservative max limit
-- @skip_result_check=true
EXPLAIN VERBOSE WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT /*+SET_VAR(cbo_cte_reuse_rate=0)*/ count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 23
-- @skip_result_check=true
-- Enable multi-cast limit push-down; each branch uses its own tighter limit
SET enable_multi_cast_limit_push_down = true;

-- query 24
-- push-down=true: count is still 7
WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 25
-- push-down=true: EXPLAIN executes without error; the L branch gets its own tighter limit (3)
-- @skip_result_check=true
EXPLAIN VERBOSE WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT /*+SET_VAR(cbo_cte_reuse_rate=0)*/ count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 26
-- @skip_result_check=true
-- Restore session defaults
SET cbo_enable_low_cardinality_optimize = true;
SET cbo_cte_force_reuse_limit_without_order_by = true;
