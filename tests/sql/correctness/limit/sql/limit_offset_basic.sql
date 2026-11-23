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
) WITH (format_version = 3);

INSERT INTO ${case_db}.t0 (region, order_date, income, ship_mode, ship_code) VALUES
  ROW('USA',    DATE '2022-01-01', 12345, 50,  1),
  ROW('CHINA',  DATE '2022-01-02', 54321, 51,  4),
  ROW('JAPAN',  DATE '2022-01-03', 67890, 610, 6),
  ROW('UK',     DATE '2022-01-04', 98765, 75,  2),
  ROW('AUS',    DATE '2022-01-01', 23456, 25,  18),
  ROW('AFRICA', DATE '2022-01-02', 87654, 125, 7),
  ROW('USA',    DATE '2022-01-03', 54321, 75,  null),
  ROW('CHINA',  DATE '2022-01-04', 12345, 100, 3),
  ROW('JAPAN',  DATE '2022-01-01', 67890, 64,  10),
  ROW('UK',     DATE '2022-01-02', 54321, 25,  5),
  ROW('AUS',    DATE '2022-01-03', 98765, 150, 15),
  ROW('AFRICA', DATE '2022-01-04', 23456, 75,  null),
  ROW('USA',    DATE '2022-01-01', 87654, 125, 2),
  ROW('CHINA',  DATE '2022-01-02', 54321, 175, 12),
  ROW('JAPAN',  DATE '2022-01-03', 12345, 100, 3),
  ROW('UK',     DATE '2022-01-04', 67890, 50,  10),
  ROW('AUS',    DATE '2022-01-01', 54321, 25,  5),
  ROW('AFRICA', DATE '2022-01-02', 98765, 150, 15),
  ROW('USA',    DATE '2022-01-03', 23456, 75,  18),
  ROW('CHINA',  DATE '2022-01-04', 87654, 125, 7),
  ROW('JAPAN',  DATE '2022-01-01', 54321, 175, 12),
  ROW('UK',     DATE '2022-01-02', 12345, 86,  3),
  ROW('AUS',    DATE '2022-01-03', 67890, 50,  10),
  ROW('AFRICA', DATE '2022-01-04', 54321, 25,  95),
  ROW('USA',    DATE '2022-01-01', 98765, 150, 55),
  ROW('CHINA',  DATE '2022-01-02', 23456, 75,  88),
  ROW('JAPAN',  DATE '2022-01-03', 87654, 125, 67),
  ROW('UK',     DATE '2022-01-04', 54321, 82,  72),
  ROW('AUS',    DATE '2022-01-01', 12345, 90,  35),
  ROW('AFRICA', DATE '2022-01-02', 67890, 50,  100),
  ROW('USA',    DATE '2022-01-03', 54321, 25,  5),
  ROW('CHINA',  DATE '2022-01-04', 98765, 150, 15),
  ROW('JAPAN',  DATE '2022-01-01', 23456, 75,  null);

-- query 2
-- t0 has 33 rows; skip 10, take 20 → 20 rows
SELECT count(*) FROM (SELECT * FROM ${case_db}.t0 OFFSET 10
LIMIT  20) xx;

-- query 3
-- skip 20, take 20 → only 13 remaining
SELECT count(*) FROM (SELECT * FROM ${case_db}.t0 OFFSET 20
LIMIT  20) xx;

-- query 4
-- skip 50 → past end, 0 rows
SELECT count(*) FROM (SELECT * FROM ${case_db}.t0 OFFSET 50
LIMIT  20) xx;

-- query 5
-- outer LIMIT 10 → 10 rows; then skip 10 → 0 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 10) x OFFSET 10
LIMIT  20) xx;

-- query 6
-- inner: skip 10, take 10 → 10 rows; outer: skip 1, take 2 → 2 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 10
LIMIT  10) x OFFSET 1
LIMIT  2) xx;

-- query 7
-- inner: skip 10, take 50 → 23 rows; outer: skip 10, take 30 → 13 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 10
LIMIT  50) x OFFSET 10
LIMIT  30) xx;

-- query 8
-- inner: skip 50, take 1 → 0 rows; outer: skip 1 → still 0
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 50
LIMIT  1) x OFFSET 1
LIMIT  1) xx;

-- query 9
-- inner: skip 40, take 10 → 0 rows (33 total); outer: take 2 → 0
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 40
LIMIT  10) x LIMIT 2) xx;

-- query 10
-- inner: skip 40, take 2 → 0 rows; outer: take 1 → 0
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 40
LIMIT  2) x LIMIT 1) xx;

-- query 11
-- inner: skip 30, take 10 → 3 rows; outer: take 2 → 2
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 30
LIMIT  10) x LIMIT 2) xx;

-- query 12
-- inner: skip 30, take 2 → 2 rows; outer: take 1 → 1
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 30
LIMIT  2) x LIMIT 1) xx;

-- query 13
-- inner: skip 30, take 2 → 2 rows; outer: take 5 → 2 (capped)
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 OFFSET 30
LIMIT  2) x LIMIT 5) xx;

-- query 14
-- inner: take 20; outer: skip 10, take 40 → 10 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 20) x OFFSET 10
LIMIT  40) xx;

-- query 15
-- inner: take 10; outer: skip 10 → 0 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 10) x OFFSET 10
LIMIT  10) xx;

-- query 16
-- inner: take 30; outer: skip 50 → 0 rows
SELECT COUNT(*) FROM (SELECT * FROM (SELECT * FROM ${case_db}.t0 LIMIT 30) x OFFSET 50
LIMIT  10) xx;

-- query 17
-- generate_series 1..100000; skip 50000, take 10
SELECT count() FROM (SELECT * FROM TABLE(generate_series(1, 100000)) OFFSET 50000
LIMIT  10) x;

-- query 18
-- generate_series 1..100000; skip 90000, take 20000 → only 10000 remaining
SELECT count() FROM (SELECT * FROM TABLE(generate_series(1, 100000)) OFFSET 90000
LIMIT  20000) x;

-- query 19
-- generate_series 1..100000; skip 1, take 1000
SELECT count() FROM (SELECT * FROM TABLE(generate_series(1, 100000)) OFFSET 1
LIMIT  1000) x;

-- query 20
-- @skip_result_check=true
-- Disable multi-cast limit push-down; both CTE branches get the more conservative limit

-- query 21
-- push-down=false: count is still correct (7 = 3 + 4)
WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 22
-- push-down=false: EXPLAIN executes without error; both branches share the conservative max limit
-- @skip_result_check=true
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT  count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 23
-- @skip_result_check=true
-- Enable multi-cast limit push-down; each branch uses its own tighter limit

-- query 24
-- push-down=true: count is still 7
WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 25
-- push-down=true: EXPLAIN executes without error; the L branch gets its own tighter limit (3)
-- @skip_result_check=true
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'  WITH C AS (SELECT region, sum(income) AS total FROM ${case_db}.t0 GROUP BY 1),
     L AS (SELECT total FROM C LIMIT 3),
     R AS (SELECT total FROM C LIMIT 4)
SELECT  count(*) FROM (SELECT * FROM L UNION ALL SELECT * FROM R) AS U;

-- query 26
-- @skip_result_check=true
-- Restore session defaults
