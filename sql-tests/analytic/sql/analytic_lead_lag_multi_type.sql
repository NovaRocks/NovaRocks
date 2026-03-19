-- Migrated from: dev/test/sql/test_lead_lag/T/test_lead_lag
-- Test Objective:
-- 1. Validate lead/lag on multiple column types: INT, VARCHAR, FLOAT, DECIMAL128, DATE.
-- 2. Test implicit NULL default, explicit NULL default, constant default, and expression default.
-- 3. Verify FE rejects non-literal or type-mismatched default arguments.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_ll_types (
  c0 INT NOT NULL,
  c1 INT NOT NULL,
  c2 VARCHAR(500) NOT NULL,
  c3 VARCHAR(500),
  c4 FLOAT,
  c5 DECIMAL128(38, 20) NOT NULL,
  c6 DATE NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(c0, c1, c2)
DISTRIBUTED BY HASH(c0, c1, c2) BUCKETS 3
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.t_ll_types VALUES
  (1, 1, 'a', 'b', 1.23, 3.33, '2021-11-11'),
  (2, 600000, 'aa', 'bb', 2.23, 4.33, '2022-12-11'),
  (3, 400000, 'aaa', 'bbb', 3.23, 5.33, '2022-06-11');

-- query 2
-- INT: implicit null default (offset 5 > 3 rows, so all rows fall back to NULL)
SELECT c0, lead(c1, 5) OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 3
-- INT: lag with null default
SELECT c0, lag(c1, 5) OVER() AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 4
-- INT: explicit null default
SELECT c0, lead(c1, 5, null) OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 5
-- INT: explicit null default (lag)
SELECT c0, lag(c1, 5, null) OVER() AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 6
-- INT: constant integer default
SELECT c0, lead(c1, 5, 1) OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 7
-- INT: constant expression default (1+2+3 evaluates to 6)
SELECT c0, lag(c1, 5, 1+2+3) OVER() AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 8
-- INT: FE rejects function call as default (not a constant literal expression)
-- @expect_error=The type of the third parameter of LEAD/LAG not match the type INT.
SELECT c0, lag(c1, 5, abs(1)) OVER() AS lag_result FROM ${case_db}.t_ll_types;

-- query 9
-- VARCHAR: null default
SELECT c0, lead(c2, 5) OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 10
-- VARCHAR: integer expression default (cast to VARCHAR)
SELECT c0, lead(c2, 5, 1+2+3) OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 11
-- VARCHAR: function expression default (day() result cast to VARCHAR)
SELECT c0, lead(c2, 5, day('2021-01-02')) OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 12
-- VARCHAR: string literal default
SELECT c0, lag(c2, 5, 'abc') OVER() AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 13
-- FLOAT: integer literal default (cast to float)
SELECT c0, lead(c4, 5, 1) OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 14
-- FLOAT: FE rejects integer expression 1+2+3 as default for FLOAT column
-- @expect_error=The type of the third parameter of LEAD/LAG not match the type FLOAT.
SELECT c0, lag(c4, 5, 1+2+3) OVER() AS lag_result FROM ${case_db}.t_ll_types;

-- query 15
-- FLOAT: decimal literal default
SELECT c0, lag(c4, 5, 3.456) OVER() AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 16
-- DECIMAL128: string literal default (cast to decimal)
SELECT c0, lead(c5, 5, '12345') OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 17
-- DECIMAL128: integer literal default (cast to decimal)
SELECT c0, lag(c5, 5, 123) OVER() AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 18
-- DECIMAL128: decimal literal default
SELECT c0, lag(c5, 5, 123.456) OVER() AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 19
-- DATE: string literal default (cast to date)
SELECT c0, lead(c6, 5, '2021-11-11') OVER() AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;
