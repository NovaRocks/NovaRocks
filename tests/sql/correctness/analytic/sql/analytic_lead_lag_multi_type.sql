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
  c4 REAL,
  c5 DECIMAL(38, 20) NOT NULL,
  c6 DATE NOT NULL
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_ll_types VALUES
  ROW(1, 1, 'a', 'b', 1.23, 3.33, DATE '2021-11-11'),
  ROW(2, 600000, 'aa', 'bb', 2.23, 4.33, DATE '2022-12-11'),
  ROW(3, 400000, 'aaa', 'bbb', 3.23, 5.33, DATE '2022-06-11');

-- query 2
-- INT: implicit null default (offset 5 > 3 rows, so all rows fall back to NULL)
SELECT c0, lead(c1, 5) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 3
-- INT: lag with null default
SELECT c0, lag(c1, 5) OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 4
-- INT: explicit null default
SELECT c0, lead(c1, 5, null) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 5
-- INT: explicit null default (lag)
SELECT c0, lag(c1, 5, null) OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 6
-- INT: constant integer default
SELECT c0, lead(c1, 5, 1) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 7
-- INT: constant expression default (1+2+3 evaluates to 6)
SELECT c0, lag(c1, 5, 1+2+3) OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 8
-- INT: a typed function expression is an eligible default.
SELECT c0, lag(c1, 5, abs(1)) OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 9
-- VARCHAR: null default
SELECT c0, lead(c2, 5) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 10
-- VARCHAR: integer expression default (cast to VARCHAR)
SELECT c0, lead(c2, 5, CAST(1+2+3 AS VARCHAR(500))) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 11
-- VARCHAR: function expression default (day() result cast to VARCHAR)
SELECT c0, lead(c2, 5, CAST(day(DATE '2021-01-02') AS VARCHAR(500))) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 12
-- VARCHAR: string literal default
SELECT c0, lag(c2, 5, 'abc') OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 13
-- FLOAT: integer literal default (cast to float)
SELECT c0, lead(c4, 5, 1) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 14
-- REAL and VARCHAR have no implicit common type.
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT c0, lag(c4, 5, '6') OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types;

-- query 15
-- FLOAT: decimal literal default
SELECT c0, lag(c4, 5, REAL '3.456') OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 16
-- DECIMAL128: string literal default (cast to decimal)
SELECT c0, lead(c5, 5, CAST('12345' AS DECIMAL(38,20))) OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 17
-- DECIMAL128: integer literal default (cast to decimal)
SELECT c0, lag(c5, 5, 123) OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 18
-- DECIMAL128: decimal literal default
SELECT c0, lag(c5, 5, 123.456) OVER (ORDER BY c0) AS lag_result FROM ${case_db}.t_ll_types ORDER BY c0;

-- query 19
-- DATE: string literal default (cast to date)
SELECT c0, lead(c6, 5, DATE '2021-11-11') OVER (ORDER BY c0) AS lead_result FROM ${case_db}.t_ll_types ORDER BY c0;
