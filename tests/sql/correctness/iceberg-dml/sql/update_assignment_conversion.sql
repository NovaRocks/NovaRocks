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

-- @order_sensitive=true
-- @tags=iceberg_dml,update,cast
-- Compare UPDATE assignment conversion with the same SELECT CAST value author.
-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_update_assignment_conversion ;
CREATE TABLE ${case_db}.t_update_assignment_conversion (
  id INT,
  d DECIMAL(8,2),
  dt DATE,
  ts TIMESTAMP(6),
  n INT,
  nullable_text VARCHAR,
  required_text VARCHAR NOT NULL
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_update_assignment_conversion VALUES
  ROW(1, 0, DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00.000000', 0, 'before', 'required'),
  ROW(2, 0, DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00.000000', 0, 'before', 'required');

-- query 2
SELECT CAST(CAST(DECIMAL '12.345' AS DECIMAL(8, 3)) AS DECIMAL(8, 2)) AS d, CAST('2024-02-29' AS DATE) AS dt, CAST('2024-02-29 12:34:56.123456' AS TIMESTAMP(6)) AS ts, CAST(BIGINT '123' AS INTEGER) AS n, CAST(NULL AS VARCHAR) AS nullable_text;
-- query 3
-- @skip_result_check=true
UPDATE ${case_db}.t_update_assignment_conversion
SET d = CAST(12.345 AS DECIMAL(8,3)),
    dt = DATE '2024-02-29',
    ts = CAST('2024-02-29 12:34:56.123456' AS TIMESTAMP(6)),
    n = BIGINT '123',
    nullable_text = NULL
WHERE id = 1;
-- query 4
SELECT d, dt, ts, n, nullable_text
FROM ${case_db}.t_update_assignment_conversion WHERE id = 1;

-- query 5
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(BIGINT '2147483648' AS INTEGER);
-- query 6
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
UPDATE ${case_db}.t_update_assignment_conversion
SET d = DECIMAL '-12.345', n = BIGINT '2147483648'
WHERE id = 2;
-- query 7
SELECT d, n FROM ${case_db}.t_update_assignment_conversion WHERE id = 2;

-- query 8
SELECT CAST(NULL AS VARCHAR) AS required_text;
-- query 9
-- @expect_error=[query.execution.invalid_data] Data failure
UPDATE ${case_db}.t_update_assignment_conversion SET required_text = NULL WHERE id = 1;
-- query 10
SELECT id, required_text FROM ${case_db}.t_update_assignment_conversion ORDER BY id;
-- query 11
-- @skip_result_check=true
DROP TABLE ${case_db}.t_update_assignment_conversion ;


-- Explicit text casts and UPDATE preserve the Trino floating-point values.
-- query 12
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_update_assignment_nonfinite ;
CREATE TABLE ${case_db}.t_update_assignment_nonfinite (
  id INT,
  token VARCHAR,
  nullable_float REAL,
  nullable_double DOUBLE,
  required_float REAL NOT NULL,
  required_double DOUBLE NOT NULL
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_update_assignment_nonfinite VALUES
  ROW(1, 'NaN', 21, 22, 11, 12),
  ROW(2, 'Infinity', 21, 22, 11, 12),
  ROW(3, '-Infinity', 21, 22, 11, 12),
  ROW(4, '1e1000', 21, 22, 11, 12),
  ROW(5, '2.5', 21, 22, 11, 12);

-- query 13
-- @skip_result_check=true
UPDATE ${case_db}.t_update_assignment_nonfinite
SET nullable_float = CAST(token AS REAL), nullable_double = CAST(token AS DOUBLE)
WHERE id <= 4;
-- query 14
SELECT id, token,
       nullable_float IS NULL AS float_is_null,
       CAST(token AS REAL) IS NULL AS cast_float_is_null,
       nullable_double IS NULL AS double_is_null,
       CAST(token AS DOUBLE) IS NULL AS cast_double_is_null
FROM ${case_db}.t_update_assignment_nonfinite ORDER BY id;

-- Assignments from VARCHAR require an explicit conversion. Rejections keep the previous rows.
-- query 15
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
UPDATE ${case_db}.t_update_assignment_nonfinite
SET required_float = token WHERE id = 1;
-- query 16
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
UPDATE ${case_db}.t_update_assignment_nonfinite
SET required_double = token WHERE id = 2;
-- query 17
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
UPDATE ${case_db}.t_update_assignment_nonfinite
SET required_float = token WHERE id = 3;
-- query 18
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
UPDATE ${case_db}.t_update_assignment_nonfinite
SET required_double = token WHERE id = 4;
-- query 19
SELECT id,
       required_float = 11 AS required_float_unchanged,
       required_double = 12 AS required_double_unchanged,
       nullable_float IS NULL AS float_is_null,
       nullable_double IS NULL AS double_is_null
FROM ${case_db}.t_update_assignment_nonfinite ORDER BY id;
-- query 20
SELECT id,
       nullable_float = 21 AS float_unchanged,
       nullable_double = 22 AS double_unchanged,
       required_float = 11 AS required_float_unchanged,
       required_double = 12 AS required_double_unchanged
FROM ${case_db}.t_update_assignment_nonfinite WHERE id = 5;
-- query 21
-- @skip_result_check=true
DROP TABLE ${case_db}.t_update_assignment_nonfinite ;
