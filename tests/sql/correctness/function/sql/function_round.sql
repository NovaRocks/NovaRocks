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

-- Check strict NULL propagation through persisted inputs and preserve the
-- null-safe comparisons with untyped NULLs from the original round fixture.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.round_inputs (
    id INTEGER,
    digits INTEGER,
    real_value REAL,
    double_value DOUBLE,
    decimal_value DECIMAL(20, 5)
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.round_inputs VALUES
    (1, NULL, NULL, NULL, NULL),
    (2, 2, NULL, NULL, NULL),
    (3, NULL, CAST(2.5e0 AS REAL), 2.5e0, DECIMAL '2.50000'),
    (4, 2, CAST(-12.375e0 AS REAL), -12.375e0, DECIMAL '-12.34565');

-- query 3
SELECT round(double_value) IS NOT DISTINCT FROM round(NULL) AS unary_nulls,
       round(double_value, digits) IS NOT DISTINCT FROM round(NULL, NULL) AS both_null,
       round(double_value, digits) IS NOT DISTINCT FROM round(double_value, NULL) AS null_digits,
       round(double_value, digits) IS NOT DISTINCT FROM round(NULL, digits) AS null_input
FROM ${case_db}.round_inputs
WHERE id = 1;

-- query 4
-- @order_sensitive=true
SELECT id,
       round(real_value, digits) AS rounded_real,
       round(double_value) AS rounded_double,
       round(double_value, digits) AS rounded_double_at_digits,
       round(decimal_value) AS rounded_decimal,
       round(decimal_value, digits) AS rounded_decimal_at_digits,
       truncate(decimal_value) AS truncated_decimal,
       truncate(decimal_value, digits) AS truncated_decimal_at_digits
FROM ${case_db}.round_inputs
ORDER BY id;

-- query 5
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT dround(double_value) FROM ${case_db}.round_inputs;

-- query 6
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT dround(double_value, digits) FROM ${case_db}.round_inputs;

-- query 7
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
-- @expect_error=[sql.analyze.type_mismatch] Unknown type: largeint
SELECT CAST(1 AS LARGEINT);

-- query 8
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT truncate(double_value, digits) FROM ${case_db}.round_inputs;
