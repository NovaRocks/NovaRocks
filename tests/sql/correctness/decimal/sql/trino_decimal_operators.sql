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

-- @order_sensitive=false
-- @tags=decimal,arithmetic,trino
-- Test Objective: Preserve Trino decimal result domains, rounding, nulls, and checked failures.
-- Oracle: Trino 484 terminal values, type signatures, and error names.
-- Maintenance: Decimal suite owner; VALUES and one small case-owned scan exercise ordinary distributed execution.

-- query 1
-- Reference: decimal_ratio
SELECT DECIMAL '1.00' / DECIMAL '3.00';

-- query 2
-- Reference: decimal_ratio_values
SELECT a / b FROM (VALUES (DECIMAL '1.00', DECIMAL '3.00')) AS t(a, b);

-- query 3
-- Reference: decimal_modulo_types_values
SELECT a % b, a + b, a - b FROM (VALUES (DECIMAL '42.01', DECIMAL '3.4')) AS t(a, b);

-- query 4
-- Reference: decimal_sum_carry_values
SELECT a + b, a - b FROM (VALUES (DECIMAL '99.95', CAST('0.05' AS DECIMAL(2,2)))) AS t(a, b);

-- query 5
-- Reference: decimal_sum_scale_shrink_values
SELECT a + b, a - b FROM (VALUES (CAST(12 AS DECIMAL(38,0)), CAST('0.5' AS DECIMAL(1,1)))) AS t(a, b);

-- query 6
-- Reference: decimal_difference_negative_half_values
SELECT a - b FROM (VALUES (CAST(-12 AS DECIMAL(38,0)), CAST('0.5' AS DECIMAL(1,1)))) AS t(a, b);

-- query 7
-- Reference: decimal_multiply_positive_half_values
SELECT a * b FROM (VALUES (CAST(1 AS DECIMAL(33,0)), CAST('0.0000005' AS DECIMAL(7,7)))) AS t(a, b);

-- query 8
-- Reference: decimal_multiply_negative_half_values
SELECT a * b FROM (VALUES (CAST(-1 AS DECIMAL(33,0)), CAST('0.0000005' AS DECIMAL(7,7)))) AS t(a, b);

-- query 9
-- Reference: decimal_divide_positive_half_values
SELECT a / b FROM (VALUES (DECIMAL '1', DECIMAL '128')) AS t(a, b);

-- query 10
-- Reference: decimal_divide_negative_half_values
SELECT a / b FROM (VALUES (DECIMAL '-1', DECIMAL '128')) AS t(a, b);

-- query 11
-- Reference: decimal_null_zero_values
SELECT a / b, a % b FROM (VALUES (CAST(NULL AS DECIMAL(3,2)), CAST('0.00' AS DECIMAL(2,2)))) AS t(a, b);

-- query 12
-- Reference: decimal_zero_null_values
SELECT a / b, a % b FROM (VALUES (CAST('0.00' AS DECIMAL(2,2)), CAST(NULL AS DECIMAL(3,2)))) AS t(a, b);

-- query 13
-- Reference: decimal_divide_zero_values
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT a / b FROM (VALUES (DECIMAL '1.00', CAST('0.00' AS DECIMAL(2,2)))) AS t(a, b);

-- query 14
-- Reference: decimal_modulo_zero_values
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT a % b FROM (VALUES (DECIMAL '1.00', CAST('0.00' AS DECIMAL(2,2)))) AS t(a, b);

-- query 15
-- Reference: decimal_with_tinyint_values
SELECT a + b, a - b, a * b, a / b, a % b FROM (VALUES (CAST(7 AS TINYINT), DECIMAL '1.50')) AS t(a, b);

-- query 16
-- Reference: decimal_then_tinyint_values
SELECT a / b, a % b FROM (VALUES (DECIMAL '1.50', CAST(7 AS TINYINT))) AS t(a, b);

-- query 17
-- Reference: decimal_with_smallint_values
SELECT a + b, a - b, a * b, a / b, a % b FROM (VALUES (CAST(7 AS SMALLINT), DECIMAL '1.50')) AS t(a, b);

-- query 18
-- Reference: decimal_then_smallint_values
SELECT a / b, a % b FROM (VALUES (DECIMAL '1.50', CAST(7 AS SMALLINT))) AS t(a, b);

-- query 19
-- Reference: decimal_with_integer_values
SELECT a + b, a - b, a * b, a / b, a % b FROM (VALUES (CAST(7 AS INTEGER), DECIMAL '1.50')) AS t(a, b);

-- query 20
-- Reference: decimal_then_integer_values
SELECT a / b, a % b FROM (VALUES (DECIMAL '1.50', CAST(7 AS INTEGER))) AS t(a, b);

-- query 21
-- Reference: decimal_with_bigint_values
SELECT a + b, a - b, a * b, a / b, a % b FROM (VALUES (CAST(7 AS BIGINT), DECIMAL '1.50')) AS t(a, b);

-- query 22
-- Reference: decimal_then_bigint_values
SELECT a / b, a % b FROM (VALUES (DECIMAL '1.50', CAST(7 AS BIGINT))) AS t(a, b);

-- query 23
-- Reference: decimal_add_alignment_error_values
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT a + b FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)), CAST('0.01' AS DECIMAL(2,2)))) AS t(a, b);

-- query 24
-- Reference: decimal_subtract_alignment_error_values
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT a - b FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)), CAST('0.01' AS DECIMAL(2,2)))) AS t(a, b);

-- query 25
-- Reference: decimal_final_precision_error_values
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT a + b FROM (VALUES (CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)), DECIMAL '1')) AS t(a, b);

-- query 26
-- Reference: decimal_quotient_carry_values
SELECT a / b FROM (VALUES (CAST('0.9999995' AS DECIMAL(38,7)), DECIMAL '1.0')) AS t(a, b);

-- query 27
-- Reference: decimal_quotient_precision_error_values
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT a / b FROM (VALUES (CAST('10000000000000000000000000000000000000' AS DECIMAL(38,0)), CAST('0.3' AS DECIMAL(1,1)))) AS t(a, b);

-- query 28
-- @skip_result_check=true
CREATE TABLE ${case_db}.trino_decimal_long_input (
    id INTEGER, a DECIMAL(38,0), b DECIMAL(38,38), n DECIMAL(1,0)
) WITH (format_version = 3);

-- query 29
-- @skip_result_check=true
INSERT INTO ${case_db}.trino_decimal_long_input VALUES
    ROW(1, CAST('10000000000000000000000000000000000000' AS DECIMAL(38,0)),
        CAST('0.3' AS DECIMAL(38,38)), CAST(0 AS DECIMAL(1,0))),
    ROW(2, CAST(0 AS DECIMAL(38,0)), CAST('0.5' AS DECIMAL(38,38)),
        CAST(0 AS DECIMAL(1,0))),
    ROW(3, CAST(2 AS DECIMAL(38,0)), CAST('0.9' AS DECIMAL(38,38)),
        CAST(0 AS DECIMAL(1,0)));

-- query 30
-- Reference: decimal_wide_divide_succeeds_values
SELECT a / a FROM ${case_db}.trino_decimal_long_input WHERE id = 1;

-- query 31
-- Reference: decimal_wide_modulo_succeeds_values
SELECT a % b FROM ${case_db}.trino_decimal_long_input WHERE id = 1;

-- query 32
-- Reference: decimal_wide_divide_rescale_error_values
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT n / b FROM ${case_db}.trino_decimal_long_input WHERE id = 2;

-- query 33
-- Reference: decimal_wide_multiply_intermediate_error_values
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT a * b FROM ${case_db}.trino_decimal_long_input WHERE id = 3;

-- query 34
-- @skip_result_check=true
DROP TABLE ${case_db}.trino_decimal_long_input;

-- query 35
-- Reference: decimal_positive_rejected
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT +DECIMAL '1.25';

-- query 36
-- Reference: decimal_positive_values_rejected
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT +a FROM (VALUES (DECIMAL '1.25')) AS t(a);
