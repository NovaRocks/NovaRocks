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
-- @tags=integer-semantics,self_contained
-- Test Objective: Preserve Trino 483 signed integer types, checked errors, and GROUPING widths through native execution.
-- Oracle: Trino tag 483 (50b0b50b75abd47f830b7805ee1b51716eb4065e), independently observed types, values, and errors.
-- Maintenance: project suite owner; small constant/VALUES queries share the normal 1FE+3BE lifecycle and cleanup.

-- query 1
-- Reference: L01_i32_boundaries
SELECT 0, 127, 128, 32767, 32768, 2147483647, 2147483648, -2147483648, -2147483649;

-- query 2
-- Reference: L02_i64_precision
SELECT 9223372036854775807, -9223372036854775808, 9007199254740993, -9007199254740993;

-- query 3
-- Reference: L03_signed_vs_unary
SELECT -2147483648, -(2147483648), +2147483648, +(-2147483648), -(-2147483647);

-- query 4
-- Reference: L05_positive_i64_out
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT 9223372036854775808;

-- query 5
-- Reference: L06_negative_i64_out
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT -9223372036854775809;

-- query 6
-- Reference: L07_parenthesized_magnitude_out
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
SELECT -(9223372036854775808);

-- query 7
-- Reference: L08_integer_negate_min
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -(-2147483648);

-- query 8
-- Reference: L09_bigint_negate_min
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -(-9223372036854775808);

-- query 9
-- Reference: A01_integer_operators
SELECT 1+2, 1-2, 3*2, 5/2, -5/2, 5/-2, -5/-2, -5%2, 5%-2, -5%-2, +1, -(1);

-- query 10
-- Reference: A02_values_integer_operators
SELECT a,b,a+b,a-b,a*b,a/b,a%b,+a,-a FROM (VALUES (CAST(5 AS integer),CAST(2 AS integer)),(CAST(-5 AS integer),CAST(2 AS integer)),(CAST(5 AS integer),CAST(-2 AS integer)),(CAST(-5 AS integer),CAST(-2 AS integer)),(CAST(NULL AS integer),CAST(2 AS integer))) AS t(a,b) ORDER BY a,b;

-- query 11
-- Reference: A03_same_widths
SELECT CAST(-5 AS tinyint)+CAST(2 AS tinyint), CAST(-5 AS tinyint)-CAST(2 AS tinyint), CAST(-5 AS tinyint)*CAST(2 AS tinyint), CAST(-5 AS tinyint)/CAST(2 AS tinyint), CAST(-5 AS tinyint)%CAST(2 AS tinyint), +(CAST(-5 AS tinyint)), -(CAST(-5 AS tinyint)), CAST(-5 AS smallint)+CAST(2 AS smallint), CAST(-5 AS smallint)-CAST(2 AS smallint), CAST(-5 AS smallint)*CAST(2 AS smallint), CAST(-5 AS smallint)/CAST(2 AS smallint), CAST(-5 AS smallint)%CAST(2 AS smallint), +(CAST(-5 AS smallint)), -(CAST(-5 AS smallint)), CAST(-5 AS integer)+CAST(2 AS integer), CAST(-5 AS integer)-CAST(2 AS integer), CAST(-5 AS integer)*CAST(2 AS integer), CAST(-5 AS integer)/CAST(2 AS integer), CAST(-5 AS integer)%CAST(2 AS integer), +(CAST(-5 AS integer)), -(CAST(-5 AS integer)), CAST(-5 AS bigint)+CAST(2 AS bigint), CAST(-5 AS bigint)-CAST(2 AS bigint), CAST(-5 AS bigint)*CAST(2 AS bigint), CAST(-5 AS bigint)/CAST(2 AS bigint), CAST(-5 AS bigint)%CAST(2 AS bigint), +(CAST(-5 AS bigint)), -(CAST(-5 AS bigint));

-- query 12
-- Reference: A04_mixed_widths
SELECT CAST(-5 AS tinyint)+CAST(2 AS smallint), CAST(-5 AS tinyint)-CAST(2 AS smallint), CAST(-5 AS tinyint)*CAST(2 AS smallint), CAST(-5 AS tinyint)/CAST(2 AS smallint), CAST(-5 AS tinyint)%CAST(2 AS smallint), CAST(-5 AS integer)+CAST(2 AS bigint), CAST(-5 AS integer)-CAST(2 AS bigint), CAST(-5 AS integer)*CAST(2 AS bigint), CAST(-5 AS integer)/CAST(2 AS bigint), CAST(-5 AS integer)%CAST(2 AS bigint);

-- query 13
-- Reference: A05_bigint_result_not_narrowed
SELECT 2147483648-1,CAST(1 AS bigint)+1,CAST(5 AS bigint)/2;

-- query 14
-- Reference: A06_min_mod_minus_one
SELECT CAST(-128 AS tinyint)%CAST(-1 AS tinyint),CAST(-32768 AS smallint)%CAST(-1 AS smallint),CAST(-2147483648 AS integer)%CAST(-1 AS integer),CAST(-9223372036854775808 AS bigint)%CAST(-1 AS bigint);

-- query 15
-- Reference: E_add_overflow_tinyint
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(127 AS tinyint)+CAST(1 AS tinyint);

-- query 16
-- Reference: E_add_overflow_smallint
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(32767 AS smallint)+CAST(1 AS smallint);

-- query 17
-- Reference: E_add_overflow_integer
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(2147483647 AS integer)+CAST(1 AS integer);

-- query 18
-- Reference: E_add_overflow_bigint
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(9223372036854775807 AS bigint)+CAST(1 AS bigint);

-- query 19
-- Reference: E_negate_min_tinyint
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -(CAST(-128 AS tinyint));

-- query 20
-- Reference: E_negate_min_smallint
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -(CAST(-32768 AS smallint));

-- query 21
-- Reference: E_negate_min_integer
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -(CAST(-2147483648 AS integer));

-- query 22
-- Reference: E_negate_min_bigint
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -(CAST(-9223372036854775808 AS bigint));

-- query 23
-- Reference: E_subtract_overflow_integer
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -2147483648-1;

-- query 24
-- Reference: E_multiply_overflow_integer
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT 1073741824*2;

-- query 25
-- Reference: E_divide_min_integer
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -2147483648/-1;

-- query 26
-- Reference: E_divide_min_bigint
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT -9223372036854775808/-1;

-- query 27
-- Reference: E_divide_zero_integer
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT 1/0;

-- query 28
-- Reference: E_modulo_zero_integer
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT 1%0;

-- query 29
-- Reference: E_divide_zero_tinyint
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT CAST(1 AS tinyint)/CAST(0 AS tinyint);

-- query 30
-- Reference: E_values_add_overflow
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT a+1 FROM (VALUES (CAST(2147483647 AS integer))) AS t(a);

-- query 31
-- Reference: E_values_divide_zero
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT a/b FROM (VALUES (CAST(1 AS integer),CAST(0 AS integer))) AS t(a,b);

-- query 32
-- Reference: R08_original
SELECT grouping(a) FROM (VALUES (CAST(1 AS integer))) AS t(a) GROUP BY ROLLUP(a);

-- query 33
-- Reference: R14_original
SELECT x + 1 FROM (VALUES (CAST(1 AS integer))) AS t(x) ORDER BY 1;

-- query 34
-- Reference: G01_thresholds_two_sets
SELECT GROUPING(a), GROUPING(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a), GROUPING(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a), GROUPING(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a) FROM (VALUES (1)) AS t(a) GROUP BY GROUPING SETS ((a), ()) ORDER BY 1;

-- query 35
-- Reference: G02_thresholds_single_set
SELECT GROUPING(a), GROUPING(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a), GROUPING(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a), GROUPING(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a) FROM (VALUES (1)) AS t(a) GROUP BY a;

-- query 36
-- Reference: G03_64_arguments
-- @expect_trino_error=TOO_MANY_ARGUMENTS,USER_ERROR
SELECT GROUPING(a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a,a) FROM (VALUES (1)) AS t(a) GROUP BY GROUPING SETS ((a), ());

-- query 37
-- Reference: G04_bit_order
SELECT grouping(a,b) FROM (VALUES (1,2)) AS t(a,b) GROUP BY GROUPING SETS ((a),(b),()) ORDER BY 1;

