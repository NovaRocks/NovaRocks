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
-- @tags=float,arithmetic,trino,self_contained
-- Test Objective: Preserve REAL and DOUBLE arithmetic width, IEEE results, and predicate semantics.
-- Oracle: Trino 484 terminal values, type signatures, and error names.
-- Maintenance: Project suite owner; constant and VALUES inputs require no additional fixture.

-- query 1
-- Reference: float_real_precision
SELECT (CAST(16777216e0 AS REAL) + CAST(1e0 AS REAL)) - CAST(16777216e0 AS REAL);

-- query 2
-- Reference: float_real_precision_values
SELECT (a + b) - a FROM (VALUES (CAST(16777216e0 AS REAL), CAST(1e0 AS REAL))) AS t(a,b);

-- query 3
-- Reference: float_real_positive_zero_values
SELECT a / b, a % b FROM (VALUES (CAST(1e0 AS REAL), CAST(0.0e0 AS REAL))) AS t(a, b);

-- query 4
-- Reference: float_real_negative_zero_values
SELECT a / b, a % b FROM (VALUES (CAST(1e0 AS REAL), CAST(-0.0e0 AS REAL))) AS t(a, b);

-- query 5
-- Reference: float_real_zero_zero_values
SELECT a / b FROM (VALUES (CAST(-0.0e0 AS REAL), CAST(0.0e0 AS REAL))) AS t(a, b);

-- query 6
-- Reference: float_real_zero_sign_values
SELECT a * b, a / b FROM (VALUES (CAST(-0.0e0 AS REAL), CAST(1e0 AS REAL))) AS t(a, b);

-- query 7
-- Reference: float_real_overflow_values
SELECT a * b FROM (VALUES (CAST(3.4028235e38 AS REAL), CAST(2e0 AS REAL))) AS t(a, b);

-- query 8
-- Reference: float_real_null_values
SELECT a + b, a / b, a % b FROM (VALUES (CAST(NULL AS REAL), CAST(0e0 AS REAL))) AS t(a, b);

-- query 9
-- Reference: float_real_positive
SELECT +CAST(1.25e0 AS REAL), +CAST(NULL AS REAL), -CAST(-0.0e0 AS REAL);

-- query 10
-- Reference: float_real_positive_values
SELECT +a, -a FROM (VALUES (CAST(-0.0e0 AS REAL)), (CAST(NULL AS REAL))) AS t(a);

-- query 11
-- Reference: float_real_negation_and_order
SELECT (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)) = (CAST(1e0 AS REAL) / CAST(0e0 AS REAL)), (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)) <> (CAST(1e0 AS REAL) / CAST(0e0 AS REAL)), (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)) < (CAST(1e0 AS REAL) / CAST(0e0 AS REAL)), (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)) <= (CAST(1e0 AS REAL) / CAST(0e0 AS REAL)), (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)) > (CAST(1e0 AS REAL) / CAST(0e0 AS REAL)), (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)) >= (CAST(1e0 AS REAL) / CAST(0e0 AS REAL)), (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)) IS NOT DISTINCT FROM (CAST(1e0 AS REAL) / CAST(0e0 AS REAL));

-- query 12
-- Reference: float_real_predicate_values
SELECT id, a = b, a <> b, a < b, a <= b, a > b, a >= b,
               a IS NOT DISTINCT FROM b, a IS DISTINCT FROM b
        FROM (VALUES (1, (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)), (CAST(0e0 AS REAL) / CAST(0e0 AS REAL))), (2, (CAST(0e0 AS REAL) / CAST(0e0 AS REAL)), CAST(1.0e0 AS REAL)),
                     (3, CAST(-0.0e0 AS REAL), CAST(0.0e0 AS REAL)), (4, NULL, NULL),
                     (5, CAST(1.5e0 AS REAL), NULL), (6, CAST(1.5e0 AS REAL), CAST(2.5e0 AS REAL))) AS t(id,a,b)
        ORDER BY id;

-- query 13
-- Reference: float_double_positive_zero_values
SELECT a / b, a % b FROM (VALUES (1e0, 0.0e0)) AS t(a, b);

-- query 14
-- Reference: float_double_negative_zero_values
SELECT a / b, a % b FROM (VALUES (1e0, (-0.0e0))) AS t(a, b);

-- query 15
-- Reference: float_double_zero_zero_values
SELECT a / b FROM (VALUES ((-0.0e0), 0.0e0)) AS t(a, b);

-- query 16
-- Reference: float_double_zero_sign_values
SELECT a * b, a / b FROM (VALUES ((-0.0e0), 1e0)) AS t(a, b);

-- query 17
-- Reference: float_double_overflow_values
SELECT a * b FROM (VALUES (1.7976931348623157e308, 2e0)) AS t(a, b);

-- query 18
-- Reference: float_double_null_values
SELECT a + b, a / b, a % b FROM (VALUES (CAST(NULL AS DOUBLE), 0e0)) AS t(a, b);

-- query 19
-- Reference: float_double_positive
SELECT +1.25e0, +CAST(NULL AS DOUBLE), -(-0.0e0);

-- query 20
-- Reference: float_double_positive_values
SELECT +a, -a FROM (VALUES ((-0.0e0)), (CAST(NULL AS DOUBLE))) AS t(a);

-- query 21
-- Reference: float_double_negation_and_order
SELECT (0e0 / 0e0) = (1e0 / 0e0), (0e0 / 0e0) <> (1e0 / 0e0), (0e0 / 0e0) < (1e0 / 0e0), (0e0 / 0e0) <= (1e0 / 0e0), (0e0 / 0e0) > (1e0 / 0e0), (0e0 / 0e0) >= (1e0 / 0e0), (0e0 / 0e0) IS NOT DISTINCT FROM (1e0 / 0e0);

-- query 22
-- Reference: float_double_predicate_values
SELECT id, a = b, a <> b, a < b, a <= b, a > b, a >= b,
               a IS NOT DISTINCT FROM b, a IS DISTINCT FROM b
        FROM (VALUES (1, (0e0 / 0e0), (0e0 / 0e0)), (2, (0e0 / 0e0), 1.0e0),
                     (3, (-0.0e0), 0.0e0), (4, NULL, NULL),
                     (5, 1.5e0, NULL), (6, 1.5e0, 2.5e0)) AS t(id,a,b)
        ORDER BY id;

-- query 23
-- Reference: float_real_double_values
SELECT a + b, a / b, a % b FROM (VALUES (CAST(1.25e0 AS REAL), 0.50e0)) AS t(a, b);

-- query 24
-- Reference: float_real_decimal_values
SELECT a + b, a / b, a % b FROM (VALUES (CAST(1.25e0 AS REAL), CAST('0.50' AS DECIMAL(2,2)))) AS t(a, b);

-- query 25
-- Reference: float_real_bigint_values
SELECT a + b, a / b, a % b FROM (VALUES (CAST(1.25e0 AS REAL), CAST(3 AS BIGINT))) AS t(a, b);

-- query 26
-- Reference: float_double_decimal_values
SELECT a + b, a / b, a % b FROM (VALUES (1.25e0, CAST('0.50' AS DECIMAL(2,2)))) AS t(a, b);

-- query 27
-- Reference: float_double_integer_values
SELECT a + b, a / b, a % b FROM (VALUES (1.25e0, CAST(3 AS INTEGER))) AS t(a, b);

-- query 28
-- Reference: boolean_add_rejected
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT TRUE + 1;

-- query 29
-- Reference: integer_bigint_modulo_zero
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT CAST(7 AS BIGINT) % CAST(0 AS BIGINT);

-- query 30
-- Reference: untyped_null_arithmetic
SELECT 1 + NULL, NULL + 1, CAST(2e0 AS REAL) + NULL, DECIMAL '3.14' + NULL;

-- query 31
-- Reference: untyped_null_add
-- @expect_trino_error=AMBIGUOUS_FUNCTION_CALL,USER_ERROR
SELECT NULL + NULL;

-- query 32
-- Reference: untyped_null_subtract
-- @expect_trino_error=AMBIGUOUS_FUNCTION_CALL,USER_ERROR
SELECT NULL - NULL;

-- query 33
-- Reference: untyped_null_multiply
-- @expect_trino_error=AMBIGUOUS_FUNCTION_CALL,USER_ERROR
SELECT NULL * NULL;

-- query 34
-- Reference: untyped_null_divide
-- @expect_trino_error=AMBIGUOUS_FUNCTION_CALL,USER_ERROR
SELECT NULL / NULL;

-- query 35
-- Reference: untyped_null_modulo
SELECT NULL % NULL;

-- query 36
-- Reference: untyped_null_negative
-- @expect_trino_error=AMBIGUOUS_FUNCTION_CALL,USER_ERROR
SELECT -NULL;

-- query 37
-- Reference: untyped_null_positive
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT +NULL;
