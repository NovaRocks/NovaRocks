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
-- ARRAY common inference and ordered strict coercion; independent expected values.

-- query 1
SELECT [CAST(1 AS INT),CAST(2 AS SMALLINT)] AS widened;

-- query 2
SELECT [CAST(1.5 AS REAL),CAST(2 AS INT)] AS real_values;

-- query 3
SELECT [CAST(12 AS DECIMAL(2,0)),CAST(1.5 AS DECIMAL(2,1))] AS decimal_values;

-- query 4
SELECT [CAST(0 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))] AS capped_zero;

-- query 5
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT [CAST(1 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))] AS capped_overflow;

-- query 6
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT TRY_CAST([CAST([1e20] AS ARRAY<DECIMAL(20,0)>)[1],CAST(0 AS DECIMAL(20,0))] AS ARRAY<DECIMAL(20,0)>) AS source_failure;

-- query 7
SELECT [[CAST(1 AS INT)],[CAST(2 AS SMALLINT),NULL]] AS nested_values;

-- query 8
SELECT [[NULL],NULL] AS nested_unknown;

-- query 9
SELECT [CAST(NULL AS INT),CAST(2 AS SMALLINT)] AS nullable_values;

-- query 10
SELECT [[],[CAST(1 AS SMALLINT)]] AS empty_then_value;

-- query 11
SELECT [] AS empty_values,[NULL] AS null_values;

-- query 12
SELECT [JSON '1',NULL] AS json_values;

-- query 13
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT [JSON '1','2'] AS mixed_values;

-- query 14
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT [1,'2'] AS mixed_values;

-- query 15
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT [CAST(1 AS DECIMAL(38,0)),CAST(CAST(['bad'] AS ARRAY<INT>)[1] AS DECIMAL(38,38)),CAST(0 AS DECIMAL(38,38))] AS first_common_failure;

-- query 16
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT [CAST(CAST(['bad'] AS ARRAY<INT>)[1] AS DECIMAL(38,38)),CAST(1 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))] AS first_source_failure;

-- query 17
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT TRY_CAST([CAST(1 AS DECIMAL(38,0)),CAST(CAST(['bad'] AS ARRAY<INT>)[1] AS DECIMAL(38,38)),CAST(0 AS DECIMAL(38,38))] AS ARRAY<DECIMAL(38,38)>) AS first_common_failure;

-- query 18
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT TRY_CAST([CAST(CAST(['bad'] AS ARRAY<INT>)[1] AS DECIMAL(38,38)),CAST(1 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))] AS ARRAY<DECIMAL(38,38)>) AS first_source_failure;

-- query 19
SELECT ordinal,[int_value,small_value] AS widened FROM (VALUES ROW(CAST(1 AS INT),CAST(1 AS INT),CAST(2 AS SMALLINT)),ROW(CAST(2 AS INT),CAST(NULL AS INT),CAST(3 AS SMALLINT))) AS inputs(ordinal,int_value,small_value) ORDER BY ordinal;

-- query 20
SELECT ordinal,[decimal_value,double_value] AS rounded FROM (VALUES ROW(CAST(1 AS INT),CAST(9007199254740993.0 AS DECIMAL(20,1)),1e0),ROW(CAST(2 AS INT),CAST(4503599627370496.5 AS DECIMAL(20,1)),2e0)) AS inputs(ordinal,decimal_value,double_value) ORDER BY ordinal;

-- query 21
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT ordinal,TRY_CAST([whole_value,fraction_value] AS ARRAY<DOUBLE>) AS converted FROM (VALUES ROW(CAST(1 AS INT),CAST(0 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38))),ROW(CAST(2 AS INT),CAST(1 AS DECIMAL(38,0)),CAST(0 AS DECIMAL(38,38)))) AS inputs(ordinal,whole_value,fraction_value) ORDER BY ordinal;
