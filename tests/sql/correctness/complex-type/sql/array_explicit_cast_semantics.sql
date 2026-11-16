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
-- @tags=array,cast,trc5e
-- Explicit arrays use Trino conversion failures. Scalar conversion policy is independent.
-- query 1
SELECT CAST([CAST(1 AS INT),CAST(2 AS INT)] AS ARRAY<BIGINT>) AS widened;

-- query 2
SELECT CAST(['+17',NULL,' -2147483648 '] AS ARRAY<INT>) AS parsed;

-- query 3
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(['17','bad',NULL] AS ARRAY<INT>);

-- query 4
SELECT TRY_CAST(['17','bad',NULL] AS ARRAY<INT>) AS whole_array,
       TRY_CAST([NULL] AS ARRAY<INT>) AS original_null_element;

-- query 5
SELECT TRY_CAST([['17'],['bad']] AS ARRAY<ARRAY<INT>>) AS whole_nested_array;

-- query 6
SELECT CAST([9007199254740993,1e0] AS ARRAY<DECIMAL(20,0)>) AS inferred_first;

-- query 7
SELECT CAST([CAST(9007199254740993.0 AS DECIMAL(20,1)),CAST(4503599627370496.5 AS DECIMAL(20,1))] AS ARRAY<DOUBLE>) AS rounded;

-- query 8
SELECT CAST([4503599627370497e0,-4503599627370497e0,0.49999999999999994e0,-0.49999999999999994e0,1.5e0,-1.5e0] AS ARRAY<DECIMAL(20,0)>) AS rounded;

-- query 9
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST([1e20] AS ARRAY<DECIMAL(20,0)>);

-- query 10
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(['2147483648'] AS ARRAY<INT>);

-- query 11
SELECT ordinal,TRY_CAST([token] AS ARRAY<INT>) AS converted
FROM (VALUES (1,'bad'),(2,'17'),(3,NULL),(4,'-3')) AS inputs(ordinal,token)
ORDER BY ordinal;

-- query 12
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST([token] AS ARRAY<INT>) AS converted
FROM (VALUES ('17'),('bad')) AS inputs(token);

-- query 13
SELECT CAST([CAST(1 AS DECIMAL(4,0)),CAST(999 AS DECIMAL(4,0))] AS ARRAY<DECIMAL(3,0)>) AS narrowed;

-- query 14
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST([CAST(1000 AS DECIMAL(4,0))] AS ARRAY<DECIMAL(3,0)>);

-- query 15
SELECT TRY_CAST([CAST(1000 AS DECIMAL(4,0))] AS ARRAY<DECIMAL(3,0)>) AS whole_array;

-- query 16
SELECT CAST([CAST(12.5 AS DECIMAL(4,1)),CAST(-12.5 AS DECIMAL(4,1))] AS ARRAY<DECIMAL(2,0)>) AS half_up;

-- query 17
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST([CAST(999.5 AS DECIMAL(4,1))] AS ARRAY<DECIMAL(3,0)>);

-- query 18
SELECT TRY_CAST([CAST(999.5 AS DECIMAL(4,1))] AS ARRAY<DECIMAL(3,0)>) AS whole_array;

-- query 19
SELECT CAST([CAST('-9223372036854775808' AS BIGINT),CAST('9223372036854775807' AS BIGINT)] AS ARRAY<DECIMAL(20,0)>) AS signed_bounds;

-- query 20
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST([1] AS ARRAY<DECIMAL(18,18)>);

-- query 21
SELECT TRY_CAST([1] AS ARRAY<DECIMAL(18,18)>) AS whole_array;
