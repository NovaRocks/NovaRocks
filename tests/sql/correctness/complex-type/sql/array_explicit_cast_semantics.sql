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
SELECT CAST(ARRAY[4503599627370497.0E0, (- (4503599627370497.0E0)), 0.49999999999999994E0, (- (0.49999999999999994E0)), 1.5E0, (- (1.5E0))] AS ARRAY(DECIMAL(20, 0))) AS rounded;

-- query 9
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST([1e20] AS ARRAY<DECIMAL(20,0)>);

-- query 10
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(['2147483648'] AS ARRAY<INT>);

-- query 11
SELECT ordinal,TRY_CAST([token] AS ARRAY<INT>) AS converted
FROM (VALUES ROW(1,'bad'),ROW(2,'17'),ROW(3,NULL),ROW(4,'-3')) AS inputs(ordinal,token)
ORDER BY ordinal;

-- query 12
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST([token] AS ARRAY<INT>) AS converted
FROM (VALUES ROW('17'),ROW('bad')) AS inputs(token);

-- query 13
SELECT CAST([CAST(1 AS DECIMAL(4,0)),CAST(999 AS DECIMAL(4,0))] AS ARRAY<DECIMAL(3,0)>) AS narrowed;

-- query 14
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST([CAST(1000 AS DECIMAL(4,0))] AS ARRAY<DECIMAL(3,0)>);

-- query 15
SELECT TRY_CAST([CAST(1000 AS DECIMAL(4,0))] AS ARRAY<DECIMAL(3,0)>) AS whole_array;

-- query 16
SELECT CAST(ARRAY[CAST(DECIMAL '12.5' AS DECIMAL(4, 1)), CAST((- (DECIMAL '12.5')) AS DECIMAL(4, 1))] AS ARRAY(DECIMAL(2, 0))) AS half_up;

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

-- query 22
SELECT CAST([CAST(-128 AS TINYINT),CAST(127 AS TINYINT),CAST(NULL AS TINYINT)] AS ARRAY<VARCHAR>) AS tiny_text,
       CAST([CAST(-32768 AS SMALLINT),CAST(32767 AS SMALLINT),CAST(NULL AS SMALLINT)] AS ARRAY<VARCHAR>) AS small_text,
       CAST([CAST(-2147483648 AS INT),CAST(2147483647 AS INT),CAST(NULL AS INT)] AS ARRAY<VARCHAR>) AS integer_text,
       CAST([CAST('-9223372036854775808' AS BIGINT),CAST('9223372036854775807' AS BIGINT),CAST(NULL AS BIGINT)] AS ARRAY<VARCHAR>) AS big_text;

-- query 23
SELECT CAST(['1970-01-01','1969-12-31','2000-02-29','0000-01-01','-1-01-01',NULL] AS ARRAY<DATE>) AS dates;

-- query 24
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(['20230102'] AS ARRAY<DATE>);

-- query 25
SELECT TRY_CAST(['1970-01-01','20230102',NULL] AS ARRAY<DATE>) AS whole_array,
       TRY_CAST([NULL] AS ARRAY<DATE>) AS original_null_element;

-- query 26
SELECT ordinal,CAST([token] AS ARRAY<DATE>) AS dates
FROM (VALUES ROW(CAST(1 AS INT),'-5877641-06-23'),
             ROW(CAST(2 AS INT),'+5881580-07-11')) AS inputs(ordinal,token)
ORDER BY ordinal;

-- query 27
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(['+5881580-07-12'] AS ARRAY<DATE>);
