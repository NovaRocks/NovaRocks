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

-- The declared Trino precision boundary applies to scalar and nested types.
-- @order_sensitive=true

-- query 1
SELECT CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0)) AS p38;

-- query 2
SELECT CAST('-0.12345678901234567890123456789012345678' AS DECIMAL(38,38)) AS fraction;

-- query 3
SELECT CAST(ARRAY['123456789012345678901234567890123456.78',NULL] AS ARRAY(DECIMAL(38,2))) AS array_decimal;

-- query 4
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT CAST('200000000000000000000000000000000000001' AS DECIMAL(39,0));

-- query 5
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(NULL AS DEC(60,2));

-- query 6
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(NULL AS NUMERIC(76,76));

-- query 7
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT TRY_CAST(ARRAY['1.23'] AS ARRAY(DECIMAL(60,2)));

-- query 8
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT CAST(NULL AS MAP(INTEGER,DECIMAL(60,2)));

-- query 9
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT CAST(NULL AS ROW(a DECIMAL(60,2)));

-- query 10
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT CAST(NULL AS DECIMAL(294,0));

-- query 11
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(NULL AS DECIMAL128(60,2));

-- query 12
SELECT 0.00000000000000000000000000000000000001 AS fraction,
       DECIMAL '0001.20' AS leading_zeroes;

-- query 13
SELECT ARRAY[0.1,0.25] AS decimals,
       0.1 = 0.25 AS equal,
       0.1 IN (0.25) AS included,
       0.1 BETWEEN 0.25 AND 0.75 AS within_range,
       CASE 0.1 WHEN 0.25 THEN 1 ELSE 0 END AS chosen;

-- query 14
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT 0.000000000000000000000000000000000000001;
