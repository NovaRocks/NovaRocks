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

-- Test Objective: SQL DECIMAL freezes exact legal carriers before explicit output casts.
-- @order_sensitive=true

-- query 1
SELECT CAST(CAST('200000000000000000000000000000000000001' AS DECIMAL(39,0)) AS VARCHAR) AS p39;

-- query 2
SELECT CAST(CAST('1000000000000000000000000000000000000000000000001.23' AS DEC(60,2)) AS VARCHAR) AS p60;

-- query 3
SELECT CAST(CAST('-0.1234567890123456789012345678901234567890123456789012345678901234567890123456' AS NUMERIC(76,76)) AS VARCHAR) AS p76;

-- query 4
SELECT CAST(CAST(['1000000000000000000000000000000000000000000000001.23'] AS ARRAY<DECIMAL(60,2)>)[1] AS VARCHAR) AS array_decimal;

-- query 5
SELECT CAST(CAST(MAP{1:'1000000000000000000000000000000000000000000000001.23'} AS MAP<INT,DECIMAL(60,2)>)[1] AS VARCHAR) AS map_decimal;

-- query 6
SELECT CAST(CAST(ROW('1000000000000000000000000000000000000000000000001.23') AS STRUCT<a DECIMAL(60,2)>)['a'] AS VARCHAR) AS struct_decimal;

-- query 7
-- @expect_error=decimal precision 294 must be between 1 and 76
SELECT CAST(NULL AS DECIMAL(294,0));

-- query 8
-- @expect_error=decimal128 precision 60 must be between 1 and 38
SELECT CAST(NULL AS DECIMAL128(60,2));
