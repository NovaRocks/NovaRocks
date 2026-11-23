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

-- Retired declarations fail before a cast, calculation, or table write.

-- query 1
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
CREATE TABLE ${case_db}.retired_decimal (d DECIMAL(50,15)) WITH (format_version = 3);

-- query 2
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT TRY_CAST('1.25' AS DECIMAL256(50,15));

-- query 3
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT CAST(ARRAY['1.25'] AS ARRAY(DECIMAL(55,4)));

-- query 4
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT TRY_CAST(NULL AS ROW(d DECIMAL(50,15)));

-- query 5
SELECT CAST('12345678901234567890123.123456789012345' AS DECIMAL(38,15)) AS exact_value, TRY_CAST('123456789012345678901234.123456789012345' AS DECIMAL(38,15)) AS overflow_value;
