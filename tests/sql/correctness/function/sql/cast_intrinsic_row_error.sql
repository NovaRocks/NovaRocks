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

-- Strict conversion errors and the independent TRY and TRY_CAST scopes.

-- query 1
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(DOUBLE '2147483648' AS INTEGER) AS narrowed;

-- query 2
SELECT TRY_CAST(DOUBLE '2147483648' AS INTEGER) AS try_cast_range, TRY(CAST(DOUBLE '2147483648' AS INTEGER)) AS try_range;

-- query 3
SELECT CAST(DOUBLE '127' AS TINYINT) AS narrowed;

-- query 4
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(X'FF' AS VARCHAR);

-- query 5
SELECT CAST(CAST('2020-01-01' AS DATE) AS VARCHAR) AS rendered;

-- query 6
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT TRY_CAST(CAST('bad' AS INTEGER) AS BIGINT);

-- query 7
SELECT CASE WHEN false THEN CAST(DOUBLE '2147483648' AS INTEGER) ELSE 17 END AS kept;
