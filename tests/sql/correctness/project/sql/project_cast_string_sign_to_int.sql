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

-- A standalone sign is invalid for every standard signed integer width.

-- query 1
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('-' AS tinyint);

-- query 2
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('-' AS smallint);

-- query 3
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('-' AS int);

-- query 4
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('-' AS bigint);

-- query 5
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST('-' AS largeint);

-- query 6
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('+' AS tinyint);

-- query 7
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('+' AS smallint);

-- query 8
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('+' AS int);

-- query 9
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('+' AS bigint);

-- query 10
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST('+' AS largeint);

-- query 11
SELECT TRY_CAST('-' AS TINYINT), TRY_CAST('-' AS SMALLINT), TRY_CAST('-' AS INTEGER), TRY_CAST('-' AS BIGINT), TRY_CAST('+' AS TINYINT), TRY_CAST('+' AS SMALLINT), TRY_CAST('+' AS INTEGER), TRY_CAST('+' AS BIGINT);
