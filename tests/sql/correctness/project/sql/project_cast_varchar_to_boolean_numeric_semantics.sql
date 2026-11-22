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

-- Only the admitted VARCHAR boolean spellings yield values.

-- query 1
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('2' AS BOOLEAN);

-- query 2
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('-1' AS BOOLEAN);

-- query 3
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('+0' AS BOOLEAN);

-- query 4
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('2147483648' AS BOOLEAN);

-- query 5
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(' 2' AS BOOLEAN);

-- query 6
SELECT TRY_CAST('2' AS BOOLEAN), TRY_CAST('-1' AS BOOLEAN), TRY_CAST('+0' AS BOOLEAN), CAST('false' AS BOOLEAN), CAST('TRUE' AS BOOLEAN), CAST('t' AS BOOLEAN), TRY_CAST('2147483648' AS BOOLEAN), TRY_CAST(' 2' AS BOOLEAN);

-- query 7
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT IF('2', 1, 0);

-- query 8
SELECT IF(NULL, 1, 0);

-- query 9
SELECT IF(CAST('t' AS BOOLEAN), 1, 0),IF(CAST('false' AS BOOLEAN),1,0),IF(TRY_CAST('2' AS BOOLEAN),1,0);

-- query 10
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT 1 WHERE 1;

-- query 11
SELECT 1 WHERE NULL;

-- query 12
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT COUNT(*) HAVING 1;

-- query 13
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT * FROM (VALUES 1) l JOIN (VALUES 2) r ON 1;
