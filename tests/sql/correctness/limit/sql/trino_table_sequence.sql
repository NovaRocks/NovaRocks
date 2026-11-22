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

-- query 1
SELECT count(*) AS rows, min(sequential_number) AS first_value,
       max(sequential_number) AS last_value
FROM TABLE(sequence(stop => 10000));

-- query 2
SELECT x FROM TABLE(system.builtin.sequence(step => -2, "STOP" => 1, start => 6)) AS s(x)
ORDER BY x;

-- query 3
SELECT sequential_number FROM TABLE(sequence(5, 5, 0));

-- query 4
SELECT x FROM TABLE(sequence(CAST('-9223372036854775808' AS BIGINT), CAST('-9223372036854775806' AS BIGINT))) AS s(x)
ORDER BY x;

-- query 5
SELECT x FROM TABLE(sequence(CAST('9223372036854775807' AS BIGINT), CAST('-9223372036854775808' AS BIGINT), CAST('-9223372036854775808' AS BIGINT))) AS s(x)
ORDER BY x;

-- query 6
SELECT count(*) AS rows
FROM TABLE(sequence(stop => 3 + CAST(floor(random()) AS BIGINT)));

-- query 7
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT * FROM TABLE(sequence(4, 1, 1));

-- query 8
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT * FROM TABLE(sequence(1, 2, 0));

-- query 9
-- @expect_trino_error=MISSING_ARGUMENT,USER_ERROR
SELECT * FROM TABLE(sequence(3));

-- query 10
-- @expect_trino_error=EXPRESSION_NOT_CONSTANT,USER_ERROR
SELECT * FROM TABLE(sequence(stop => (SELECT 3)));

-- query 11
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT * FROM TABLE(sequence(stop => '3'));

-- query 12
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT sequence.sequential_number FROM TABLE(sequence(0, 3));

-- query 13
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT * FROM TABLE(sequence("stop" => 3));

-- query 14
-- @result_contains=GENERATE_SERIES(0, 16, 1)
EXPLAIN SELECT * FROM TABLE(sequence(stop => 20 - 4));
