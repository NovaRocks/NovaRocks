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

-- @order_sensitive=true
-- query 1
SELECT k, TRY_CAST(s AS INTEGER) AS value
FROM (VALUES (1, '42'), (2, 'bad'), (3, NULL)) t(k,s)
ORDER BY k;

-- query 2
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT a[i] FROM (VALUES (ARRAY[11,21],1),(ARRAY[12,22],3),(ARRAY[13,23],2)) t(a,i);

-- query 3
SELECT k, TRY(a[i]) AS value
FROM (VALUES (1,ARRAY[11,21],1),(2,ARRAY[12,22],3),(3,ARRAY[13,23],2)) t(k,a,i)
ORDER BY k;

-- query 4
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT TRY_CAST(a[i] AS BIGINT)
FROM (VALUES (ARRAY[11,21],1),(ARRAY[12,22],3),(ARRAY[13,23],2)) t(a,i);

-- query 5
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT TRY_CAST(CAST('bad' AS INTEGER) AS BIGINT);

-- query 6
SELECT '甲乙' AS literal, CAST('甲乙x' AS VARCHAR(2)) AS narrowed,
       CAST('é' AS CHAR(4)) AS padded, CAST(CAST('é' AS CHAR(4)) AS VARCHAR(4)) AS trimmed;

-- query 7
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT length(BIGINT '123');

-- query 8
SELECT CAST(length(NULL) AS BIGINT) AS empty_length, CAST(length('abc') AS BIGINT) AS ascii_length;

-- query 9
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(NULL AS ARRAY(LARGEINT));

-- query 10
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT CAST(NULL AS DECIMAL(39,0));

-- query 11
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT '42' = 42;

-- query 12
SELECT CAST(16777217 AS INTEGER) = REAL '16777216' AS coerced,
       COALESCE(CAST(1 AS SMALLINT), DECIMAL '0.1') AS common_decimal;

-- query 13
SELECT * FROM (VALUES (INTEGER '16777217')) l(k)
JOIN (VALUES (REAL '16777216')) r(k) USING (k);

-- query 14
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT l.k FROM (VALUES (INTEGER '16777217')) l(k)
JOIN (VALUES (REAL '16777216')) r(k) USING (k);

-- query 15
SELECT k FROM (VALUES (SMALLINT '1')) l(k)
FULL JOIN (VALUES (BIGINT '2')) r(k) USING (k)
ORDER BY k;

-- query 16
SELECT CAST(CAST('{"k":1}' AS JSON) AS VARCHAR) AS original_text,
       TRY_CAST(ARRAY['17','bad',NULL] AS ARRAY(INTEGER)) AS array_value;

-- query 17
SELECT TRY(1 / 0) AS division, TRY(BIGINT '9223372036854775807' + BIGINT '1') AS overflow;

-- query 18
SELECT k, TRY(12 / divisor) AS division, TRY(n + BIGINT '1') AS addition
FROM (VALUES (1, 3, BIGINT '40'), (2, 0, BIGINT '9223372036854775807'),
             (3, 2, CAST(NULL AS BIGINT))) t(k, divisor, n)
ORDER BY k;

-- query 19
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT TRY_CAST(12 / divisor AS BIGINT) FROM (VALUES (3), (0)) t(divisor);

-- query 20
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT TRY_CAST(n + BIGINT '1' AS VARCHAR)
FROM (VALUES (BIGINT '40'), (BIGINT '9223372036854775807')) t(n);

-- query 21
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT sum(x) FROM (VALUES '2') t(x);

-- query 22
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT avg(x) FROM (VALUES '2') t(x);

-- query 23
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT sum(DISTINCT x) FROM (VALUES '2') t(x);

-- query 24
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT bool_or(x) OVER () FROM (VALUES 2) t(x);

-- query 25
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT bool_or(x) FROM (VALUES 2) t(x);

-- query 26
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT count_if(x) FROM (VALUES 2) t(x);

-- query 27
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT date_trunc('week', BIGINT '2');

-- query 28
SELECT sum(CAST(x AS DOUBLE)) AS total, avg(CAST(x AS DOUBLE)) AS mean,
       bool_or(CAST(x AS BOOLEAN)) AS any_true, count_if(NULL) AS counted,
       date_trunc('week', DATE '2026-11-22') AS week
FROM (VALUES '1', '0') t(x);

-- query 29
SELECT k, bool_or(CAST(x AS BOOLEAN)) OVER () AS any_true
FROM (VALUES (1, BIGINT '1'), (2, NULL)) t(k, x)
ORDER BY k;
