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

-- Integer conversions preserve typed extrema and nulls.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 BIGINT,
    c_tinyint TINYINT,
    c_int INTEGER,
    c_bigint BIGINT,
    c_wide DECIMAL(38,0)
) WITH (format_version = 3);
INSERT INTO ${case_db}.t1 VALUES
    ROW(BIGINT '1', TINYINT '127', INTEGER '2147483647', BIGINT '9223372036854775807', DECIMAL '99999999999999999999999999999999999999'),
    ROW(BIGINT '2', TINYINT '-128', INTEGER '-2147483648', BIGINT '-9223372036854775808', DECIMAL '-99999999999999999999999999999999999999'),
    ROW(BIGINT '3', NULL, NULL, NULL, NULL),
    ROW(BIGINT '4', TINYINT '0', INTEGER '0', BIGINT '0', DECIMAL '0'),
    ROW(BIGINT '5', TINYINT '1', INTEGER '1', BIGINT '1', DECIMAL '1'),
    ROW(BIGINT '6', TINYINT '-1', INTEGER '-1', BIGINT '-1', DECIMAL '-1'),
    ROW(BIGINT '7', TINYINT '12', INTEGER '214748364', BIGINT '922337203685477580', DECIMAL '17014118346046923173168730371588410572'),
    ROW(BIGINT '8', TINYINT '-12', INTEGER '-214748364', BIGINT '-922337203685477580', DECIMAL '-17014118346046923173168730371588410572');

-- query 2
SELECT k1,
       TRY_CAST(c_tinyint AS DECIMAL(9,0)),
       TRY_CAST(c_int AS DECIMAL(9,0)),
       TRY_CAST(c_bigint AS DECIMAL(9,0)),
       TRY_CAST(c_wide AS DECIMAL(9,0))
FROM ${case_db}.t1 ORDER BY k1;

-- query 3
SELECT k1,
       TRY_CAST(c_tinyint AS DECIMAL(9,1)),
       TRY_CAST(c_int AS DECIMAL(9,1)),
       TRY_CAST(c_bigint AS DECIMAL(9,1)),
       TRY_CAST(c_wide AS DECIMAL(9,1))
FROM ${case_db}.t1 ORDER BY k1;

-- query 4
SELECT k1,
       TRY_CAST(c_tinyint AS DECIMAL(27,0)),
       TRY_CAST(c_int AS DECIMAL(27,0)),
       TRY_CAST(c_bigint AS DECIMAL(27,0)),
       TRY_CAST(c_wide AS DECIMAL(27,0))
FROM ${case_db}.t1 ORDER BY k1;

-- query 5
SELECT k1,
       TRY_CAST(c_tinyint AS DECIMAL(27,1)),
       TRY_CAST(c_int AS DECIMAL(27,1)),
       TRY_CAST(c_bigint AS DECIMAL(27,1)),
       TRY_CAST(c_wide AS DECIMAL(27,1))
FROM ${case_db}.t1 ORDER BY k1;

-- query 6
SELECT k1,
       CAST(c_tinyint AS DECIMAL(38,0)),
       CAST(c_int AS DECIMAL(38,0)),
       CAST(c_bigint AS DECIMAL(38,0)),
       CAST(c_wide AS DECIMAL(38,0))
FROM ${case_db}.t1 ORDER BY k1;

-- query 7
SELECT k1,
       TRY_CAST(c_tinyint AS DECIMAL(38,1)),
       TRY_CAST(c_int AS DECIMAL(38,1)),
       TRY_CAST(c_bigint AS DECIMAL(38,1)),
       TRY_CAST(c_wide AS DECIMAL(38,1))
FROM ${case_db}.t1 ORDER BY k1;

-- query 8
SELECT k1, c_tinyint * 0, c_int * 0, c_bigint * 0
FROM ${case_db}.t1 ORDER BY k1;

-- query 9
SELECT k1, c_tinyint * 1, c_int * 1, c_bigint * 1
FROM ${case_db}.t1 ORDER BY k1;

-- query 10
SELECT k1, CAST(c_tinyint * 0 AS DECIMAL(38,0)), CAST(c_int * 0 AS DECIMAL(38,0)), CAST(c_bigint * 0 AS DECIMAL(38,0))
FROM ${case_db}.t1 ORDER BY k1;

-- query 11
SELECT k1, CAST(c_tinyint * 1 AS DECIMAL(38,0)), CAST(c_int * 1 AS DECIMAL(38,0)), CAST(c_bigint * 1 AS DECIMAL(38,0))
FROM ${case_db}.t1 ORDER BY k1;

-- query 12
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(c_int AS DECIMAL(9,0)) FROM ${case_db}.t1 WHERE k1 = 1;

-- query 13
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT CAST(NULL AS LARGEINT);

-- query 14
-- Trino 484 admits this coefficient through an overflowing absolute value.
-- A successful DECIMAL result must fit its declared precision.
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT CAST(c_bigint AS DECIMAL(9,0)) FROM ${case_db}.t1 WHERE k1 = 2;

-- query 15
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t1;
