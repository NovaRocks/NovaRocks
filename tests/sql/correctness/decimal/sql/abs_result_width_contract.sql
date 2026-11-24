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

-- ABS preserves the signed input width and rejects its unrepresentable minimum.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.abs_result_width_input (
    id INTEGER, i8 TINYINT, i16 SMALLINT, i32 INTEGER, i64 BIGINT,
    d DECIMAL(18,3), f REAL, g DOUBLE
) WITH (format_version = 3);
INSERT INTO ${case_db}.abs_result_width_input VALUES
    ROW(1, -128, -32768, -2147483648, BIGINT '-9223372036854775808', DECIMAL '-12.345', REAL '-1.5', DOUBLE '-2.5'),
    ROW(2, -7, -7, -7, -7, DECIMAL '-7.125', REAL '-1.5', DOUBLE '-2.5'),
    ROW(3, 0, 0, 0, 0, DECIMAL '0.000', REAL '0.0', DOUBLE '0.0'),
    ROW(4, 7, 7, 7, 7, DECIMAL '7.125', REAL '1.5', DOUBLE '2.5'),
    ROW(5, 127, 32767, 2147483647, BIGINT '9223372036854775807', DECIMAL '12.345', REAL '1.5', DOUBLE '2.5'),
    ROW(6, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- query 2
-- Explicit casts restore narrow input domains after Iceberg INT storage.
SELECT id, TRY(ABS(CAST(i8 AS TINYINT))) AS abs_i8,
       TRY(ABS(CAST(i16 AS SMALLINT))) AS abs_i16, TRY(ABS(i32)) AS abs_i32,
       TRY(ABS(i64)) AS abs_i64
FROM ${case_db}.abs_result_width_input ORDER BY id;

-- query 3
-- When a wider absolute value is needed, widen the input before applying ABS.
WITH typed_input AS (
    SELECT id, CAST(i8 AS TINYINT) AS i8 FROM ${case_db}.abs_result_width_input
)
SELECT id,
       CASE WHEN i8 > 0 THEN CAST(i8 AS DECIMAL(38,15))
            WHEN i8 < 0 THEN CAST(ABS(CAST(i8 AS SMALLINT)) AS DECIMAL(38,15))
            ELSE 0.000000000000000 END AS case_abs_i8
FROM typed_input ORDER BY id;

-- query 4
SELECT id, ABS(d) AS abs_decimal, ABS(f) AS abs_float, ABS(g) AS abs_double
FROM ${case_db}.abs_result_width_input ORDER BY id;

-- query 5
SELECT ABS(TINYINT '-7') AS literal_i8, ABS(SMALLINT '-7') AS literal_i16,
       ABS(INTEGER '-7') AS literal_i32, ABS(BIGINT '-7') AS literal_i64;

-- query 6
SELECT ABS(CAST(NULL AS TINYINT)) AS null_i8,
       ABS(CAST(NULL AS SMALLINT)) AS null_i16,
       ABS(CAST(NULL AS INTEGER)) AS null_i32,
       ABS(CAST(NULL AS BIGINT)) AS null_i64;

-- query 7
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT ABS(CAST(i8 AS TINYINT)) FROM ${case_db}.abs_result_width_input WHERE id = 1;

-- query 8
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT ABS(CAST(i16 AS SMALLINT)) FROM ${case_db}.abs_result_width_input WHERE id = 1;

-- query 9
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT ABS(i32) FROM ${case_db}.abs_result_width_input WHERE id = 1;

-- query 10
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT ABS(i64) FROM ${case_db}.abs_result_width_input WHERE id = 1;

-- query 11
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT ABS(CAST('-170141183460469231731687303715884105728' AS LARGEINT));
