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

-- Test Objective: ABS freezes the promoted signed integer output before execution.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.abs_result_width_input (
    id INT, i8 TINYINT, i16 SMALLINT, i32 INT, i64 BIGINT, i128 LARGEINT,
    d DECIMAL(18,3), f FLOAT, g DOUBLE
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.abs_result_width_input VALUES
(1, -128, -32768, -2147483648, -9223372036854775808, -170141183460469231731687303715884105728, -12.345, -1.5, -2.5),
(2, -7, -7, -7, -7, -7, -7.125, -1.5, -2.5),
(3, 0, 0, 0, 0, 0, 0.000, 0, 0),
(4, 7, 7, 7, 7, 7, 7.125, 1.5, 2.5),
(5, 127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727, 12.345, 1.5, 2.5),
(6, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);

-- query 2
-- Explicit casts freeze narrow input domains independently of Iceberg INT storage.
SELECT id, ABS(CAST(i8 AS TINYINT)) AS abs_i8,
       ABS(CAST(i16 AS SMALLINT)) AS abs_i16, ABS(i32) AS abs_i32,
       ABS(i64) AS abs_i64, ABS(i128) AS abs_i128
FROM ${case_db}.abs_result_width_input ORDER BY id;

-- query 3
WITH typed_input AS (
    SELECT id, CAST(i8 AS TINYINT) AS i8 FROM ${case_db}.abs_result_width_input
)
SELECT id,
       CASE WHEN i8 > 0 THEN CAST(i8 AS DECIMAL(38,15))
            WHEN i8 < 0 THEN CAST(ABS(i8) AS DECIMAL(38,15))
            ELSE 0.000000000000000 END AS case_abs_i8
FROM typed_input ORDER BY id;

-- query 4
SELECT id, ABS(d) AS abs_decimal, ABS(f) AS abs_float, ABS(g) AS abs_double
FROM ${case_db}.abs_result_width_input ORDER BY id;

-- query 5
SELECT ABS(CAST(-128 AS TINYINT)) AS literal_i8,
       ABS(CAST(-32768 AS SMALLINT)) AS literal_i16,
       ABS(CAST(-2147483648 AS INT)) AS literal_i32,
       ABS(CAST('-9223372036854775808' AS BIGINT)) AS literal_i64,
       ABS(CAST('-170141183460469231731687303715884105728' AS LARGEINT)) AS literal_i128;

-- query 6
SELECT ABS(CAST(NULL AS TINYINT)) AS null_i8,
       ABS(CAST(NULL AS SMALLINT)) AS null_i16,
       ABS(CAST(NULL AS INT)) AS null_i32,
       ABS(CAST(NULL AS BIGINT)) AS null_i64,
       ABS(CAST(NULL AS LARGEINT)) AS null_i128;
