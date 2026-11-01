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

-- Migrated from dev/test/sql/test_function/T/test_cast
-- Test Objective:
-- 1. Validate CAST from STRING to BINARY/VARBINARY and back.
-- 2. Validate CAST from floating-point (DOUBLE/FLOAT) to integral types (BIGINT, INT, SMALLINT, TINYINT).
-- 3. Verify overflow behavior: silent truncation in default mode vs exception in ALLOW_THROW_EXCEPTION mode.
-- 4. Cover boundary values for BIGINT, INT, SMALLINT, TINYINT with positive/negative/fractional inputs.

-- query 1
-- Setup: create t1 (string columns) and t2 (binary columns) for string-to-binary cast test
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1(c1 int, c2 string, c3 string)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2(c1 int, c2 binary, c3 varbinary)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES (1, "1,2,3,4,5", "6,7,8,9");

-- query 4
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES (2, null, null);

-- query 5
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t2 SELECT * FROM t1;

-- query 6
USE ${case_db};
SELECT * FROM t2 ORDER BY c1;

-- query 7
USE ${case_db};
SELECT c1, cast(c2 as binary), cast(c2 as varbinary) FROM t1 ORDER BY c1;

-- query 8
SELECT cast("1,2,3,4,5" as binary), cast("6,7,8,9.10" as varbinary);

-- query 9
-- Setup: create t1 (binary columns) and t2 (string columns) for binary-to-string cast test
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 10
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t2;

-- query 11
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1(c1 int, c2 binary, c3 varbinary)
TBLPROPERTIES ("format-version" = "3");

-- query 12
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2(c1 int, c2 string, c3 string)
TBLPROPERTIES ("format-version" = "3");

-- query 13
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES (1, "1,2,3,4,5", "6,7,8,9");

-- query 14
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES (2, null, null);

-- query 15
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t2 SELECT * FROM t1;

-- query 16
USE ${case_db};
SELECT * FROM t2 ORDER BY c1;

-- query 17
USE ${case_db};
SELECT c1, cast(c2 as string), cast(c3 as string) FROM t1 ORDER BY c1;

-- query 18
-- Setup: create t1 for floating-to-integral cast test
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;

-- query 19
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t2;

-- query 20
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1(k1 int, d1 double, f1 float)
TBLPROPERTIES ("format-version" = "3");

-- query 21
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1
-- Seed IEEE floating values explicitly. A decimal literal followed by a
-- DECIMAL-to-DOUBLE cast can round differently at the BIGINT boundary; this
-- case exercises floating-to-integral casts, not decimal conversion.
VALUES
    (1, CAST('9223372036854775000.0' AS DOUBLE), CAST('2147483000.0' AS FLOAT)),
    (2, CAST('9223372036854775807.0' AS DOUBLE), CAST('2147483647.0' AS FLOAT)),
    (3, CAST('9223372036854775807.3' AS DOUBLE), CAST('2147483647.3' AS FLOAT)),
    (4, CAST('9223372036854775808.0' AS DOUBLE), CAST('2147483648.0' AS FLOAT)),
    (5, CAST('9223372036854775809.0' AS DOUBLE), CAST('2147483649.0' AS FLOAT)),
    (6, CAST('-9223372036854675807.0' AS DOUBLE), CAST('-2147473647.0' AS FLOAT)),
    (7, CAST('-9223372036854775807.0' AS DOUBLE), CAST('-2147483647.0' AS FLOAT)),
    (8, CAST('-9223372036854775808.0' AS DOUBLE), CAST('-2147483648.0' AS FLOAT)),
    (9, CAST('-9223372036854775809.0' AS DOUBLE), CAST('-2147483649.0' AS FLOAT)),
    (10, CAST('-9223372036854778808.0' AS DOUBLE), CAST('-2147494649.0' AS FLOAT)),
    (11, 32767, 32767),
    (12, 32768, 32768),
    (13, 32769, 32769),
    (14, -32765, -32765),
    (15, -32767, -32767),
    (16, -32768, -32768),
    (17, -32770, -32770),
    (18, 127, 127),
    (19, 128, 128),
    (20, 129, 129),
    (21, -127, -127),
    (22, -128, -128),
    (23, -129, -129),
    (24, 0.1, 0.1),
    (25, 0.9, 0.9),
    (26, -0.1, -0.1),
    (27, -0.9, -0.9),
    (28, 0.0, 0.0),
    (29, -0.0, -0.0);

-- query 22
-- Cast double to bigint, float to int (default mode, silent truncation on overflow)
USE ${case_db};
SELECT k1, d1, f1, cast(d1 as bigint), cast(f1 as int) FROM t1 ORDER BY k1;

-- query 23
-- Cast double to smallint, float to smallint
USE ${case_db};
SELECT k1, d1, f1, cast(d1 as smallint), cast(f1 as smallint) FROM t1 ORDER BY k1;

-- query 24
-- Cast double to tinyint, float to tinyint
USE ${case_db};
SELECT k1, d1, f1, cast(d1 as tinyint), cast(f1 as tinyint) FROM t1 ORDER BY k1;

-- query 25
-- ALLOW_THROW_EXCEPTION: cast double to bigint, float to int (overflow expected)
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
USE ${case_db};
SELECT /*+SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION')*/ k1, d1, f1, cast(d1 as bigint), cast(f1 as int) FROM t1 ORDER BY k1;

-- query 26
-- ALLOW_THROW_EXCEPTION: cast double to smallint, float to smallint (overflow expected)
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
USE ${case_db};
SELECT /*+SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION')*/ k1, d1, f1, cast(d1 as smallint), cast(f1 as smallint) FROM t1 WHERE k1 >= 11 ORDER BY k1;

-- query 27
-- ALLOW_THROW_EXCEPTION: cast double to tinyint, float to tinyint (overflow expected)
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
USE ${case_db};
SELECT /*+SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION')*/ k1, d1, f1, cast(d1 as tinyint), cast(f1 as tinyint) FROM t1 WHERE k1 >= 18 ORDER BY k1;

-- query 28
-- ALLOW_THROW_EXCEPTION: bigint/int boundary subset
USE ${case_db};
SELECT /*+SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION')*/ k1, d1, f1, cast(d1 as bigint), cast(f1 as int) FROM t1 WHERE k1 IN (1, 6, 7, 8, 9) ORDER BY k1;

-- query 29
-- ALLOW_THROW_EXCEPTION: smallint boundary subset
USE ${case_db};
SELECT /*+SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION')*/ k1, d1, f1, cast(d1 as smallint), cast(f1 as smallint) FROM t1 WHERE k1 IN (11, 14, 15, 16) ORDER BY k1;

-- query 30
-- ALLOW_THROW_EXCEPTION: tinyint boundary subset
USE ${case_db};
SELECT /*+SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION')*/ k1, d1, f1, cast(d1 as tinyint), cast(f1 as tinyint) FROM t1 WHERE k1 IN (18, 21, 22) ORDER BY k1;
