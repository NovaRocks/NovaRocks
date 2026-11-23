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

-- Binary and text columns preserve their stored values and reject cross-type
-- assignments and casts. Floating inputs retain their IEEE widths, with
-- separate successful, failing and TRY_CAST calls over the same stored rows.

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1(c1 int, c2 VARCHAR, c3 VARCHAR) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2(c1 int, c2 VARBINARY, c3 varbinary) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES ROW(1, '1,2,3,4,5', '6,7,8,9');

-- query 4
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES ROW(2, null, null);

-- query 5
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
INSERT INTO ${case_db}.t2 SELECT * FROM ${case_db}.t1;

-- query 6
INSERT INTO ${case_db}.t2 VALUES ROW(1, X'312C322C332C342C35', X'362C372C382C39'), ROW(2, NULL, NULL);
SELECT * FROM ${case_db}.t2 ORDER BY c1;

-- query 7
SELECT c1, CAST(c2 AS VARBINARY), CAST(c3 AS VARBINARY) FROM ${case_db}.t1 ORDER BY c1;

-- query 8
SELECT CAST('1,2,3,4,5' AS VARBINARY), CAST('6,7,8,9.10' AS VARBINARY);

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
CREATE TABLE t1(c1 int, c2 VARBINARY, c3 varbinary) WITH (format_version = 3);

-- query 12
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2(c1 int, c2 VARCHAR, c3 VARCHAR) WITH (format_version = 3);

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 VALUES ROW(1, X'312C322C332C342C35', X'362C372C382C39');

-- query 14
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES ROW(2, null, null);

-- query 15
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
INSERT INTO ${case_db}.t2 SELECT * FROM ${case_db}.t1;

-- query 16
INSERT INTO ${case_db}.t2 VALUES ROW(1, '1,2,3,4,5', '6,7,8,9'), ROW(2, NULL, NULL);
SELECT * FROM ${case_db}.t2 ORDER BY c1;

-- query 17
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT c1, CAST(c2 AS VARCHAR), CAST(c3 AS VARCHAR) FROM ${case_db}.t1 ORDER BY c1;

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
CREATE TABLE t1(k1 int, d1 double, f1 REAL) WITH (format_version = 3);

-- query 21
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1
-- Seed IEEE floating values explicitly. A decimal literal followed by a
-- DECIMAL-to-DOUBLE cast can round differently at the BIGINT boundary; this
-- case exercises floating-to-integral casts, not decimal conversion.
VALUES ROW(1, CAST('9223372036854775000.0' AS DOUBLE), CAST('2147483000.0' AS REAL)), ROW(2, CAST('9223372036854775807.0' AS DOUBLE), CAST('2147483647.0' AS REAL)), ROW(3, CAST('9223372036854775807.3' AS DOUBLE), CAST('2147483647.3' AS REAL)), ROW(4, CAST('9223372036854775808.0' AS DOUBLE), CAST('2147483648.0' AS REAL)), ROW(5, CAST('9223372036854775809.0' AS DOUBLE), CAST('2147483649.0' AS REAL)), ROW(6, CAST('-9223372036854675807.0' AS DOUBLE), CAST('-2147473647.0' AS REAL)), ROW(7, CAST('-9223372036854775807.0' AS DOUBLE), CAST('-2147483647.0' AS REAL)), ROW(8, CAST('-9223372036854775808.0' AS DOUBLE), CAST('-2147483648.0' AS REAL)), ROW(9, CAST('-9223372036854775809.0' AS DOUBLE), CAST('-2147483649.0' AS REAL)), ROW(10, CAST('-9223372036854778808.0' AS DOUBLE), CAST('-2147494649.0' AS REAL)), ROW(11, 32767, 32767), ROW(12, 32768, 32768), ROW(13, 32769, 32769), ROW(14, -32765, -32765), ROW(15, -32767, -32767), ROW(16, -32768, -32768), ROW(17, -32770, -32770), ROW(18, 127, 127), ROW(19, 128, 128), ROW(20, 129, 129), ROW(21, -127, -127), ROW(22, -128, -128), ROW(23, -129, -129), ROW(24, DECIMAL '0.1', DECIMAL '0.1'), ROW(25, DECIMAL '0.9', DECIMAL '0.9'), ROW(26, (- (DECIMAL '0.1')), (- (DECIMAL '0.1'))), ROW(27, (- (DECIMAL '0.9')), (- (DECIMAL '0.9'))), ROW(28, DECIMAL '0.0', DECIMAL '0.0'), ROW(29, (- (DECIMAL '0.0')), (- (DECIMAL '0.0')));

-- query 22
SELECT k1, d1, f1, try_cast(d1 as bigint), try_cast(f1 as int) FROM ${case_db}.t1 ORDER BY k1;

-- query 23
SELECT k1, d1, f1, try_cast(d1 as smallint), try_cast(f1 as smallint) FROM ${case_db}.t1 ORDER BY k1;

-- query 24
SELECT k1, d1, f1, try_cast(d1 as tinyint), try_cast(f1 as tinyint) FROM ${case_db}.t1 ORDER BY k1;

-- query 25
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT k1, d1, f1, cast(d1 as bigint), cast(f1 as int) FROM ${case_db}.t1 ORDER BY k1;

-- query 26
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT k1, d1, f1, cast(d1 as smallint), cast(f1 as smallint) FROM ${case_db}.t1 WHERE k1 >= 11 ORDER BY k1;

-- query 27
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT k1, d1, f1, cast(d1 as tinyint), cast(f1 as tinyint) FROM ${case_db}.t1 WHERE k1 >= 18 ORDER BY k1;

-- query 28
SELECT k1, d1, f1, cast(d1 as bigint), cast(f1 as int) FROM ${case_db}.t1 WHERE k1 IN (1, 6, 7, 8, 9) ORDER BY k1;

-- query 29
SELECT k1, d1, f1, cast(d1 as smallint), cast(f1 as smallint) FROM ${case_db}.t1 WHERE k1 IN (11, 14, 15, 16) ORDER BY k1;

-- query 30
SELECT k1, d1, f1, cast(d1 as tinyint), cast(f1 as tinyint) FROM ${case_db}.t1 WHERE k1 IN (18, 21, 22) ORDER BY k1;
