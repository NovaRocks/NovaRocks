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

-- @order_sensitive=true
-- @tags=decimal,overflow,session
-- Test Objective: Freeze independent Decimal error policy per operation and preserve NULL controls.

-- query 1
-- @skip_result_check=true
SET sql_mode=32;
SET decimal_overflow_to_double=false;
CREATE TABLE ${case_db}.decimal_error_policy (k INT,a DECIMAL(38,0),b DECIMAL(2,1))
TBLPROPERTIES ("format-version"="3");
INSERT INTO ${case_db}.decimal_error_policy VALUES
(1,1,1.0),(2,99999999999999999999999999999999999999,0.1),(3,NULL,0.0),(4,0,0.0);

-- query 2
SELECT k,a+1 AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 3
-- @skip_result_check=true
-- @expect_error=The 'add' operation involving decimal values overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ k,a+1 AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 4
SELECT -CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0))-1 AS v;

-- query 5
-- @skip_result_check=true
-- @expect_error=The 'sub' operation involving decimal values overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ -CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0))-1 AS v;

-- query 6
SELECT k,a*2 AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 7
-- @skip_result_check=true
-- @expect_error=The 'mul' operation involving decimal values overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ k,a*2 AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 8
SELECT k,a/b AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 9
-- @skip_result_check=true
-- @expect_error=The 'div' operation involving decimal values overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ k,a/b AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 10
SELECT k,a%b AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 11
-- @skip_result_check=true
-- @expect_error=The 'mod' operation involving decimal values overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ k,a%b AS v FROM ${case_db}.decimal_error_policy ORDER BY k;

-- query 12
-- @skip_result_check=true
SET sql_mode='ERROR_IF_OVERFLOW';

-- query 13
SELECT a/b AS div_zero,a%b AS mod_zero FROM ${case_db}.decimal_error_policy WHERE k=4;

-- query 14
SELECT /*+ SET_VAR(sql_mode=32) */ a+1 AS v FROM ${case_db}.decimal_error_policy WHERE k=2;

-- query 15
-- @skip_result_check=true
-- @expect_error=The 'add' operation involving decimal values overflows
SELECT a+1 AS v FROM ${case_db}.decimal_error_policy WHERE k=2;

-- query 16
-- @skip_result_check=true
SET sql_mode=32;

-- query 17
SELECT CAST(1000000000 AS DECIMAL(9,0)) AS narrowed;

-- query 18
-- @skip_result_check=true
-- @expect_error=numeric type cast involving decimal overflows
SELECT /*+ SET_VAR(sql_mode=34359738368) */ CAST(1000000000 AS DECIMAL(9,0)) AS narrowed;

-- query 19
SELECT CAST(CAST(999.995 AS DECIMAL(6,3)) AS DECIMAL(5,2)) AS carry;

-- query 20
-- @skip_result_check=true
-- @expect_error=numeric type cast involving decimal overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(CAST(999.995 AS DECIMAL(6,3)) AS DECIMAL(5,2)) AS carry;

-- query 21
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(NULL AS DECIMAL(5,2)) AS input_null;

-- query 22
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(CAST(12.345 AS DECIMAL(5,3)) AS DECIMAL(4,2)) AS rounded;

-- query 23
SELECT 1 AS branch,CAST(NULL AS DECIMAL(38,0)) AS v UNION ALL SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 2 AS branch,CAST(1 AS DECIMAL(38,0))+1 AS v ORDER BY branch;

-- query 24
-- @skip_result_check=true
-- @expect_error=The 'add' operation involving decimal values overflows
SELECT CAST(1 AS DECIMAL(38,0))+1 AS v UNION ALL SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ a+1 AS v FROM ${case_db}.decimal_error_policy WHERE k=2;

-- query 25
-- @skip_result_check=true
-- @expect_error=ERROR_IF_OVERFLOW is not captured
CREATE VIEW default_catalog.${case_db}.decimal_error_uncaptured_v AS SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ 1 AS v;

-- query 26
SET decimal_overflow_to_double=true;
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS promoted;

-- query 27
-- @skip_result_check=true
-- @expect_error=numeric type cast involving decimal overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS DECIMAL(9,0)) AS narrowed;

-- query 28
-- @skip_result_check=true
-- @expect_error=ERROR_IF_OVERFLOW is unsupported for nested Decimal numeric CAST
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(CAST(['1'] AS ARRAY<DECIMAL(4,0)>) AS ARRAY<DECIMAL(3,0)>) AS unsupported;

-- query 29
SELECT CAST(CAST(CAST(['1'] AS ARRAY<DECIMAL(4,0)>) AS ARRAY<DECIMAL(3,0)>)[1] AS VARCHAR) AS allowed_null_policy;

-- query 30
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(CAST(CAST(['1'] AS ARRAY<DECIMAL(4,0)>) AS ARRAY<DECIMAL(4,0)>)[1] AS VARCHAR) AS same_physical_type;

-- query 31
-- @skip_result_check=true
-- @expect_error=numeric type cast involving decimal overflows
SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ CAST(CAST(1000 AS DECIMAL(4,0)) AS DECIMAL(3,0)) AS legacy_cast;

-- query 32
-- @skip_result_check=true
-- @expect_error=The 'mul' operation involving decimal values overflows
SELECT /*+ SET_VAR(sql_mode='ALLOW_THROW_EXCEPTION') */ CAST(99999999999999999999999999999999999999 AS DECIMAL(38,0))*CAST(2 AS DECIMAL(38,0)) AS legacy_mul;

-- query 33
-- @skip_result_check=true
CREATE VIEW default_catalog.${case_db}.decimal_error_allowed_v AS SELECT 1 AS n;

-- query 34
-- @skip_result_check=true
-- @expect_error=ERROR_IF_OVERFLOW is not captured
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ n FROM default_catalog.${case_db}.decimal_error_allowed_v;

-- query 35
SELECT n FROM default_catalog.${case_db}.decimal_error_allowed_v;

-- query 36
-- @skip_result_check=true
SET sql_mode='ERROR_IF_OVERFLOW';

-- query 37
-- @skip_result_check=true
-- @expect_error=ERROR_IF_OVERFLOW is not captured
CREATE VIEW default_catalog.${case_db}.decimal_error_inherited_v AS SELECT 1 AS n;

-- query 38
-- @skip_result_check=true
SET sql_mode=32;

-- query 39
-- @skip_result_check=true
SET sql_mode=32;
SET decimal_overflow_to_double=false;
DROP VIEW default_catalog.${case_db}.decimal_error_allowed_v;
DROP TABLE ${case_db}.decimal_error_policy;

-- query 40
-- @skip_result_check=true
SET sql_mode=32;
SET decimal_overflow_to_double=false;
CREATE TABLE ${case_db}.decimal_implicit_left (id INT,k DECIMAL(3,0),c BIGINT) TBLPROPERTIES ("format-version"="3");
CREATE TABLE ${case_db}.decimal_implicit_right (id INT,k BIGINT) TBLPROPERTIES ("format-version"="3");
INSERT INTO ${case_db}.decimal_implicit_left VALUES (1,1,1),(2,NULL,2);
INSERT INTO ${case_db}.decimal_implicit_right VALUES (10,1),(20,1000),(30,NULL);

-- query 41
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ id,FIELD(k,c) AS f FROM ${case_db}.decimal_implicit_left ORDER BY id;

-- query 42
SELECT l.id AS left_id,r.id AS right_id,CAST(k AS VARCHAR) AS merged_key
FROM (SELECT id,k FROM ${case_db}.decimal_implicit_left WHERE id=1) l
FULL OUTER JOIN ${case_db}.decimal_implicit_right r USING(k)
ORDER BY r.id;

-- query 43
-- @skip_result_check=true
-- @expect_error=numeric type cast involving decimal overflows
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ CAST(k AS VARCHAR) AS merged_key
FROM (SELECT id,k FROM ${case_db}.decimal_implicit_left WHERE id=1) l
FULL OUTER JOIN ${case_db}.decimal_implicit_right r USING(k);

-- query 44
SELECT /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW') */ l.id AS left_id,r.id AS right_id,CAST(k AS VARCHAR) AS merged_key
FROM (SELECT id,k FROM ${case_db}.decimal_implicit_left WHERE id=2) l
FULL OUTER JOIN (SELECT id,k FROM ${case_db}.decimal_implicit_right WHERE id=30) r USING(k)
ORDER BY COALESCE(l.id,r.id);

-- query 45
-- @skip_result_check=true
SET sql_mode=32;
DROP TABLE ${case_db}.decimal_implicit_left;
DROP TABLE ${case_db}.decimal_implicit_right;
