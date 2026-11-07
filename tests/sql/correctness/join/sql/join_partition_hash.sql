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

-- @tags=join,partition_hash,broadcast
-- Test Objective:
-- Validate broadcast self-join correctness for partition-hash-style coverage at
-- stable correctness scale. Physical partition-hash optimization coverage
-- belongs in dedicated optimizer/perf tests.
-- Covers: count aggregation through broadcast join with USING clause, modulus
-- filters on probe side, and both probe-side and build-side column references.
-- Test Flow:
-- 1. Create a row-generator utility table to produce 40,000 unique rows.
-- 2. Create a main table with bigint key and string column, self-join via broadcast.
-- 3. Assert count correctness with various modulus filters on k1.
-- 4. Assert count correctness referencing c_string from both probe and build sides.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.phj_row_util_base;

-- query 2
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.phj_row_util;

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.phj_t1;

-- query 4
-- @skip_result_check=true
CREATE TABLE ${case_db}.phj_row_util_base (
  k1 BIGINT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT generate_series FROM TABLE(generate_series(0, 10000 - 1));

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util_base SELECT * FROM ${case_db}.phj_row_util_base;

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.phj_row_util (
  idx BIGINT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_row_util SELECT row_number() OVER() AS idx FROM ${case_db}.phj_row_util_base;

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.phj_t1 (
    k1 BIGINT NULL,
    c_bigint_null BIGINT NULL,
    c_string STRING
)
TBLPROPERTIES ("format-version" = "3");

-- query 11
-- @skip_result_check=true
INSERT INTO ${case_db}.phj_t1
SELECT idx, idx, substr(uuid(), 1, 6)
FROM ${case_db}.phj_row_util;

-- query 12
-- count(k1) from probe side, no filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null);

-- query 13
-- count(k1) with mod 2 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 2 = 0;

-- query 14
-- count(k1) with mod 3 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 3 = 0;

-- query 15
-- count(k1) with mod 5 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 5 = 0;

-- query 16
-- count(k1) with mod 10 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 10 = 0;

-- query 17
-- count(k1) with mod 100 filter
SELECT count(tt1.k1) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 100 = 0;

-- query 18
-- count(c_string) from probe side, no filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null);

-- query 19
-- count(c_string) from probe side with mod 2 filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 2 = 0;

-- query 20
-- count(c_string) from probe side with mod 10 filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 10 = 0;

-- query 21
-- count(c_string) from probe side with mod 100 filter
SELECT count(tt1.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 100 = 0;

-- query 22
-- count(c_string) from build side, no filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null);

-- query 23
-- count(c_string) from build side with mod 2 filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 2 = 0;

-- query 24
-- count(c_string) from build side with mod 10 filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 10 = 0;

-- query 25
-- count(c_string) from build side with mod 100 filter
SELECT count(tt2.c_string) FROM ${case_db}.phj_t1 tt1 JOIN  ${case_db}.phj_t1 tt2 USING(c_bigint_null) WHERE tt1.k1 % 100 = 0;
