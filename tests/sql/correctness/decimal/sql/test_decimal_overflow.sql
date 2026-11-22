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

-- Test Objective:
-- 1. Validate checked decimal arithmetic under both SQL modes.
-- 2. Preserve the default NULL and strict overflow behavior of explicit narrowing CASTs.
-- 3. Validate decimal subtraction and complex aggregate expressions.
-- Migrated from dev/test/sql/test_decimal/T/test_decimal_overflow

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}."t_decimal_overflow" (
  "c_id" INTEGER NOT NULL,
  "c_d32" DECIMAL(9,3) NOT NULL,
  "c_d64" DECIMAL(18,5) NOT NULL,
  "c_d128" DECIMAL(38,7) NOT NULL
) WITH (format_version = 3);
CREATE TABLE ${case_db}."avg_test" (
  "c0" bigint,
  "c1" array<int>,
  "c2" bigint,
  "c3" INTEGER
) WITH (format_version = 3);
INSERT INTO ${case_db}."t_decimal_overflow" (c_id, c_d32, c_d64, c_d128) VALUES ROW(1, DECIMAL '999999.99', DECIMAL '9999999999999.99999', DECIMAL '9999999999999999999999999999999.9999999'), ROW(2, (- (DECIMAL '999999.99')), (- (DECIMAL '9999999999999.99999')), (- (DECIMAL '9999999999999999999999999999999.9999999')));
insert into ${case_db}.avg_test values ROW(1, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 123456789, 1);
insert into ${case_db}.avg_test values ROW(1, [11, 12, 13, 14, 15, 16, 17, 18, 19, 20], 123456789, 1);

-- query 2
-- ROUND retains NovaRocks DECIMAL(38,16) results. The standard product is DECIMAL(38,6).
select 274.97790000000000000000 * (round(1103.00000000000000000000 * 1.0000,16) /round(1103.00000000000000000000,16));

-- query 3
-- The representable product also succeeds with strict CAST overflow enabled.
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ 274.97790000000000000000 * (round(1103.00000000000000000000 * 1.0000,16) /round(1103.00000000000000000000,16));

-- query 4
select cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 5
select cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 6
select cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 7
select cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 8
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 9
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 10
select cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 11
select cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 12
select cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 13
select cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 14
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 15
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 16
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 17
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * c_d32 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 18
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 19
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * c_d64 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 20
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 21
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * c_d128 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 22
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 23
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d32 * 1.000 as decimal32) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 24
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 25
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d64 * 1.000000 as decimal64) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 26
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 27
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ cast(c_d128 * 1.000000000 as decimal128) from ${case_db}.t_decimal_overflow where c_id = 2;

-- query 28
select c_id - 1.12345678901234567890 from ${case_db}.t_decimal_overflow where c_id = 1;

-- query 29
select max(c0- 2.8665963056616452*(lt - 3.062472673706541)) as adjust_lt from (select c0, c2, c3, sum(n) lt from ${case_db}.avg_test CROSS JOIN UNNEST(c1) AS item(n) GROUP BY c0,c2,c3,c1) t group by c2;

-- query 30
select avg(c0- 2.8665963056616452*(lt - 3.062472673706541)) as adjust_lt from (select c0, c2, c3, sum(n) lt from ${case_db}.avg_test CROSS JOIN UNNEST(c1) AS item(n) GROUP BY c0,c2,c3,c1) t group by c2;

-- query 31
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ max(lt- 2.8665963056616452*(c2 - 3.062472673706541)) as adjust_lt from (select c0, c2, c3, sum(n) lt from ${case_db}.avg_test CROSS JOIN UNNEST(c1) AS item(n) GROUP BY c0,c2,c3,c1) t group by c0;

-- query 32
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
WITH SESSION sql_mode = 'ERROR_IF_OVERFLOW' select /*+ SET_VAR(sql_mode='ERROR_IF_OVERFLOW')*/ avg(lt- 2.8665963056616452*(c2 - 3.062472673706541)) as adjust_lt from (select c0, c2, c3, sum(n) lt from ${case_db}.avg_test CROSS JOIN UNNEST(c1) AS item(n) GROUP BY c0,c2,c3,c1) t group by c0;
