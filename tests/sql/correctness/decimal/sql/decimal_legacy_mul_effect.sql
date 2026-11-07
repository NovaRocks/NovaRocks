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
-- Test Objective: Preserve legacy multiplication errors through unused projection pruning.

-- query 1
-- @skip_result_check=true
SET sql_mode='32';
SET decimal_overflow_to_double=false;
CREATE TABLE ${case_db}.legacy_mul_effect (k INT,a DECIMAL(38,0)) TBLPROPERTIES ("format-version"="3");
INSERT INTO ${case_db}.legacy_mul_effect VALUES (1,1),(2,CAST('99999999999999999999999999999999999999' AS DECIMAL(38,0))),(3,NULL);

-- query 2
SELECT k,CAST(a*2 AS VARCHAR) AS v FROM ${case_db}.legacy_mul_effect ORDER BY k;

-- query 3
-- @skip_result_check=true
SET sql_mode='ALLOW_THROW_EXCEPTION';

-- query 4
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
SELECT CAST(a*2 AS VARCHAR) AS v FROM ${case_db}.legacy_mul_effect;

-- query 5
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
SELECT k FROM (SELECT k,a*2 AS unreferenced FROM ${case_db}.legacy_mul_effect) s;

-- query 6
SELECT k FROM (SELECT k,a+2 AS unreferenced FROM ${case_db}.legacy_mul_effect) s ORDER BY k;

-- query 7
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
SELECT CAST(a AS DECIMAL(3,0)) AS narrowed FROM ${case_db}.legacy_mul_effect;

-- query 8
-- @skip_result_check=true
-- @expect_error_tier=target
-- @expect_sql_code=query.execution.numeric_out_of_range
-- @expect_sql_phase=Execution
-- @expect_error=[query.execution.numeric_out_of_range]
SELECT k FROM (SELECT k,CAST(a AS DECIMAL(3,0)) AS unreferenced FROM ${case_db}.legacy_mul_effect) s;

-- query 9
SELECT k FROM (SELECT k,CAST(a AS DECIMAL(76,0)) AS safe_widening FROM ${case_db}.legacy_mul_effect) s ORDER BY k;

-- query 10
-- @skip_result_check=true
SET sql_mode='32';
DROP TABLE ${case_db}.legacy_mul_effect;
