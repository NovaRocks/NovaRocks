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
-- @tags=decimal,arithmetic,overflow
-- Test Objective: Retain exact products and reject signed coefficient or declared precision overflow.
-- Oracle: exact decimal coefficients and the Trino 484 multiplication overflow boundary.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}."t_decimal_precision_overflow" (
  "c_id" int(11) NOT NULL,
  "c_d64_max" decimal64(18,9) NOT NULL,
  "c_d128_large" decimal128(30,10) NOT NULL,
  "c_d128_max" decimal128(38,15) NOT NULL
) WITH (format_version = 3);

INSERT INTO ${case_db}."t_decimal_precision_overflow" (c_id, c_d64_max, c_d128_large, c_d128_max) VALUES ROW(1, DECIMAL '123456789.123456789', DECIMAL '12345678901234567890.1234567890', DECIMAL '12345678901234567890123.123456789012345'), ROW(2, (- (DECIMAL '123456789.123456789')), (- (DECIMAL '12345678901234567890.1234567890')), (- (DECIMAL '12345678901234567890123.123456789012345'))), ROW(3, DECIMAL '987654321.987654321', DECIMAL '98765432109876543210.9876543210', DECIMAL '98765432109876543210987.987654321098765'), ROW(4, DECIMAL '111222333.444555666', DECIMAL '11122233344455566677.8899001122', DECIMAL '11122233344455566677889.900112233445566'), ROW(5, DECIMAL '555666777.888999111', DECIMAL '55566677788899911122.2333444555', DECIMAL '55566677788899911122233.344455566677888');

-- query 2
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 3
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 2;

-- query 4
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 3;

-- query 5
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 4;

-- query 6
select c_d64_max * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 5;

-- query 7
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select c_d128_large * c_d64_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 12
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select c_d128_max * c_d128_max from ${case_db}.t_decimal_precision_overflow where c_id = 1;

-- query 15
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select 12345678901234567890123456789.123456789 * 98765432109876543210987654321.987654321;

-- query 36
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select 1234567890123456789.1234567890123456789 * 1.0000000000000000000;

-- query 38
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select 1234567890123456789012345678901234567.0 * 1.0;

-- query 40
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select c_id, c_d128_large * c_d64_max + 1.0 from ${case_db}.t_decimal_precision_overflow where c_id <= 2 order by 1;

-- query 41
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
select c_id, (c_d128_max * c_d128_max) / 2.0 from ${case_db}.t_decimal_precision_overflow where c_id <= 2 order by 1;

-- query 51
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
SELECT 0.58000000 * 0.970825897017235893 * 3021621.785498;

-- query 52
-- @skip_result_check=true
DROP TABLE ${case_db}.t_decimal_precision_overflow;
