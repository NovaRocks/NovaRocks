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

-- @tags=iceberg_dml,datetime,delete
-- Test Objective:
-- 1. Validate DATETIME stores sub-second fractional precision up to microseconds (6 digits).
-- 2. Validate DELETE by exact DATETIME value including sub-second comparisons on an Iceberg v3 table.
-- 3. Cover: truncation of excess digits, zero-padding on display, same value via different literal forms.
-- Key semantics:
--   '2020-01-01 00:00:00'    == '2020-01-01 00:00:00.0' (both stored as 00:00:00)
--   '2020-01-01 00:00:00.1'  == '2020-01-01 00:00:00.100000'
--   '2020-01-01 00:00:00.123450' == '2020-01-01 00:00:00.12345' (trailing zero trimmed)

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_datetime;
CREATE TABLE ${case_db}.t_datetime (
    c1 int,
    c2 TIMESTAMP(6)
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_datetime VALUES
ROW(1, TIMESTAMP '2020-01-01 00:00:00'),
ROW(2, TIMESTAMP '2020-01-01 00:00:00.0'),
ROW(3, TIMESTAMP '2020-01-01 00:00:00.01'),
ROW(4, TIMESTAMP '2020-01-01 00:00:00.012'),
ROW(5, TIMESTAMP '2020-01-01 00:00:00.0123'),
ROW(6, TIMESTAMP '2020-01-01 00:00:00.01234'),
ROW(7, TIMESTAMP '2020-01-01 00:00:00.012345'),
ROW(8, TIMESTAMP '2020-01-01 00:00:00.1'),
ROW(9, TIMESTAMP '2020-01-01 00:00:00.12'),
ROW(10, TIMESTAMP '2020-01-01 00:00:00.123'),
ROW(11, TIMESTAMP '2020-01-01 00:00:00.1234'),
ROW(12, TIMESTAMP '2020-01-01 00:00:00.12345'),
ROW(13, TIMESTAMP '2020-01-01 00:00:00.123450');

-- query 2
-- @order_sensitive=true
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 3
-- @skip_result_check=true
DELETE FROM ${case_db}.t_datetime WHERE c2 = TIMESTAMP '2020-01-01 00:00:00';
DELETE FROM ${case_db}.t_datetime WHERE c2 = TIMESTAMP '2020-01-01 00:00:00.0';
DELETE FROM ${case_db}.t_datetime WHERE c2 = TIMESTAMP '2020-01-01 00:00:00.012';

-- query 4
-- @order_sensitive=true
-- Remaining: c1=3,5,6,7,8,9,10,11,12,13 (c1=1,2,4 deleted)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 5
-- @skip_result_check=true
DELETE FROM ${case_db}.t_datetime WHERE c2 = TIMESTAMP '2020-01-01 00:00:00.1';
DELETE FROM ${case_db}.t_datetime WHERE c2 = TIMESTAMP '2020-01-01 00:00:00.123';
DELETE FROM ${case_db}.t_datetime WHERE c2 = TIMESTAMP '2020-01-01 00:00:00.123450';

-- query 6
-- @order_sensitive=true
-- Final remaining: c1=3,5,6,7,9,11 (6 rows)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;
