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
-- The original SELECT negotiates the aligned time predicate and keeps an unaligned residual.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm18_read_time (id BIGINT, ts TIMESTAMP(6))
WITH (partitioning = ARRAY['day(ts)'], format_version = 3);
INSERT INTO ${case_db}.rwm18_read_time VALUES
  ROW(1, TIMESTAMP '1969-12-30 23:59:59.999999'),
  ROW(2, TIMESTAMP '1969-12-31 00:00:00.000000'),
  ROW(3, TIMESTAMP '1969-12-31 12:00:00.000001'),
  ROW(4, TIMESTAMP '1970-01-01 00:00:00.000000'),
  ROW(5, NULL);

-- query 2
-- @skip_result_check=true
-- @result_contains=provider-read={provider=iceberg
-- @result_contains=provider-guarantee root=
-- @result_contains=kind=exact
-- @result_not_contains=residual root=
EXPLAIN WITH SESSION explain_detail = 'CONTRACT'
SELECT id FROM ${case_db}.rwm18_read_time WHERE ts >= TIMESTAMP '1969-12-31 00:00:00.000000' AND ts < TIMESTAMP '1970-01-01 00:00:00.000000' ORDER BY id;

-- query 3
SELECT id FROM ${case_db}.rwm18_read_time WHERE ts >= TIMESTAMP '1969-12-31 00:00:00.000000' AND ts < TIMESTAMP '1970-01-01 00:00:00.000000' ORDER BY id;

-- query 4
-- @skip_result_check=true
-- @result_contains=provider-read={provider=iceberg
-- @result_contains=residual root=
-- @result_not_contains=kind=exact
EXPLAIN WITH SESSION explain_detail = 'CONTRACT'
SELECT id FROM ${case_db}.rwm18_read_time WHERE ts >= TIMESTAMP '1969-12-31 12:00:00.000000' AND ts < TIMESTAMP '1970-01-01 00:00:00.000000' ORDER BY id;

-- query 5
SELECT id FROM ${case_db}.rwm18_read_time WHERE ts >= TIMESTAMP '1969-12-31 12:00:00.000000' AND ts < TIMESTAMP '1970-01-01 00:00:00.000000' ORDER BY id;

-- query 6
SELECT id FROM ${case_db}.rwm18_read_time ORDER BY id;

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm18_read_time;
