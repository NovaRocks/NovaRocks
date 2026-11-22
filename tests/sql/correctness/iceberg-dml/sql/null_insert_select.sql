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
-- @tags=iceberg_dml,null
-- Test Objective:
-- 1. Validate NULL values are preserved across INSERT-SELECT into typed sink columns.
-- 2. Validate mixed NULL/non-NULL rows across INT/STRING/DECIMAL/DATETIME columns.
DROP TABLE IF EXISTS ${case_db}.t_null_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_null_insert_sink;
CREATE TABLE ${case_db}.t_null_insert_src (
  id BIGINT,
  c_int BIGINT,
  c_str VARCHAR,
  c_dec DECIMAL(9, 2),
  c_dt TIMESTAMP(6)
);
CREATE TABLE ${case_db}.t_null_insert_sink (
  id INT,
  c_int INT,
  c_str VARCHAR,
  c_dec DECIMAL(9, 2),
  c_dt TIMESTAMP(6)
);
INSERT INTO ${case_db}.t_null_insert_src VALUES
  ROW(1, NULL, NULL, NULL, NULL),
  ROW(2, 20, 'ok', 12.30, TIMESTAMP '2024-01-02 03:04:05'),
  ROW(3, NULL, 'tail', NULL, TIMESTAMP '2024-06-01 00:00:00');
INSERT INTO ${case_db}.t_null_insert_sink
SELECT id, c_int, c_str, c_dec, c_dt
FROM ${case_db}.t_null_insert_src;
SELECT id, c_int, c_str, c_dec, c_dt
FROM ${case_db}.t_null_insert_sink
ORDER BY id;
