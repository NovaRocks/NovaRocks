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
-- Validate REST createTable / dropTable commit:
-- - basic primitive columns
-- - PARTITION BY one column
-- - CREATE TABLE IF NOT EXISTS idempotency
-- - DROP TABLE IF EXISTS idempotency
-- DESCRIBE result is the primary positive assertion.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_basic_${uuid0} (
  id BIGINT,
  name STRING,
  amount DOUBLE,
  d DATE,
  ts TIMESTAMP
);

-- query 3
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_part_${uuid0} (
  id BIGINT,
  region STRING,
  amount DOUBLE
) WITH (partitioning = ARRAY['region']);

-- query 4
-- @skip_result_check=true
-- IF NOT EXISTS on an existing table must be a no-op.
CREATE TABLE IF NOT EXISTS iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_basic_${uuid0} (
  id BIGINT,
  name STRING,
  amount DOUBLE,
  d DATE,
  ts TIMESTAMP
);

-- query 5
-- Schema assertion for the basic table. standalone-server v1 does not
-- support DESCRIBE; LIMIT 0 produces the bound column headers without rows
-- and errors if any of the listed columns is missing.
SELECT id, name, amount, d, ts
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_basic_${uuid0}
  LIMIT 0;

-- query 6
-- Schema assertion for the partitioned table.
SELECT id, region, amount
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_part_${uuid0}
  LIMIT 0;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_basic_${uuid0};

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_part_${uuid0};

-- query 9
-- @skip_result_check=true
-- IF EXISTS on an already-dropped table must be a no-op.
DROP TABLE IF EXISTS iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0}.t_basic_${uuid0};

-- query 10
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_tbl_db_${uuid0};
