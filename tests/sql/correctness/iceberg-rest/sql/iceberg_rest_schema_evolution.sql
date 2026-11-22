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
-- Validate REST updateSchema commit: ADD COLUMN, RENAME COLUMN, type widen
-- INT→BIGINT, DROP COLUMN. After each ALTER, SELECT verifies the new schema
-- and existing data are read back correctly.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} (id INT, v INT);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} VALUES ROW(1, 10), ROW(2, 20);

-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} ADD COLUMN c VARCHAR;

-- query 5
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} VALUES ROW(3, 30, 'three');

-- query 6
-- Old rows must read c as NULL; new row must read c='three'.
SELECT id, v, c
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 7
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} RENAME COLUMN v TO val;

-- query 8
-- After rename, val must read the original v values.
SELECT id, val, c
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 9
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} ALTER COLUMN id SET DATA TYPE BIGINT;

-- query 10
-- After widen INT→BIGINT, existing values must be readable.
SELECT id, val, c
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 11
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0} DROP COLUMN c;

-- query 12
-- After DROP COLUMN c, only id and val remain.
SELECT id, val
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0}
  ORDER BY id;

-- query 13
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0}.t_se_${uuid0};

-- query 14
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_se_db_${uuid0};
