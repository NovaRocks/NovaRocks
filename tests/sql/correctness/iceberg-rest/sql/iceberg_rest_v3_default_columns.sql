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
-- Validate format-v3 default-value commit through REST: ADD COLUMN with DEFAULT
-- on a v3 table backfills initial-default for existing rows and materializes
-- write-default for new INSERTs that omit the column.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_${uuid0} (a INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_${uuid0} VALUES ROW(1), ROW(2);

-- query 4
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_${uuid0}
  ADD COLUMN b INT DEFAULT 9;

-- query 5
-- Pre-existing rows must read b=9 via initial-default backfill.
SELECT a, b
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_${uuid0}
  ORDER BY a;

-- query 6
-- @skip_result_check=true
-- INSERT omitting b must materialize write-default 9.
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_${uuid0} (a) VALUES ROW(3);

-- query 7
-- All 3 rows must read b=9.
SELECT a, b
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_${uuid0}
  ORDER BY a;

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_${uuid0};

-- query 9
-- @skip_result_check=true
-- Negative path: ADD COLUMN with DEFAULT on v2 table must fail with format-version error.
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_v2_${uuid0} (a INT);

-- query 10
-- @expect_error=format-version 3
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_v2_${uuid0}
  ADD COLUMN b INT DEFAULT 5;

-- query 11
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0}.t_def_v2_${uuid0};

-- query 12
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_def_db_${uuid0};
