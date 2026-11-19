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
-- Validate REST updateRefs commit: CREATE BRANCH/TAG, branch-qualified write,
-- DROP BRANCH/TAG. v3 (row-lineage) is required for branch writes.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} (id INT, v INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} VALUES ROW(1, 10), ROW(2, 20);

-- query 4
-- @skip_result_check=true
CREATE BRANCH dev IN TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0};

-- query 5
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} EXECUTE create_tag(name => 'release_v1', replace => false, if_not_exists => false);

-- query 6
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} @dev VALUES (3, 30);

-- query 7
-- main is unchanged: 2 rows.
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 8
-- dev now has 3 rows.
SELECT id, v
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 9
-- @skip_result_check=true
DROP BRANCH dev IN TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0};

-- query 10
-- @skip_result_check=true
ALTER TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0} EXECUTE drop_tag(name => 'release_v1', if_exists => false);

-- query 11
-- @skip_result_check=true
-- Idempotent IF EXISTS after DROP.
DROP BRANCH IF EXISTS dev IN TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0};

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0}.t_ref_${uuid0};

-- query 13
-- @skip_result_check=true
DROP SCHEMA iceberg_rest_${suite_uuid0}.iceberg_rest_ref_db_${uuid0};
