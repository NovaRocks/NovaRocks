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
-- Validate branch-qualified INSERT / UPDATE / DELETE on iceberg branches.
-- Verifies the target ref is advanced and main is untouched, then drops the branch.
-- Table must be Iceberg v3 (row-lineage) because branch writes require v3.

-- query 1
-- @skip_result_check=true
-- Create test database.
CREATE SCHEMA iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0};

-- query 2
-- @skip_result_check=true
-- Create v3 row-lineage table (branch writes require v3).
CREATE TABLE iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} (id INT, v INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
-- Initial INSERT to main (creates first snapshot).
INSERT INTO iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} VALUES ROW(1, 10), ROW(2, 20);

-- query 4
-- @skip_result_check=true
-- Branch dev off main.
CREATE BRANCH dev IN TABLE iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0};

-- query 5
-- @skip_result_check=true
-- INSERT to branch dev.
INSERT INTO iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} @dev VALUES (3, 30);

-- query 6
-- main should still have only 2 rows.
SELECT id, v FROM iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 7
-- dev should now have 3 rows.
SELECT id, v FROM iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 8
-- @skip_result_check=true
-- UPDATE on branch dev: set v=99 for row id=1.
UPDATE iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} @dev
  SET v = 99 WHERE id = 1;

-- query 9
-- main: row 1 v=10 unchanged.
SELECT id, v FROM iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 10
-- dev: row 1 v=99.
SELECT id, v FROM iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 11
-- @skip_result_check=true
-- DELETE on branch dev: remove row id=2.
DELETE FROM iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0} @dev
  WHERE id = 2;

-- query 12
-- main: row 2 still there.
SELECT id, v FROM iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'main' ORDER BY id;

-- query 13
-- dev: row 2 gone.
SELECT id, v FROM iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0}
  FOR VERSION AS OF 'dev' ORDER BY id;

-- query 14
-- @skip_result_check=true
-- Cleanup: drop branch, table, database.
DROP BRANCH dev IN TABLE iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0};

-- query 15
-- @skip_result_check=true
DROP TABLE iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0}.t_bw_${uuid0};

-- query 16
-- @skip_result_check=true
DROP SCHEMA iceberg_dml_cat_${suite_uuid0}.iceberg_bw_db_${uuid0};
