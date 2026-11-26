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
-- IV3-8: $manifests lists manifest files with file counts.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.t_${uuid0} (id INT, v INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.t_${uuid0} VALUES ROW(1,10);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.t_${uuid0} VALUES ROW(2,20);

-- query 5
-- $manifests is non-empty and reports added data files.
SELECT count(*) > 0 AS has_manifests, sum(added_data_files_count) > 0 AS has_added
  FROM iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}."t_${uuid0}$manifests";

-- query 6
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.v1_${uuid0} (id INT) WITH (format_version = 1);

-- query 7
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}.v1_${uuid0} VALUES ROW(1);

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT count(*) FROM iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}."v1_${uuid0}$manifests";

-- query 9
SELECT count(*) > 0 AS has_snapshots
  FROM iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}."v1_${uuid0}$snapshots";

-- query 10
SELECT count(*) > 0 AS has_files
  FROM iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0}."v1_${uuid0}$files";

-- query 11
-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.iv38m_db_${uuid0};
