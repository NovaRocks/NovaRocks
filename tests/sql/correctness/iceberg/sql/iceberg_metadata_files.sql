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
-- IV3-8: $files lists data files with stats, including typed partition structs.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} (id INT, region VARCHAR, v INT) WITH (partitioning = ARRAY['region'], format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} VALUES ROW(1, 'us', 10);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}.t_${uuid0} VALUES ROW(2, 'eu', 20);

-- query 5
-- two appends -> two data files, all content=0
SELECT count(*) AS n_data_files
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}."t_${uuid0}$files"
  WHERE content = 0;

-- query 6
-- total record_count across data files == 2 rows inserted
SELECT sum(record_count) AS total_records
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}."t_${uuid0}$files"
  WHERE content = 0;

-- query 7
-- partition is a typed struct derived from the table partition spec.
SELECT f."partition".region AS part_region, sum(record_count) AS records
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}."t_${uuid0}$files" AS f
  WHERE content = 0
  GROUP BY f."partition".region
  ORDER BY part_region;

-- query 8
-- @be_log_be_count_at_least=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN,2
SELECT f.lower_bounds."1" AS lower_id, f.upper_bounds."1" AS upper_id,
       typeof(f.readable_metrics) AS metrics_type
  FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}."t_${uuid0}$files" AS f
 WHERE f.content = 0
 ORDER BY lower_id;

-- query 9
SELECT * FROM iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0}."t_${uuid0}$files" LIMIT 0;

-- query 10
-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.iv38f_db_${uuid0};
