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
-- @tags=write_path,iceberg,row_lineage,overwrite_partitions
-- Test Point:
--   Dynamic partition overwrite is delete+insert inside touched
--   partitions and carry-forward only outside touched partitions.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_partition_overwrite_lineage (
  id INT,
  region VARCHAR(8)
) WITH (partitioning = ARRAY['region'], format_version = 3);
INSERT INTO ${case_db}.t_partition_overwrite_lineage VALUES
  ROW(1, 'us'),
  ROW(2, 'us'),
  ROW(3, 'eu'),
  ROW(4, 'eu');
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_partition_overwrite_lineage EXECUTE create_tag(name => 'before_overwrite', replace => false, if_not_exists => false);

-- query 2
SELECT region, COUNT(*) AS n
FROM ${case_db}.t_partition_overwrite_lineage
GROUP BY region
ORDER BY region;

-- query 3
-- @skip_result_check=true
INSERT OVERWRITE PARTITIONS ${case_db}.t_partition_overwrite_lineage VALUES
  ROW(1, 'us'),
  ROW(99, 'us');

-- query 4
SELECT id, region
FROM ${case_db}.t_partition_overwrite_lineage
ORDER BY id;

-- query 5
SELECT
  SUM(CASE WHEN cur.region = 'us' AND cur.cur_row_id = old.old_row_id THEN 1 ELSE 0 END) AS same_touched_rows,
  SUM(CASE WHEN cur.region = 'us' AND cur.cur_row_id <> old.old_row_id THEN 1 ELSE 0 END) AS changed_touched_rows,
  SUM(CASE WHEN cur.region = 'eu' AND cur.cur_row_id = old.old_row_id THEN 1 ELSE 0 END) AS same_untouched_rows,
  SUM(CASE WHEN cur.region = 'us' THEN 1 ELSE 0 END) AS current_us_rows,
  SUM(CASE WHEN cur.region = 'eu' THEN 1 ELSE 0 END) AS current_eu_rows
FROM (
  SELECT id, region, _row_id AS cur_row_id
  FROM ${case_db}.t_partition_overwrite_lineage
) cur
LEFT JOIN (
  SELECT id, region, _row_id AS old_row_id
  FROM ${case_db}.t_partition_overwrite_lineage FOR VERSION AS OF 'before_overwrite'
) old
ON cur.id = old.id AND cur.region = old.region;

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.t_partition_overwrite_lineage VALUES
  ROW(100, 'ap');

-- query 7
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT _row_id) AS distinct_row_ids
FROM ${case_db}.t_partition_overwrite_lineage;

-- query 8
-- @skip_result_check=true
ALTER TABLE iceberg_dml_cat_${suite_uuid0}.${case_db}.t_partition_overwrite_lineage EXECUTE drop_tag(name => 'before_overwrite', if_exists => false);
DROP TABLE ${case_db}.t_partition_overwrite_lineage ;
