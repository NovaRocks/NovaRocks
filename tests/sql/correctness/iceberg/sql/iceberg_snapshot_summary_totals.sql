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
-- IV3-2: validate that snapshot summaries carry total-* and engine identity.
-- Numeric carry-forward correctness is unit-tested in commit/*.rs; here we
-- assert the surfaced summary carries exact totals across operations.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.iv32_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} (id INT, v INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} VALUES ROW(1, 10), ROW(2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} VALUES ROW(3, 30);

-- query 5
-- Latest append summary carries exact total records/data-files + engine-name.
-- `summary` is a MAP, so a key is read by subscript. It used to be a rendered
-- string and these were LIKE patterns; asserting on a rendering asserted the
-- shape of the rendering rather than the value.
-- Use a subquery so ORDER BY can reference committed_at without it appearing
-- in the golden output.
SELECT total_data_files_is_2, total_records_is_3, has_total_files_size, has_engine_name
FROM (
  SELECT
    committed_at,
    summary[CAST('total-data-files' AS VARCHAR)] = '2'        AS total_data_files_is_2,
    summary[CAST('total-records' AS VARCHAR)] = '3'           AS total_records_is_3,
    summary[CAST('total-files-size' AS VARCHAR)] IS NOT NULL  AS has_total_files_size,
    summary[CAST('engine-name' AS VARCHAR)] IS NOT NULL       AS has_engine_name
  FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}."t_${uuid0}$snapshots"
  ORDER BY committed_at DESC
  LIMIT 1
) t;

-- query 6
-- @skip_result_check=true
DELETE FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}.t_${uuid0} WHERE id = 1;

-- query 7
-- A v3 DELETE adds a DV and preserves the three physical data-file records.
-- The two visible rows are distinct from the snapshot summary total-records.
SELECT total_records_is_3, has_engine_name
FROM (
  SELECT
    committed_at,
    summary[CAST('total-records' AS VARCHAR)] = '3'          AS total_records_is_3,
    summary[CAST('engine-name' AS VARCHAR)] IS NOT NULL      AS has_engine_name
  FROM iceberg_cat_${suite_uuid0}.iv32_db_${uuid0}."t_${uuid0}$snapshots"
  ORDER BY committed_at DESC
  LIMIT 1
) t;

-- query 8
-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.iv32_db_${uuid0};
