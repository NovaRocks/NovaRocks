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
-- Verify per-partition counts and typed data statistics.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0} (id INT, region VARCHAR) WITH (partitioning = ARRAY['region'], format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0} VALUES
  ROW(1, 'us'), ROW(2, 'us'), ROW(3, 'eu');

-- query 4
-- 2 partitions: 'us' and 'eu'.
SELECT count(*) AS n_partitions
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metapart_${uuid0}$partitions";

-- query 5
-- Total rows across all partitions = 3.
SELECT sum(record_count) AS total_records
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metapart_${uuid0}$partitions";

-- query 6
SELECT p."partition".region AS region, p.data.id.min AS min_id, p.data.id.max AS max_id
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metapart_${uuid0}$partitions" p
 ORDER BY region;

-- query 7
SELECT * FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metapart_${uuid0}$partitions" LIMIT 0;

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metapart_${uuid0};

-- query 9
-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};
