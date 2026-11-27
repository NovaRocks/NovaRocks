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
-- Validate SELECT * FROM <tbl>$snapshots routes through parser/analyzer/lowering
-- chain and surfaces the snapshot summary metadata.
-- Tests Phase A (metadata table SQL routing) of the Iceberg V3 row-lineage
-- completion plan.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metasnap_${uuid0} (id INT, v INT) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metasnap_${uuid0} VALUES ROW(1, 10);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metasnap_${uuid0} VALUES ROW(2, 20);

-- query 5
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metasnap_${uuid0} VALUES ROW(3, 30);

-- query 6
-- 3 snapshots committed.
SELECT count(*) AS n_snapshots
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metasnap_${uuid0}$snapshots";

-- query 7
-- All 3 snapshot operations are 'append'.
SELECT operation, count(*) AS n
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metasnap_${uuid0}$snapshots"
  GROUP BY operation
  ORDER BY operation;

-- query 8
-- Exactly one snapshot has parent_id NULL (the first one).
SELECT count(*) AS n_root
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metasnap_${uuid0}$snapshots"
  WHERE parent_id IS NULL;

-- query 9
-- All 3 snapshots have non-null snapshot_id.
SELECT count(*) AS n_with_id
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metasnap_${uuid0}$snapshots"
  WHERE snapshot_id IS NOT NULL;

-- query 10
-- Alias works.
SELECT count(*) AS n_root_aliased
  FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metasnap_${uuid0}$snapshots" AS s
  WHERE s.parent_id IS NULL;

-- query 11
SELECT * FROM iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}."metasnap_${uuid0}$snapshots" LIMIT 0;

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0}.metasnap_${uuid0};

-- query 13
-- @skip_result_check=true
DROP SCHEMA iceberg_cat_${suite_uuid0}.iceberg_meta_db_${uuid0};
