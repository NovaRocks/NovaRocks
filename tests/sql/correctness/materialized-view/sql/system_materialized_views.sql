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

-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,system_catalog
-- Correctness owner: materialized-view. Trino 484 A06/D05/D07/D09/D11/D18
-- and E08/E12/E14 provide independent schemas, states and lookup rules.
-- Accepted differences: storage is the MV's own table, last_fresh_time is
-- always NULL, and definition preserves the exact user SQL after AS.
-- The suite owns a private REST fixture; this case owns and drops its MV
-- before its base table. No candidate output is used to construct the golden.

-- query 1
SELECT * FROM system.metadata.materialized_views WHERE false;

-- query 2
-- @skip_result_check=true
CREATE TABLE system_base (k1 INT NOT NULL, v2 BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO system_base VALUES (1, 10);
CREATE MATERIALIZED VIEW system_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM system_base;

-- query 3
SELECT name, freshness, last_fresh_time, comment, storage_table
FROM system.metadata.materialized_views
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}'
  AND schema_name = '${case_db}' AND name = 'system_mv';

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW system_mv;

-- query 5
SELECT name, freshness, last_fresh_time
FROM system.metadata.materialized_views
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}'
  AND schema_name = '${case_db}' AND name = 'system_mv';

-- query 6
-- @skip_result_check=true
INSERT INTO system_base VALUES (2, 20);

-- query 7
SELECT name, freshness, last_fresh_time
FROM system.metadata.materialized_views
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}'
  AND schema_name = '${case_db}' AND name = 'system_mv';

-- query 8
SELECT definition = 'SELECT k1, v2 FROM system_base' AS saved_definition
FROM system.metadata.materialized_views
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}'
  AND schema_name = '${case_db}' AND name = 'system_mv';

-- query 9
SELECT table_name, table_type
FROM materialized_view_cat_${suite_uuid0}.information_schema.tables
WHERE table_schema = '${case_db}'
ORDER BY table_name;

-- query 10
SELECT table_name, table_type
FROM system.information_schema.tables
WHERE table_schema = 'metadata' AND table_name = 'materialized_views';

-- query 11
-- @skip_result_check=true
USE system.metadata;

-- query 12
SELECT count(*) AS mv_count FROM materialized_views
WHERE schema_name = '${case_db}' AND name = 'system_mv';

-- query 13
SELECT name, freshness FROM system.metadata.materialized_views
WHERE catalog_name = 'trc_no_such_catalog';

-- query 14
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
DELETE FROM materialized_view_cat_${suite_uuid0}.${case_db}.system_base
WHERE k1 IN (
  SELECT CAST(1 AS integer) FROM system.information_schema.tables
  WHERE table_schema = 'metadata' AND table_name = 'materialized_views'
);

-- query 15
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
UPDATE materialized_view_cat_${suite_uuid0}.${case_db}.system_base SET v2 = 999
WHERE k1 IN (
  SELECT CAST(1 AS integer) FROM system.information_schema.tables
  WHERE table_schema = 'metadata' AND table_name = 'materialized_views'
);

-- query 16
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
MERGE INTO materialized_view_cat_${suite_uuid0}.${case_db}.system_base AS t
USING (
  SELECT CAST(1 AS integer) AS k1, CAST(999 AS bigint) AS v2
  FROM system.information_schema.tables
  WHERE table_schema = 'metadata' AND table_name = 'materialized_views'
) AS s ON t.k1 = s.k1
WHEN MATCHED THEN UPDATE SET v2 = s.v2;

-- query 17
SELECT k1, v2 FROM materialized_view_cat_${suite_uuid0}.${case_db}.system_base ORDER BY k1;

-- query 18
-- @cleanup=true
-- @skip_result_check=true
USE materialized_view_cat_${suite_uuid0}.${case_db};
DROP MATERIALIZED VIEW IF EXISTS system_mv;
DROP TABLE IF EXISTS system_base FORCE;
