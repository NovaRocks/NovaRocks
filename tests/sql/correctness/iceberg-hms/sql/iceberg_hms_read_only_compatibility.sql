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
-- @tags=iceberg,hive,read_only,fail_closed
-- Spark owns all writes and cleanup. Refused SQL must leave the complete
-- namespace inventory and current HMS metadata pointers unchanged.

-- query 1
-- @result_contains=HMS_PREPARE_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-hms/fixture.py" prepare iru7_${uuid0}

-- query 2
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_hms_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hive",
  "iceberg.catalog.hive.metastore.uris" = "${iceberg_hms_uris}",
  "iceberg.catalog.warehouse" = "${iceberg_hms_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);

-- query 3
SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v1 ORDER BY id;

-- query 4
SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 ORDER BY id;

-- query 5
SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v3 ORDER BY id;

-- query 6
SELECT COUNT(*) AS snapshots FROM ice_hms_${uuid0}.iru7_${uuid0}.v2$snapshots;

-- query 7
SELECT content, COUNT(*) AS files FROM ice_hms_${uuid0}.iru7_${uuid0}.v2$files GROUP BY content ORDER BY content;

-- query 8
SELECT content, COUNT(*) AS files FROM ice_hms_${uuid0}.iru7_${uuid0}.v3$files GROUP BY content ORDER BY content;

-- query 9
SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 FOR VERSION AS OF 'kept_branch' ORDER BY id;

-- query 10
SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 FOR VERSION AS OF 'kept_tag' ORDER BY id;

-- query 11
-- @skip_result_check=true
SET @hms_first_snapshot = (SELECT snapshot_id FROM ice_hms_${uuid0}.iru7_${uuid0}.v2$snapshots ORDER BY committed_at LIMIT 1);

-- query 12
SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 FOR VERSION AS OF @hms_first_snapshot ORDER BY id;

-- query 13
-- @result_contains=INVENTORY_CAPTURED
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-hms/fixture.py" before iru7_${uuid0}

-- query 14
-- Create namespace
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
CREATE DATABASE ice_hms_${uuid0}.iru7_${uuid0}_new;

-- query 15
-- Drop namespace
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
DROP DATABASE ice_hms_${uuid0}.iru7_${uuid0} FORCE;

-- query 16
-- Create table
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
CREATE TABLE ice_hms_${uuid0}.iru7_${uuid0}.new_table (id BIGINT, p INT);

-- query 17
-- Create table like
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
CREATE TABLE ice_hms_${uuid0}.iru7_${uuid0}.new_like LIKE ice_hms_${uuid0}.iru7_${uuid0}.v3;

-- query 18
-- Create table as select
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
CREATE TABLE ice_hms_${uuid0}.iru7_${uuid0}.new_ctas AS SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v3;

-- query 19
-- Drop table
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
DROP TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 FORCE;

-- query 20
-- Add schema field
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 ADD COLUMN added STRING;

-- query 21
-- Rename schema field
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 RENAME COLUMN note TO renamed;

-- query 22
-- Modify schema field
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 MODIFY COLUMN note VARCHAR(128);

-- query 23
-- Drop schema field
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 DROP COLUMN note;

-- query 24
-- Set properties
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 SET TBLPROPERTIES ('comment'='refused');

-- query 25
-- Unset properties
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 UNSET TBLPROPERTIES ('write.delete.mode');

-- query 26
-- Add partition field
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 ADD PARTITION COLUMN bucket(id, 4);

-- query 27
-- Drop partition field
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 DROP PARTITION COLUMN p;

-- query 28
-- Create branch
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 CREATE BRANCH rejected_branch;

-- query 29
-- Drop branch
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v2 DROP BRANCH kept_branch;

-- query 30
-- Create tag
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 CREATE TAG rejected_tag;

-- query 31
-- Drop tag
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v2 DROP TAG kept_tag;

-- query 32
-- Append
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
INSERT INTO ice_hms_${uuid0}.iru7_${uuid0}.v3 VALUES (4, 2, 'four');

-- query 33
-- Overwrite
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
INSERT OVERWRITE ice_hms_${uuid0}.iru7_${uuid0}.v3 SELECT * FROM ice_hms_${uuid0}.iru7_${uuid0}.v1;

-- query 34
-- Position or vector row delta
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
DELETE FROM ice_hms_${uuid0}.iru7_${uuid0}.v3 WHERE id = 2;

-- query 35
-- Equality row delta
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v2 ADD EQUALITY DELETE (id) VALUES (2);

-- query 36
-- Copy-on-write mutation
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
DELETE FROM ice_hms_${uuid0}.iru7_${uuid0}.v2 WHERE id = 2;

-- query 37
-- Row mutation
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
UPDATE ice_hms_${uuid0}.iru7_${uuid0}.v3 SET note = 'refused' WHERE id = 2;

-- query 38
-- Merge row mutation
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
MERGE INTO ice_hms_${uuid0}.iru7_${uuid0}.v3 AS t USING ice_hms_${uuid0}.iru7_${uuid0}.v1 AS s ON t.id = s.id WHEN MATCHED THEN UPDATE SET note = s.note WHEN NOT MATCHED THEN INSERT (id,p,note) VALUES (s.id,s.p,s.note);

-- query 39
-- Truncate
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
TRUNCATE TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3;

-- query 40
-- Register files
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v2 ADD FILES FROM '${iceberg_hms_warehouse}/iru7/iru7_${uuid0}/v1/data';

-- query 41
-- Expire snapshots
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 EXPIRE SNAPSHOTS RETAIN LAST 1;

-- query 42
-- Rewrite manifests
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 REWRITE MANIFESTS;

-- query 43
-- Remove orphan files
-- Governed preparation preserves the original HMS refusal through its safe typed projection.
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- @expect_error=ERROR 1105 (HY000): [query.execution.unsupported] table-maintenance original operation failed
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00';

-- query 44
-- Rewrite data files
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- @expect_error=ERROR 1105 (HY000): [query.execution.unsupported] table-maintenance original operation failed
CALL ice_hms_${uuid0}.system.rewrite_data_files(table => 'iru7_${uuid0}.v3', options => map('rewrite-all','true'));

-- query 45
-- Rewrite position deletes
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- @expect_error=ERROR 1105 (HY000): [query.execution.unsupported] table-maintenance original operation failed
CALL ice_hms_${uuid0}.system.rewrite_position_delete_files(table => 'iru7_${uuid0}.v2', options => map('rewrite-all','true'));

-- query 46
-- Optimize admission
-- @expect_sql_code=query.execution.unsupported
-- @expect_sql_phase=Execution
-- @expect_error=ERROR 1105 (HY000): [query.execution.unsupported] table-maintenance original operation failed
ALTER TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 OPTIMIZE;

-- query 47
-- Analyze admission
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
ANALYZE TABLE ice_hms_${uuid0}.iru7_${uuid0}.v3 (id);

-- query 48
-- Create view
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
CREATE VIEW ice_hms_${uuid0}.iru7_${uuid0}.new_view AS SELECT id FROM ice_hms_${uuid0}.iru7_${uuid0}.v3;

-- query 49
-- Replace view
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
CREATE OR REPLACE VIEW ice_hms_${uuid0}.iru7_${uuid0}.new_view AS SELECT id FROM ice_hms_${uuid0}.iru7_${uuid0}.v3;

-- query 50
-- Drop view
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
DROP VIEW IF EXISTS ice_hms_${uuid0}.iru7_${uuid0}.new_view;

-- query 51
-- @skip_result_check=true
SET CATALOG ice_hms_${uuid0};
USE iru7_${uuid0};

-- query 52
-- Application-document creation
-- @expect_error=Hive Metastore catalog is a read-only compatibility entry
CREATE MATERIALIZED VIEW agg_mv DISTRIBUTED BY HASH(p) BUCKETS 1 PROPERTIES ('storage_engine'='iceberg') AS SELECT p, SUM(id) AS s FROM ice_hms_${uuid0}.iru7_${uuid0}.v3 GROUP BY p;

-- query 53
-- @result_rows_where=TableName=v3
-- @result_rows_count=0
SHOW ALTER TABLE OPTIMIZE FROM ice_hms_${uuid0}.iru7_${uuid0} WHERE TableName = 'v3';

-- query 54
-- @result_rows_where=catalog=ice_hms_${uuid0}
-- @result_rows_where=namespace=iru7_${uuid0}
-- @result_rows_where=table=v3
-- @result_rows_count=0
SHOW ANALYZE JOBS;

-- query 55
-- @result_contains=INVENTORY_UNCHANGED
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-hms/fixture.py" after iru7_${uuid0}

-- query 56
SELECT id, p FROM ice_hms_${uuid0}.iru7_${uuid0}.v3 ORDER BY id;

-- query 57
SELECT table_name FROM information_schema.materialized_views WHERE table_schema = 'iru7_${uuid0}' AND table_name = 'agg_mv';

-- query 58
-- @cleanup=true
-- @result_contains=HMS_CLEANUP_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/fixtures/iceberg-hms/fixture.py" cleanup iru7_${uuid0}

-- query 59
-- @cleanup=true
-- @skip_result_check=true
DROP CATALOG ice_hms_${uuid0};
