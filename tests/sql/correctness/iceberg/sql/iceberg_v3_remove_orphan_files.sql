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
-- Exercise real orphan cleanup on an isolated REST/MinIO table with GC
-- disabled, including the retained historical snapshot and a subsequent write.
-- The dedicated Iceberg IVM case checks the materialized-view table guard.

-- query 1
-- @skip_result_check=true
-- This suite defaults to Hadoop. The orphan fixture reads this case's explicit
-- isolated REST catalog, which owns the actual table and its object prefix.
CREATE CATALOG orphan_rest_${uuid0} USING iceberg WITH (
    "iceberg.catalog.type" = 'rest',
    "uri" = '${iceberg_rest_uri}',
    "warehouse" = '${iceberg_rest_warehouse}',
    "credential.object-store-metadata.consumer-role" = 'frontend',
    "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}',
    "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}',
    "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}',
    "credential.object-store-data.consumer-role" = 'backend',
    "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}',
    "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}',
    "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}',
    "aws.s3.endpoint" = '${oss_endpoint}',
    "aws.s3.region" = '${oss_region}',
    "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA orphan_rest_${uuid0}.${case_db}
WITH (location = '${iceberg_rest_warehouse}/${case_db}');

-- query 2
-- @skip_result_check=true
CREATE TABLE orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled (id INT, v VARCHAR)
WITH (format_version = 3, gc_enabled = false, statistics_collect_on_write = true);

-- query 3
-- @skip_result_check=true
INSERT INTO orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled VALUES ROW(1, 'history');

-- query 4
-- @skip_result_check=true
ALTER TABLE orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled
EXECUTE create_tag(name => 'before_cleanup', replace => false, if_not_exists => false);

-- query 5
-- @skip_result_check=true
INSERT INTO orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled VALUES ROW(2, 'current');

-- query 6
-- @expect_error=Retention specified (1.00d) is shorter than the minimum retention configured in the system (7.00d). Minimum retention can be changed with iceberg.remove-orphan-files.min-retention configuration property or iceberg.remove_orphan_files_min_retention session property
ALTER TABLE orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled EXECUTE remove_orphan_files(retention_threshold => '1d');

-- query 7
-- @iceberg_orphan_fixture=orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled
-- @iceberg_orphan_fixture_absent=true
-- Protect two data files, two manifests, two manifest lists, four metadata
-- JSON files, and two Puffin statistics files. Scan those plus one orphan.
-- The fixture is installed before the step. The session statement completes
-- before EXECUTE freezes its cutoff. The strict equal-time boundary is
-- verified with independent file times in the Provider tests.
SET SESSION orphan_rest_${uuid0}.remove_orphan_files_min_retention = '0s';
ALTER TABLE orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled EXECUTE remove_orphan_files(retention_threshold => '0s');

-- query 8
SELECT id, v FROM orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled ORDER BY id;

-- query 9
SELECT id, v FROM orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled FOR VERSION AS OF 'before_cleanup' ORDER BY id;

-- query 10
-- @skip_result_check=true
CREATE TABLE orphan_rest_${uuid0}.${case_db}.empty_v2 (id INT) WITH (format_version = 2);

-- query 11
ALTER TABLE orphan_rest_${uuid0}.${case_db}.empty_v2 EXECUTE remove_orphan_files(retention_threshold => '0s');

-- query 12
-- @skip_result_check=true
RESET SESSION orphan_rest_${uuid0}.remove_orphan_files_min_retention;

-- query 13
-- @expect_error=table procedure 'REMOVE_ORPHAN_FILES' property 'older_than' does not exist
ALTER TABLE orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled EXECUTE remove_orphan_files(older_than => 1700000000000);

-- query 14
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled@dev EXECUTE remove_orphan_files(retention_threshold => '7d');

-- query 15
-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR
ALTER TABLE orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled REMOVE ORPHAN FILES OLDER THAN '2020-01-01 00:00:00' WHERE size_in_bytes < 100;

-- query 16
-- @skip_result_check=true
INSERT INTO orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled VALUES ROW(3, 'after');

-- query 17
SELECT id, v FROM orphan_rest_${uuid0}.${case_db}.orphan_gc_disabled ORDER BY id;

-- query 18
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS orphan_rest_${uuid0}.${case_db} CASCADE;
DROP CATALOG IF EXISTS orphan_rest_${uuid0};
