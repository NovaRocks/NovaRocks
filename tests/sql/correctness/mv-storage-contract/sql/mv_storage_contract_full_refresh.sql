-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract
-- FULL publishes consecutive delete/append snapshots in one REST commit.
-- Lose the second FULL's commit response and require exact reconciliation.

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvfull_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvfull_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvfull_${uuid0}.ns_${uuid0}.fact (k VARCHAR, v BIGINT) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
INSERT INTO mvfull_${uuid0}.ns_${uuid0}.fact VALUES ROW('east', 10), ROW('east', 10), ROW('west', 20);

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=0,slot-changes=0,table-commits=1
USE mvfull_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW target_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;

-- query 6
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,slot-changes=0,table-commits=2,full-refreshes=1,full-refresh-last=true,logical-rows=3,total-records=3
REFRESH MATERIALIZED VIEW target_mv;

-- query 7
-- @skip_result_check=true
INSERT INTO mvfull_${uuid0}.ns_${uuid0}.fact VALUES ROW('east', 10), ROW('north', 30);
SET SESSION "mvfull_${uuid0}".incremental_refresh_enabled = false;

-- query 8
-- @skip_result_check=true
-- @publication_catalog_fault=table-commit,after-commit-before-response
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=2,slot-changes=0,table-commits=3,full-refreshes=2,full-refresh-last=true,logical-rows=5,total-records=5
REFRESH MATERIALIZED VIEW target_mv;

-- query 9
SELECT k, v FROM target_mv ORDER BY k, v;

-- query 10
-- @cleanup=true
-- @skip_result_check=true
RESET SESSION "mvfull_${uuid0}".incremental_refresh_enabled;
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.target_mv;
DROP TABLE IF EXISTS mvfull_${uuid0}.ns_${uuid0}.fact;
DROP SCHEMA IF EXISTS mvfull_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS mvfull_${uuid0};
