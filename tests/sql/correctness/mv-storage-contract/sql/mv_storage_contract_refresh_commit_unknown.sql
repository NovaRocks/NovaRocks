-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract
-- FULL publishes consecutive delete/append snapshots in one REST commit.
-- Hide the response and readback for the original query. The same owner
-- must keep the unresolved effect guarded until an admitted observation.

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvunknown_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvunknown_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvunknown_${uuid0}.ns_${uuid0}.fact (k VARCHAR, v BIGINT) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
INSERT INTO mvunknown_${uuid0}.ns_${uuid0}.fact VALUES ROW('east', 10), ROW('east', 10), ROW('west', 20);

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=0,slot-changes=0,table-commits=1
USE mvunknown_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW target_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;

-- query 6
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,slot-changes=0,table-commits=2,full-refreshes=1,full-refresh-last=true,logical-rows=3,total-records=3
REFRESH MATERIALIZED VIEW target_mv;

-- query 7
-- @skip_result_check=true
INSERT INTO mvunknown_${uuid0}.ns_${uuid0}.fact VALUES ROW('east', 10), ROW('north', 30);
SET SESSION "mvunknown_${uuid0}".incremental_refresh_enabled = false;

-- query 8
-- @retry_count=1
-- @publication_catalog_fault=table-commit,after-commit-before-response-hide-table-load
-- @publication_catalog_fault_target=namespace=ns_${uuid0},table=target_mv
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit the transaction during refresh materialized view: [CommitUnknown]
REFRESH MATERIALIZED VIEW target_mv;

-- query 9
-- The proxy guard verifies the lost-response query made no second dispatch.
-- Once the fault clears, independently read the exact second P and stages.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=2,slot-changes=0,table-commits=3,full-refreshes=2,full-refresh-last=true,logical-rows=5,total-records=5,source=fact
SELECT 1;

-- query 10
-- @skip_result_check=true
-- @result_rows_where=Property=UnsettledEffects
-- @result_rows_count=1
-- @result_rows_expect=Value=1
CALL novarocks_mv_management_status('mvunknown_${uuid0}', 'ns_${uuid0}', 'target_mv');

-- query 11
-- @skip_result_check=true
-- @mv_resume_management=target_mv,catalog=mvunknown_${uuid0},database=ns_${uuid0}
SELECT 1;

-- query 12
-- @skip_result_check=true
-- @result_rows_where=Property=UnsettledEffects
-- @result_rows_count=1
-- @result_rows_expect=Value=0
CALL novarocks_mv_management_status('mvunknown_${uuid0}', 'ns_${uuid0}', 'target_mv');

-- query 13
SELECT k, v FROM target_mv ORDER BY k, v;

-- query 14
-- @cleanup=true
-- @skip_result_check=true
RESET SESSION "mvunknown_${uuid0}".incremental_refresh_enabled;
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.target_mv;
DROP TABLE IF EXISTS mvunknown_${uuid0}.ns_${uuid0}.fact;
DROP SCHEMA IF EXISTS mvunknown_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS mvunknown_${uuid0};
