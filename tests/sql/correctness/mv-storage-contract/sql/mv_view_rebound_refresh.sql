-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,storage-contract

-- query 1
-- @skip_result_check=true
CREATE CATALOG rwm10b_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA rwm10b_${uuid0}.ns_${uuid0};
USE rwm10b_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE original (id BIGINT NOT NULL) WITH (format_version = 3);
CREATE TABLE replacement (id BIGINT NOT NULL) WITH (format_version = 3);
INSERT INTO original VALUES (1), (1);
INSERT INTO replacement VALUES (2), (2);
CREATE VIEW source_view AS SELECT id FROM original;
CREATE MATERIALIZED VIEW target_mv WITH (refresh_policy = 'MANUAL') AS SELECT id FROM source_view;

-- query 2
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,table-commits=2,full-refreshes=1,logical-rows=2,source=original,markers=clear
REFRESH MATERIALIZED VIEW target_mv;

-- query 3
SELECT id FROM target_mv ORDER BY id;

-- query 4
SELECT id FROM source_view ORDER BY id;

-- query 5
-- @skip_result_check=true
CREATE OR REPLACE VIEW source_view AS
SELECT id FROM replacement WHERE IF(false, random() < 0e0, true);

-- query 6
-- @skip_result_check=true
-- @result_contains=MV refresh: InvalidDefinition
-- @result_contains=Materialized view definition cannot use volatile function 'random'
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,table-commits=2,full-refreshes=1,logical-rows=2,source=original,markers=clear
EXPLAIN REFRESH MATERIALIZED VIEW target_mv;

-- query 7
-- @expect_trino_error=VIEW_IS_STALE,USER_ERROR
REFRESH MATERIALIZED VIEW target_mv;

-- query 8
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=1,table-commits=3,full-refreshes=1,logical-rows=2,source=original,markers=invalid-definition-publication
SELECT 1;

-- query 9
SELECT id FROM target_mv ORDER BY id;

-- query 10
-- @skip_result_check=true
CREATE OR REPLACE VIEW source_view AS SELECT id FROM replacement;

-- query 11
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=2,table-commits=5,full-refreshes=2,full-refresh-last=true,logical-rows=2,binding-changes=1,source=replacement,markers=clear
REFRESH MATERIALIZED VIEW target_mv;

-- query 12
SELECT id FROM target_mv ORDER BY id;

-- query 13
SELECT id FROM source_view ORDER BY id;

-- query 14
-- @skip_result_check=true
INSERT INTO replacement VALUES (3);

-- query 15
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=3,table-commits=6,full-refreshes=2,append-last=true,logical-rows=3,binding-changes=1,source=replacement,markers=clear
REFRESH MATERIALIZED VIEW target_mv;

-- query 16
SELECT id FROM target_mv ORDER BY id;

-- query 17
SELECT id FROM source_view ORDER BY id;

-- query 18
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.target_mv;
DROP VIEW IF EXISTS ns_${uuid0}.source_view;
DROP TABLE IF EXISTS ns_${uuid0}.original;
DROP TABLE IF EXISTS ns_${uuid0}.replacement;
DROP SCHEMA IF EXISTS rwm10b_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS rwm10b_${uuid0};
