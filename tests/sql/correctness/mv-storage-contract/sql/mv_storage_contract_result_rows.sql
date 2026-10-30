-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract,logical-result-rows,deletion-vector

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvrows_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvrows_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvrows_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvrows_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10), ('west', 20);

-- query 5
-- @skip_result_check=true
SET CATALOG mvrows_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW rows_mv
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 6
-- @skip_result_check=true
DELETE FROM fact WHERE k = 'east';

-- query 7
-- The MV delta deletes one logical row through a v3 deletion vector. Physical
-- data-file records are still two; SHOW and P must report the one visible row.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=2,slot-changes=0,logical-rows=1,total-records=2,delete-files-min=1
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 8
-- @skip_result_check=true
-- @result_rows_where=Name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=LastRefreshRows=1
SHOW MATERIALIZED VIEWS;

-- query 9
SELECT k, v FROM rows_mv ORDER BY k;

-- query 10
-- @skip_result_check=true
INSERT INTO mvrows_${uuid0}.ns_${uuid0}.fact VALUES ('north', 30);

-- query 11
-- This refresh accepts one new row, while the complete target contains two.
-- Its physical data-file records include the previously deleted east row.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=3,slot-changes=0,logical-rows=2,total-records=3,delete-files-min=1
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 12
-- @skip_result_check=true
-- @result_rows_where=Name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=LastRefreshRows=2
SHOW MATERIALIZED VIEWS;

-- query 13
SELECT k, v FROM rows_mv ORDER BY k;

-- query 14
-- An unchanged source produces a metadata-only publication. Its zero accepted
-- rows must preserve the complete target's logical row count and physical files.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=4,slot-changes=0,logical-rows=2,total-records=3,delete-files-min=1,metadata-only-last=true
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 15
-- @skip_result_check=true
-- @result_rows_where=Name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=LastRefreshRows=2
SHOW MATERIALIZED VIEWS;

-- query 16
SELECT k, v FROM rows_mv ORDER BY k;

-- query 17
-- @skip_result_check=true
DELETE FROM fact WHERE k IN ('west', 'north');

-- query 18
-- Deleting both remaining rows accepts two changes, but leaves zero target rows.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.rows_mv,publications=5,slot-changes=0,logical-rows=0
REFRESH MATERIALIZED VIEW rows_mv WITH SYNC MODE;

-- query 19
-- @skip_result_check=true
-- @result_rows_where=Name=rows_mv
-- @result_rows_count=1
-- @result_rows_expect=LastRefreshRows=0
SHOW MATERIALIZED VIEWS;

-- query 20
SELECT COUNT(*) AS row_count FROM rows_mv;

-- query 21
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG mvrows_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS rows_mv;
DROP TABLE mvrows_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE mvrows_${uuid0}.ns_${uuid0};
DROP CATALOG mvrows_${uuid0};
