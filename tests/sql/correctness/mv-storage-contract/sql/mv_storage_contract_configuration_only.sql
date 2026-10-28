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
-- @tags=mv,iceberg,rest,minio,storage-contract,configuration
-- Configuration statements commit main-conditioned C slot changes. The
-- runner checks the actual REST graph and exact publication references;
-- ordinary table properties must also reobserve keyed Current successfully.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvcfg_${uuid0}
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
CREATE DATABASE mvcfg_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvcfg_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvcfg_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10);

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=0,slot-changes=0
SET CATALOG mvcfg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_config
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM fact;

-- query 6
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=1,slot-changes=0
REFRESH MATERIALIZED VIEW mv_config WITH SYNC MODE;

-- query 7
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=1,slot-changes=1,table-commits=3
ALTER MATERIALIZED VIEW mv_config PAUSE REFRESH;

-- query 8
-- @result_contains=mv_config
-- @result_contains=PAUSED
SHOW MATERIALIZED VIEWS;

-- query 9
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=1,slot-changes=2,table-commits=4
ALTER MATERIALIZED VIEW mv_config SET REFRESH ASYNC ON CHANGE;

-- query 10
-- @result_contains=mv_config
-- @result_contains=ASYNC_ON_CHANGE
-- @result_contains=PAUSED
SHOW MATERIALIZED VIEWS;

-- query 11
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=1,slot-changes=3,table-commits=5
ALTER MATERIALIZED VIEW mv_config SET REFRESH DEFERRED MANUAL;

-- query 12
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=1,slot-changes=4,table-commits=6
ALTER MATERIALIZED VIEW mv_config RESUME REFRESH;

-- query 13
-- @result_contains=east
-- @result_contains=10
SELECT k, v FROM mv_config ORDER BY k, v;

-- query 14
-- @result_contains=mv_config
-- @result_contains=MANUAL
-- @result_contains=false
SHOW MATERIALIZED VIEWS;

-- query 15
-- A later publication observes the revised C but still uses the same D/L/P
-- dependencies. Its metadata-only snapshot must preserve the final policy.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=2,slot-changes=4,table-commits=7
REFRESH MATERIALIZED VIEW mv_config WITH SYNC MODE;

-- query 16
-- @result_contains=mv_config
-- @result_contains=MANUAL
-- @result_contains=false
SHOW MATERIALIZED VIEWS;

-- query 17
-- A metadata-only user property update must not require a legacy package.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=2,slot-changes=4,table-commits=8
ALTER MATERIALIZED VIEW mv_config SET TBLPROPERTIES ("read.split.target-size" = "16777216");

-- query 18
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_config,publications=3,slot-changes=4,table-commits=9
REFRESH MATERIALIZED VIEW mv_config WITH SYNC MODE;

-- query 19
-- @result_contains=east
-- @result_contains=10
SELECT k, v FROM mv_config ORDER BY k, v;

-- query 20
-- @result_contains=mv_config
-- @result_contains=MANUAL
-- @result_contains=false
SHOW MATERIALIZED VIEWS;

-- query 21
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG mvcfg_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW mv_config;
DROP TABLE mvcfg_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE mvcfg_${uuid0}.ns_${uuid0};
DROP CATALOG mvcfg_${uuid0};
