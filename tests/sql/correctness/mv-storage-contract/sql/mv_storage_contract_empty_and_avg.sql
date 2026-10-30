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
-- @tags=mv,iceberg,rest,minio,storage-contract
-- Test Objective:
-- Two publication shapes the storage contract has to carry on the product
-- topology:
--   1. A view over an empty source. Its first refresh produces nothing and
--      must still publish: the view exists, is readable, and has a
--      publication to be rewritten against. A target left without a snapshot
--      would have no version to report and nothing to refresh from later.
--   2. AVG. The published columns are the state the view stores, not the
--      value it shows, so the read has to come back as an average.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvsc_${uuid0}
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
CREATE DATABASE mvsc_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvsc_${uuid0}.ns_${uuid0}.readings (
  sensor STRING,
  value BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
-- A source without any snapshot has no exact read point. Leave a current
-- snapshot whose visible rows are empty, so the first refresh can publish a
-- proven zero-row result rather than skip an unversioned source.
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.readings VALUES ('seed', 0);
DELETE FROM mvsc_${uuid0}.ns_${uuid0}.readings WHERE sensor = 'seed';

-- query 4
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};

-- query 5
-- @skip_result_check=true
USE ns_${uuid0};

-- query 6
-- The source is empty at CREATE and still empty at the first refresh.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.readings_total,publications=0,slot-changes=0
CREATE MATERIALIZED VIEW readings_total
DISTRIBUTED BY HASH(sensor) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT sensor, SUM(value) AS total FROM readings GROUP BY sensor;

-- query 7
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.readings_total,publications=1,slot-changes=0
REFRESH MATERIALIZED VIEW readings_total WITH SYNC MODE;

-- query 8
-- A view over an empty source is a view, not an error.
SELECT sensor, total FROM readings_total ORDER BY sensor;

-- query 9
-- It has a publication, so it is manageable rather than stuck.
-- @skip_result_check=true
-- @result_contains=readings_total
-- @result_contains=MANAGEABLE
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 10
-- @skip_result_check=true
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.readings VALUES ('a', 10), ('a', 20), ('b', 7);

-- query 11
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.readings_total,publications=2,slot-changes=0
REFRESH MATERIALIZED VIEW readings_total WITH SYNC MODE;

-- query 12
SELECT sensor, total FROM readings_total ORDER BY sensor;

-- query 13
-- AVG stores its state and shows its value; the two are not the same columns.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.readings_avg,publications=0,slot-changes=0
CREATE MATERIALIZED VIEW readings_avg
DISTRIBUTED BY HASH(sensor) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT sensor, AVG(value) AS mean FROM readings GROUP BY sensor;

-- query 14
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.readings_avg,publications=1,slot-changes=0
REFRESH MATERIALIZED VIEW readings_avg WITH SYNC MODE;

-- query 15
SELECT sensor, mean FROM readings_avg ORDER BY sensor;

-- query 16
-- @skip_result_check=true
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.readings VALUES ('a', 60), ('b', 3);

-- query 17
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.readings_avg,publications=2,slot-changes=0
REFRESH MATERIALIZED VIEW readings_avg WITH SYNC MODE;

-- query 18
SELECT sensor, mean FROM readings_avg ORDER BY sensor;

-- query 19
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW readings_avg;
DROP MATERIALIZED VIEW readings_total;
DROP TABLE mvsc_${uuid0}.ns_${uuid0}.readings FORCE;
DROP DATABASE mvsc_${uuid0}.ns_${uuid0};
-- Dropping the attachment matters: two attachments onto one warehouse make the
-- same physical MV discoverable under two catalog names, and its management
-- binds to whichever attachment discovered it first -- so the next case's
-- status query, which names its own catalog, would find nothing.
DROP CATALOG mvsc_${uuid0};
