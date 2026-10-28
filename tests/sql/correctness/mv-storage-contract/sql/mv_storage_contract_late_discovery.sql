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
-- @tags=mv,iceberg,rest,minio,storage-contract,cold-restart
-- Test Objective:
-- Two published views, and a startup that only discovers one of them.
--
-- The one it found installs. The one it missed is absent -- and absent is all
-- it is: nothing drops it to make the inventory self-consistent, and its
-- published result stays in the lake untouched. When a later startup sees the
-- whole namespace, the late view installs beside the first, which is not
-- rolled back to make room for it.
--
-- An incomplete listing is a fact about the listing, not about the views.

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
CREATE TABLE mvsc_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.orders VALUES ('east', 10), ('east', 20), ('west', 7);

-- query 5
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW a_total
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, SUM(amount) AS total FROM orders GROUP BY region;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW a_total WITH SYNC MODE;

-- query 9
-- @skip_result_check=true
CREATE MATERIALIZED VIEW z_count
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, COUNT(*) AS rows_in FROM orders GROUP BY region;

-- query 10
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW z_count WITH SYNC MODE;

-- query 11
-- @skip_result_check=true
-- @result_contains=a_total
-- @result_contains=z_count
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 12
-- Restart with a listing that does not name everything in the namespace.
-- @publication_catalog_fault=namespace-list,incomplete-discovery
-- @restart_fe_after_step=true
-- @skip_result_check=true
SELECT 1;

-- query 13
-- One view is missing from the inventory. Nothing dropped it.
-- @skip_result_check=true
-- @result_not_contains=a_mv
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 14
-- Its published result is still exactly what it published.
-- @retry_count=40
-- @retry_interval_ms=250
SELECT region, total
FROM mvsc_${uuid0}.ns_${uuid0}.a_total
ORDER BY region;

-- query 15
-- A complete listing installs the late view beside the first.
-- @restart_fe_after_step=true
-- @skip_result_check=true
SELECT 1;

-- query 16
-- Both views come back to the inventory. Rediscovery runs behind catalog
-- admission rather than inside it, so this waits for it rather than assuming
-- it finished before the connection did.
-- @retry_count=40
-- @retry_interval_ms=250
-- @skip_result_check=true
-- @result_contains=z_count
-- @result_contains=a_total
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 17
-- Its management is closed behind the restart barrier, not lost: the
-- declaration below retires it, which is the same route any restarted target
-- takes.
-- @skip_result_check=true
-- @result_contains=AWAITING_EFFECT_SETTLEMENT
CALL novarocks_mv_management_status('mvsc_${uuid0}', 'ns_${uuid0}', 'a_total');

-- query 18
SELECT region, rows_in
FROM mvsc_${uuid0}.ns_${uuid0}.z_count
ORDER BY region;

-- query 19
-- @mv_resume_management=a_total,catalog=mvsc_${uuid0},database=ns_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 20
-- @mv_resume_management=z_count,catalog=mvsc_${uuid0},database=ns_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 21
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW z_count;
DROP MATERIALIZED VIEW a_total;
DROP TABLE mvsc_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mvsc_${uuid0}.ns_${uuid0};
-- Dropping the attachment matters: two attachments onto one warehouse make the
-- same physical MV discoverable under two catalog names, and its management
-- binds to whichever attachment discovered it first -- so the next case's
-- status query, which names its own catalog, would find nothing.
DROP CATALOG mvsc_${uuid0};
