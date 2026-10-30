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
-- The publication committed and the process that committed it died before it
-- could record the projection. The lake is the authority, so what the commit
-- did stands: the next process must find the published result and serve it.
--
-- What must NOT happen is a compensating Drop or a rollback. A projection this
-- process failed to write is a fact it has not caught up with, not a fact that
-- did not happen, and undoing someone else's committed publication because a
-- local write failed would destroy a result the lake already holds.

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
CREATE MATERIALIZED VIEW orders_total
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, SUM(amount) AS total FROM orders GROUP BY region;

-- query 8
-- The refresh commits and the frontend dies before recording it.
-- @kill_fe_after_mv_known_committed_before_projector_cas=true
-- @expect_error_tier=drift
-- @expect_error=server disconnected
REFRESH MATERIALIZED VIEW orders_total WITH SYNC MODE;

-- query 9
-- The published result is there, found from the lake by the next process.
-- @retry_count=40
-- @retry_interval_ms=250
SELECT region, total
FROM mvsc_${uuid0}.ns_${uuid0}.orders_total
ORDER BY region;

-- query 10
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};

-- query 11
-- The view is still there. Nothing dropped it to make the local failure
-- consistent, and its rows are the ones the commit published. Startup
-- rediscovery runs behind catalog admission, so this waits for it.
-- @retry_count=40
-- @retry_interval_ms=250
-- @skip_result_check=true
-- @result_contains=orders_total
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 12
-- Dropping it is a management write, and management is closed for the same
-- reason a refresh would be: the process that committed is gone and its
-- dispatch cannot be accounted for from here. The declaration is what opens
-- it, which is also the shape of the cleanup an operator would have to do.
-- @mv_resume_management=orders_total,catalog=mvsc_${uuid0},database=ns_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 13
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW orders_total;
DROP TABLE mvsc_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mvsc_${uuid0}.ns_${uuid0};
-- Dropping the attachment matters: two attachments onto one warehouse make the
-- same physical MV discoverable under two catalog names, and its management
-- binds to whichever attachment discovered it first -- so the next case's
-- status query, which names its own catalog, would find nothing.
DROP CATALOG mvsc_${uuid0};
