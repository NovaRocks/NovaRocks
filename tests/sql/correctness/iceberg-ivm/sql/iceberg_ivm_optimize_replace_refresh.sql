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
-- @tags=mv,iceberg,ivm,aggregate,optimize,replace_snapshot
-- Test Point: an incremental aggregate MV crosses an Iceberg OPTIMIZE replace
-- snapshot and still accounts for the append before and after the rewrite.
-- Method: initial REFRESH, append a second batch, OPTIMIZE the base table,
-- append a third batch, then REFRESH and compare with the base aggregate.
-- Scope: native FE/BE writer, DML, OPTIMIZE and IMV refresh lifecycle.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_opt_replace_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/ice_ivm_opt_replace_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_opt_replace_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_opt_replace_${uuid0}.ns_${uuid0}.fact (
  id BIGINT NOT NULL,
  region STRING,
  amount BIGINT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_opt_replace_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW fact_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, COUNT(*) AS c, SUM(amount) AS s
FROM fact
GROUP BY region;
INSERT INTO fact VALUES (1, 'east', 10);
REFRESH MATERIALIZED VIEW fact_mv_${uuid0};

-- query 2
-- @skip_result_check=true
INSERT INTO fact VALUES (2, 'west', 5);

-- query 3
-- @skip_result_check=true
-- @wait_alter_optimize=fact
ALTER TABLE fact OPTIMIZE;

-- query 4
-- @skip_result_check=true
INSERT INTO fact VALUES (3, 'east', 7);
REFRESH MATERIALIZED VIEW fact_mv_${uuid0};

-- query 5
-- @imv_equivalence_check=fact_mv_${uuid0}
SELECT region, c, s
FROM fact_mv_${uuid0}
ORDER BY region;

-- query 6
-- @skip_result_check=true
DROP MATERIALIZED VIEW fact_mv_${uuid0};
DROP TABLE ice_ivm_opt_replace_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE ice_ivm_opt_replace_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_opt_replace_${uuid0};
