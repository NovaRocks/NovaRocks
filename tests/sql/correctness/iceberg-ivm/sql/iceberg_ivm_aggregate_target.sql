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
-- @tags=mv,iceberg,ivm,aggregate,target_state
-- Test Point: Iceberg-backed aggregate MV stores aggregate state and refreshes incrementally.
-- Method: Create v3 row-lineage base table, create storage_engine='iceberg' aggregate MV, refresh after insert/delete/update, and verify hidden state isolation.
-- Scope: Iceberg target MV, single-base aggregate, COUNT/SUM/AVG, group row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_agg_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_agg_${uuid0}",
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
CREATE DATABASE ice_ivm_agg_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_agg_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_agg_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW agg_mv_${uuid0}
DISTRIBUTED BY HASH(region) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT region, COUNT(*) AS c, COUNT(amount) AS c_amount, SUM(amount) AS s, AVG(amount) AS a
FROM ice_ivm_agg_${uuid0}.ns_${uuid0}.orders
GROUP BY region;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_${uuid0}.ns_${uuid0}.orders VALUES
  ('east', 10),
  ('east', 20),
  ('west', NULL);
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 3
SELECT region, c, c_amount, s, a
FROM agg_mv_${uuid0}
ORDER BY region;

-- query 4
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_${uuid0}.ns_${uuid0}.orders VALUES ('east', 30);

-- query 5
-- @skip_result_check=true
-- @explain_contains=LEFT OUTER JOIN
-- @explain_contains=IcebergMvTargetState
-- @explain_contains=__change_op
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 6
SELECT region, c, c_amount, s, a
FROM agg_mv_${uuid0}
ORDER BY region;

-- query 7
-- @skip_result_check=true
INSERT INTO ice_ivm_agg_${uuid0}.ns_${uuid0}.orders VALUES ('north', 5);
DELETE FROM ice_ivm_agg_${uuid0}.ns_${uuid0}.orders WHERE region = 'west';
UPDATE ice_ivm_agg_${uuid0}.ns_${uuid0}.orders SET amount = 40 WHERE region = 'east' AND amount = 10;

-- query 8
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW agg_mv_${uuid0};

-- query 9
SELECT region, c, c_amount, s, a
FROM agg_mv_${uuid0}
ORDER BY region;

-- query 10
-- @expect_error=Column '__row_id__' cannot be resolved
SELECT __row_id__ FROM agg_mv_${uuid0};

-- query 11
-- @expect_error=Column '__agg_state_c' cannot be resolved
SELECT __agg_state_c FROM agg_mv_${uuid0};

-- query 12
-- @skip_result_check=true
DROP MATERIALIZED VIEW agg_mv_${uuid0};
DROP TABLE ice_ivm_agg_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE ice_ivm_agg_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_agg_${uuid0};
