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
-- @tags=mv,iceberg,ivm,aggregate,partitioned,day_transform,target_state
-- Test Point: Iceberg-backed partitioned aggregate MV using day(ts) transform on a
-- DATE group key refreshes incrementally with partition-pruned target state lookup.
-- Method: Create v3 row-lineage base, create PARTITION BY day(ts) aggregate MV.
-- Insert rows spanning multiple days; update one day's amounts; verify MV matches
-- the equivalent base query after each refresh.
-- Scope: Iceberg target MV, single-base aggregate, day-transform partition, group
-- row-id apply.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_pday_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_pday_${uuid0}",
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
CREATE DATABASE ice_ivm_pday_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_pday_${uuid0}.ns_${uuid0}.orders (
  ts DATE,
  amount BIGINT
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
SET CATALOG ice_ivm_pday_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW pday_mv_${uuid0}
PARTITION BY day(ts)
DISTRIBUTED BY HASH(ts) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT ts, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_pday_${uuid0}.ns_${uuid0}.orders
GROUP BY ts;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_pday_${uuid0}.ns_${uuid0}.orders VALUES
  ('2026-01-10', 10),
  ('2026-01-10', 20),
  ('2026-01-11', 5),
  ('2026-01-12', 15);
REFRESH MATERIALIZED VIEW pday_mv_${uuid0};

-- query 3
SELECT ts, c, s
FROM pday_mv_${uuid0}
ORDER BY ts;

-- query 4
-- @skip_result_check=true
UPDATE ice_ivm_pday_${uuid0}.ns_${uuid0}.orders SET amount = 50 WHERE ts = DATE '2026-01-11';
REFRESH MATERIALIZED VIEW pday_mv_${uuid0};

-- query 5
SELECT ts, c, s
FROM pday_mv_${uuid0}
ORDER BY ts;

-- query 6
SELECT ts, c, s
FROM pday_mv_${uuid0}
ORDER BY ts;

SELECT ts, COUNT(*) AS c, SUM(amount) AS s
FROM ice_ivm_pday_${uuid0}.ns_${uuid0}.orders
GROUP BY ts
ORDER BY ts;

-- query 7
-- @skip_result_check=true
DROP MATERIALIZED VIEW pday_mv_${uuid0};
DROP TABLE ice_ivm_pday_${uuid0}.ns_${uuid0}.orders;
DROP DATABASE ice_ivm_pday_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_pday_${uuid0};
