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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,projection_filter,row_lineage,delete,partition_evolution
-- Test Point:
--   Validate projection MV incremental refresh over Iceberg v3 row-lineage
--   deletion-vector deletes after partition evolution.
-- Method:
--   Create a partitioned row-lineage table, evolve the partition spec, delete
--   one old-spec row and one new-spec row, refresh MV, and verify both rows
--   retract.
-- Scope:
--   Iceberg-target projection/filter MV on an Iceberg v3 row-lineage base
--   table with evolved partition specs.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_read_sem_part_v3_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/mv_read_sem_part_v3_${uuid0}",
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
CREATE DATABASE mv_read_sem_part_v3_${uuid0}.ns_${uuid0};
CREATE TABLE mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  customer STRING,
  amount BIGINT
)
PARTITION BY (customer)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 10),
  (2, 'A', 20);

-- query 2
-- @skip_result_check=true
ALTER TABLE mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders DROP PARTITION COLUMN customer;

-- query 3
-- @skip_result_check=true
INSERT INTO mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders VALUES
  (3, 'B', 30),
  (4, 'B', 40);

-- query 4
-- @skip_result_check=true
SET CATALOG mv_read_sem_part_v3_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW mv_read_sem_part_v3_orders
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT id, customer, amount
FROM orders
WHERE amount >= 10;

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_read_sem_part_v3_orders;

-- query 6
SELECT id, customer, amount
FROM mv_read_sem_part_v3_orders
ORDER BY id;

-- query 7
-- @skip_result_check=true
DELETE FROM mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders WHERE id IN (1, 3);

-- query 8
SELECT id, customer, amount
FROM mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders
ORDER BY id;

-- query 9
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_read_sem_part_v3_orders;

-- query 10
SELECT id, customer, amount
FROM mv_read_sem_part_v3_orders
ORDER BY id;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_read_sem_part_v3_orders;
DROP TABLE mv_read_sem_part_v3_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_read_sem_part_v3_${uuid0}.ns_${uuid0};
DROP CATALOG mv_read_sem_part_v3_${uuid0};
