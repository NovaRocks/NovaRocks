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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,validation
-- Test Objective:
-- 1. Validate that CREATE MATERIALIZED VIEW with PRIMARY KEY is rejected for
--    Iceberg-target aggregate MVs (PRIMARY KEY is unsupported at the storage level).
-- 2. Confirm that omitting PRIMARY KEY is accepted (unchanged behavior).
-- Note: The per-column validations (missing column, nullable, empty PK, duplicate)
--   are superseded by the iceberg-target aggregate MV blanket rejection, which fires
--   before any column-level check.
-- MV is Iceberg-target (PROPERTIES('storage_engine'='iceberg')).

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_ivm_pk_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_pk_${uuid0}",
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
CREATE DATABASE mv_ivm_pk_${uuid0}.ns_${uuid0};
CREATE TABLE mv_ivm_pk_${uuid0}.ns_${uuid0}.orders (
  order_id BIGINT NOT NULL,
  customer STRING,
  amount DOUBLE,
  tags ARRAY<STRING>
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_ivm_pk_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 100.0, ['x']),
  (2, 'B', 200.0, ['y']);
SET CATALOG mv_ivm_pk_${uuid0};
USE ns_${uuid0};

-- query 2
-- @expect_error=iceberg-backed aggregate materialized views do not support PRIMARY KEY
CREATE MATERIALIZED VIEW mv_pk_missing
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (bogus)
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 3
-- @expect_error=iceberg-backed aggregate materialized views do not support PRIMARY KEY
CREATE MATERIALIZED VIEW mv_pk_nullable
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (customer)
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 4
-- @expect_error=iceberg-backed aggregate materialized views do not support PRIMARY KEY
CREATE MATERIALIZED VIEW mv_pk_nullable2
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (amount)
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 5
-- @expect_error=PRIMARY KEY clause requires at least one column
CREATE MATERIALIZED VIEW mv_pk_empty
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY ()
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 6
-- @expect_error=duplicate column `order_id` in PRIMARY KEY clause
CREATE MATERIALIZED VIEW mv_pk_dupe
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (order_id, order_id)
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_no_pk
DISTRIBUTED BY HASH(customer) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_no_pk;
DROP TABLE mv_ivm_pk_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mv_ivm_pk_${uuid0}.ns_${uuid0};
DROP CATALOG mv_ivm_pk_${uuid0};
