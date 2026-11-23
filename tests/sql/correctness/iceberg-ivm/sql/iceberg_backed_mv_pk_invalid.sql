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
CREATE CATALOG mv_ivm_pk_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA mv_ivm_pk_${uuid0}.ns_${uuid0};
CREATE TABLE mv_ivm_pk_${uuid0}.ns_${uuid0}.orders (
  order_id BIGINT NOT NULL,
  customer VARCHAR,
  amount DOUBLE,
  tags ARRAY<VARCHAR>
) WITH (format_version = 3);
INSERT INTO mv_ivm_pk_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(1, 'A', 100.0, ['x']),
  ROW(2, 'B', 200.0, ['y']);
USE mv_ivm_pk_${uuid0}.ns_${uuid0};

-- query 2
-- @expect_error=iceberg-backed aggregate materialized views do not support PRIMARY KEY
CREATE MATERIALIZED VIEW mv_pk_missing WITH (primary_key = ARRAY['bogus']) AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 3
-- @expect_error=iceberg-backed aggregate materialized views do not support PRIMARY KEY
CREATE MATERIALIZED VIEW mv_pk_nullable WITH (primary_key = ARRAY['customer']) AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 4
-- @expect_error=iceberg-backed aggregate materialized views do not support PRIMARY KEY
CREATE MATERIALIZED VIEW mv_pk_nullable2 WITH (primary_key = ARRAY['amount']) AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 5
-- @expect_trino_error=INVALID_MATERIALIZED_VIEW_PROPERTY,USER_ERROR
CREATE MATERIALIZED VIEW mv_pk_empty WITH (primary_key = ARRAY[])
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 6
-- @expect_trino_error=INVALID_MATERIALIZED_VIEW_PROPERTY,USER_ERROR
CREATE MATERIALIZED VIEW mv_pk_dupe WITH (primary_key = ARRAY['order_id', 'order_id'])
AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_no_pk AS SELECT customer, count(*) AS c
FROM orders
GROUP BY customer;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_no_pk;
DROP TABLE mv_ivm_pk_${uuid0}.ns_${uuid0}.orders ;
DROP SCHEMA mv_ivm_pk_${uuid0}.ns_${uuid0};
DROP CATALOG mv_ivm_pk_${uuid0};
