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
-- @tags=iceberg,ivm,content_apply,source_admission

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_v2_admission_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_v2_admission_${uuid0}.ns_${uuid0};
USE ivm_v2_admission_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE orders (id BIGINT NOT NULL, label VARCHAR) WITH (format_version = 2);
INSERT INTO orders VALUES ROW(1, 'old'), ROW(2, 'keep');
CREATE MATERIALIZED VIEW order_labels AS SELECT label FROM orders;
REFRESH MATERIALIZED VIEW order_labels;

-- query 2
-- @imv_equivalence_check=order_labels
SELECT label FROM order_labels ORDER BY label;

-- query 3
-- @skip_result_check=true
INSERT INTO orders VALUES ROW(3, 'new');
REFRESH MATERIALIZED VIEW order_labels;

-- query 4
-- @imv_equivalence_check=order_labels
SELECT label FROM order_labels ORDER BY label;

-- query 5
-- @expect_sql_code=query.execution.invalid_data
-- @expect_error=Iceberg format-version=3
CREATE MATERIALIZED VIEW orders_by_label AS SELECT label, count(*) AS n FROM orders GROUP BY label;

-- query 6
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS orders_by_label;
DROP MATERIALIZED VIEW IF EXISTS order_labels;
DROP TABLE IF EXISTS orders;
DROP SCHEMA IF EXISTS ivm_v2_admission_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ivm_v2_admission_${uuid0};
