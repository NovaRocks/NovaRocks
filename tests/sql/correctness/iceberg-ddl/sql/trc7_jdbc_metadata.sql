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
-- @tags=iceberg,view,system_catalog,jdbc
-- Trino 484's JDBC table and column projections over one Iceberg v3
-- relation and one view. The visible ordinal is assigned before column filters.

-- query 1
-- @skip_result_check=true
CREATE CATALOG trc_jdbc_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA trc_jdbc_${uuid0}.metadata_${uuid0};
CREATE TABLE trc_jdbc_${uuid0}.metadata_${uuid0}.metadata_base (
    order_id bigint NOT NULL,
    total decimal(12,2) DEFAULT 12.34,
    code varchar,
    payload row(x bigint, ys array(decimal(9,2)))
) WITH (format_version = 3);
USE trc_jdbc_${uuid0}.metadata_${uuid0};
COMMENT ON COLUMN metadata_base.order_id IS 'id';
CREATE VIEW trc_jdbc_${uuid0}.metadata_${uuid0}.metadata_view
AS SELECT order_id, total FROM trc_jdbc_${uuid0}.metadata_${uuid0}.metadata_base;

-- query 2
SELECT table_name, table_type, table_comment
FROM trc_jdbc_${uuid0}.information_schema.tables
WHERE table_schema = 'metadata_${uuid0}'
ORDER BY table_name;

-- query 3
SELECT column_name, ordinal_position, column_default, is_nullable, data_type, comment, column_comment
FROM trc_jdbc_${uuid0}.information_schema.columns
WHERE table_schema = 'metadata_${uuid0}' AND table_name = 'metadata_base'
ORDER BY ordinal_position;

-- query 4
SELECT table_name, view_definition IS NOT NULL AS has_definition
FROM trc_jdbc_${uuid0}.information_schema.views
WHERE table_schema = 'metadata_${uuid0}' AND table_name = 'metadata_view';

-- query 5
DESCRIBE metadata_base;

-- query 6
SELECT table_name, table_type, remarks
FROM system.jdbc.tables
WHERE table_cat = 'trc_jdbc_${uuid0}' AND table_schem = 'metadata_${uuid0}'
  AND table_name LIKE 'metadata\_%' ESCAPE '\' AND table_type IN ('TABLE', 'VIEW')
ORDER BY table_type, table_name;

-- query 7
SELECT column_name, data_type, type_name, column_size, buffer_length,
       decimal_digits, num_prec_radix, nullable, remarks, column_def,
       char_octet_length, ordinal_position, is_nullable
FROM system.jdbc.columns
WHERE table_cat = 'trc_jdbc_${uuid0}' AND table_schem = 'metadata_${uuid0}'
  AND table_name LIKE 'metadata\_base' ESCAPE '\'
ORDER BY ordinal_position;

-- query 8
SELECT column_name, ordinal_position
FROM system.jdbc.columns
WHERE table_cat = 'trc_jdbc_${uuid0}' AND table_schem = 'metadata_${uuid0}'
  AND table_name = 'metadata_base' AND column_name LIKE 'total' ESCAPE '\';

-- query 9
SELECT table_type FROM system.jdbc.table_types ORDER BY table_type;

-- query 10
SELECT type_name, data_type, precision
FROM system.jdbc.types
WHERE type_name IN ('array', 'bigint', 'char', 'decimal', 'real')
ORDER BY type_name;

-- query 11
SELECT count(*) AS attributes_rows FROM system.jdbc.attributes;

-- query 12
SELECT count(*) AS procedure_columns_rows FROM system.jdbc.procedure_columns;

-- query 13
SELECT count(*) AS procedures_rows FROM system.jdbc.procedures;

-- query 14
SELECT count(*) AS pseudo_columns_rows FROM system.jdbc.pseudo_columns;

-- query 15
SELECT count(*) AS super_tables_rows FROM system.jdbc.super_tables;

-- query 16
SELECT count(*) AS super_types_rows FROM system.jdbc.super_types;

-- query 17
SELECT count(*) AS udts_rows FROM system.jdbc.udts;

-- query 18
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT count(*) AS retained_types FROM system.jdbc.table_types
WHERE CAST(table_type AS BIGINT) > 0 AND table_type = '__missing__';

-- query 19
SELECT count(*) AS retained_types FROM system.jdbc.table_types
WHERE table_type = '__missing__' AND CAST(table_type AS BIGINT) > 0;

-- query 20
-- @cleanup=true
-- @skip_result_check=true
DROP VIEW IF EXISTS trc_jdbc_${uuid0}.metadata_${uuid0}.metadata_view;
DROP TABLE IF EXISTS trc_jdbc_${uuid0}.metadata_${uuid0}.metadata_base;
USE iceberg_ddl_cat_${suite_uuid0}.${case_db};
DROP SCHEMA IF EXISTS trc_jdbc_${uuid0}.metadata_${uuid0};
DROP CATALOG trc_jdbc_${uuid0};
