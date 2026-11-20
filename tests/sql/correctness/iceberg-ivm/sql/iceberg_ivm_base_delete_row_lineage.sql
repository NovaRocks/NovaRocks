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
-- @tags=mv,iceberg,ivm,row_lineage,target_apply,base_delete,projection,filter,equality_delete
-- Test Objective:
-- 1. Validate Iceberg-backed projection/filter MV incremental refresh uses the
--    base Iceberg v3 _row_id as the hidden target apply key.
-- 2. Validate base DELETE, UPDATE, predicate in/out, and equality-delete
--    changes are applied to the Iceberg MV target without a PRIMARY KEY.
-- 3. Validate the hidden apply-key column is not exposed to users.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_row_lineage_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_row_lineage_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0} (
  id INT NOT NULL,
  amount BIGINT,
  customer STRING
) WITH (format_version = 3);
INSERT INTO ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0} VALUES
  ROW(1, 100, 'Alice'),
  ROW(2, 40, 'Bob'),
  ROW(3, 70, 'Carol');
USE ice_ivm_row_lineage_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv_${uuid0} AS SELECT id, amount
FROM ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0}
WHERE amount >= 50;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 3
SELECT * FROM orders_mv_${uuid0} ORDER BY id;

-- query 4
-- @expect_error=__nova_base_row_id
SELECT __nova_base_row_id FROM orders_mv_${uuid0};

-- query 5
-- @skip_result_check=true
INSERT INTO orders_${uuid0} VALUES ROW(4, 60, 'Dave');
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 6
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 7
-- @skip_result_check=true
DELETE FROM orders_${uuid0} WHERE id = 1;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 8
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 9
-- @skip_result_check=true
UPDATE orders_${uuid0} SET amount = 90 WHERE id = 3;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 10
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 11
-- @skip_result_check=true
UPDATE orders_${uuid0} SET amount = 30 WHERE id = 4;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 12
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 13
-- @skip_result_check=true
UPDATE orders_${uuid0} SET amount = 55 WHERE id = 2;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 14
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 15
-- @skip_result_check=true
ALTER TABLE orders_${uuid0} EXECUTE add_equality_delete(columns => ARRAY['id'], rows => ARRAY[ROW(3)]);
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 16
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 17
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv_${uuid0};
DROP TABLE ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0} ;
DROP SCHEMA ice_ivm_row_lineage_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_row_lineage_${uuid0};
