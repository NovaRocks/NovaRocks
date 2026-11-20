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
-- @tags=mv,iceberg,ivm,w6,single_table,guard
-- Test Objective:
-- 1. Validate an Iceberg-backed single-table MV is queryable after refresh.
-- 2. Validate direct table mutations and Iceberg maintenance commands are rejected for MV targets.
-- 3. Validate namespace/catalog drops are blocked while an MV exists and cleanup succeeds after DROP MATERIALIZED VIEW.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_w6_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_w6_${uuid0}.ns_${uuid0};
CREATE TABLE ice_w6_${uuid0}.ns_${uuid0}.base_orders (
  order_id INT,
  amount BIGINT
) WITH (format_version = 3);
INSERT INTO ice_w6_${uuid0}.ns_${uuid0}.base_orders VALUES
  ROW(1, 10),
  ROW(2, 20);

USE ice_w6_${uuid0}.ns_${uuid0};

CREATE MATERIALIZED VIEW target_mv AS SELECT order_id, amount FROM base_orders;

REFRESH MATERIALIZED VIEW target_mv;

-- query 2
SELECT order_id, amount FROM target_mv ORDER BY order_id;

-- query 3
-- @expect_error=materialized view
INSERT INTO target_mv VALUES ROW(3, 30, 3);

-- query 4
-- @expect_error=materialized view
UPDATE target_mv SET amount = 100 WHERE order_id = 1;

-- query 5
-- @expect_error=materialized view
DELETE FROM target_mv WHERE order_id = 1;

-- query 6
-- @expect_error=materialized view
MERGE INTO target_mv AS t
USING (SELECT 1 AS order_id, 100 AS amount) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET amount = s.amount;

-- query 7
-- @expect_error=materialized view
TRUNCATE TABLE target_mv;

-- query 8
-- @expect_error=materialized view
ALTER TABLE target_mv ADD COLUMN extra INT;

-- query 9
-- @expect_error=materialized view
ALTER TABLE target_mv SET PROPERTIES extra_properties = MAP(ARRAY['comment'], ARRAY['blocked']);

-- query 10
-- @expect_error=materialized view
ALTER TABLE target_mv EXECUTE add_files(location => 's3://blocked/path');

-- query 11
-- @expect_error=materialized view
ALTER TABLE target_mv EXECUTE add_equality_delete(columns => ARRAY['order_id'], rows => ARRAY[ROW(1)]);

-- query 12
-- @expect_error=materialized view
CALL ice_w6_${uuid0}.system.rewrite_manifests("table" => 'ns_${uuid0}.target_mv');

-- query 13
-- @expect_error=materialized view
ALTER TABLE target_mv EXECUTE remove_orphan_files(retention_threshold => '7d');

-- query 14
-- @expect_error=materialized view
CALL ice_w6_${uuid0}.system.remove_orphan_files(
  "table" => 'ns_${uuid0}.target_mv',
  older_than => TIMESTAMP '2020-01-01 00:00:00');

-- query 15
-- @expect_error=materialized view
DROP TABLE target_mv ;

-- query 16
-- @expect_error=materialized view
CREATE BRANCH blocked_branch IN TABLE ice_w6_${uuid0}.ns_${uuid0}.target_mv;

-- query 17
-- @expect_error=materialized view
DROP SCHEMA ice_w6_${uuid0}.ns_${uuid0};

-- query 18
-- @expect_error=materialized view
DROP CATALOG ice_w6_${uuid0};

-- query 19
SELECT order_id, amount FROM target_mv ORDER BY order_id;

-- query 20
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW target_mv;
DROP TABLE ice_w6_${uuid0}.ns_${uuid0}.base_orders ;
DROP SCHEMA ice_w6_${uuid0}.ns_${uuid0};
DROP CATALOG ice_w6_${uuid0};
