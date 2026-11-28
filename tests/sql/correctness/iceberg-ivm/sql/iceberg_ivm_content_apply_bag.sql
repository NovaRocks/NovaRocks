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
-- @tags=iceberg,ivm,content_apply,visible_bag

-- query 1
-- @skip_result_check=true
CREATE CATALOG ivm_visible_bag_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ivm_visible_bag_${uuid0}.ns_${uuid0};
USE ivm_visible_bag_${uuid0}.ns_${uuid0};
SET SESSION enable_materialized_view_rewrite = false;
CREATE TABLE left_input (
  id BIGINT NOT NULL, label VARCHAR, measurement DOUBLE, amount DECIMAL(8,2),
  observed_at TIMESTAMP(6) WITH TIME ZONE, details ROW(code BIGINT, note VARCHAR),
  attrs MAP(VARCHAR, BIGINT)
) WITH (format_version = 3);
CREATE TABLE right_input (
  id BIGINT NOT NULL, label VARCHAR, measurement DOUBLE, amount DECIMAL(8,2),
  observed_at TIMESTAMP(6) WITH TIME ZONE, details ROW(code BIGINT, note VARCHAR),
  attrs MAP(VARCHAR, BIGINT)
) WITH (format_version = 3);
INSERT INTO left_input VALUES
  ROW(1, 'zero', CAST('-0.0' AS DOUBLE), DECIMAL '12.30',
      TIMESTAMP '2024-01-02 03:04:05.123456 UTC',
      CAST(ROW(4, NULL) AS ROW(code BIGINT, note VARCHAR)),
      MAP(ARRAY['a', 'b'], ARRAY[11, NULL])),
  ROW(2, 'nan', CAST('NaN' AS DOUBLE), NULL, NULL, NULL, NULL);
INSERT INTO right_input VALUES
  ROW(1, 'nan', CAST('NaN' AS DOUBLE), NULL, NULL, NULL, NULL),
  ROW(2, 'kept', 7.0, NULL, NULL, NULL, NULL),
  ROW(4, NULL, NULL, NULL, NULL, NULL, NULL);
CREATE MATERIALIZED VIEW visible_bag AS
SELECT label, measurement, amount, observed_at, details, attrs FROM left_input
UNION ALL
SELECT label, measurement, amount, observed_at, details, attrs FROM right_input;
REFRESH MATERIALIZED VIEW visible_bag;

-- query 2
-- @imv_equivalence_check=visible_bag
SELECT label, count(*) AS multiplicity FROM visible_bag
GROUP BY label ORDER BY label NULLS FIRST;

-- query 3
-- @skip_result_check=true
-- The zero tuple moves between branches, changing only representations.
-- A separate NaN occurrence retracts while the other occurrence remains.
-- Both retracted rows are in the left branch, so the source uses a metadata delete.
DELETE FROM left_input;
INSERT INTO right_input VALUES
  ROW(3, 'zero', CAST('0.0' AS DOUBLE), DECIMAL '12.30',
      TIMESTAMP '2024-01-02 03:04:05.123456 UTC',
      CAST(ROW(4, NULL) AS ROW(code BIGINT, note VARCHAR)),
      MAP(ARRAY['b', 'a'], ARRAY[NULL, 11]));
REFRESH MATERIALIZED VIEW visible_bag;

-- query 4
-- Use a scalar projection of every visible field. This gives the two legal
-- zero spellings and either physical MAP entry order the same representation.
WITH all_rows AS (
  SELECT 'mv' AS origin, label, measurement, amount, observed_at, details, attrs
  FROM visible_bag
  UNION ALL
  SELECT 'full', label, measurement, amount, observed_at, details, attrs FROM left_input
  UNION ALL
  SELECT 'full', label, measurement, amount, observed_at, details, attrs FROM right_input
)
SELECT origin, label, count(*) AS multiplicity,
       count_if(measurement = 0.0 AND amount = DECIMAL '12.30'
         AND observed_at = TIMESTAMP '2024-01-02 03:04:05.123456 UTC'
         AND details.code = 4 AND details.note IS NULL
         AND cardinality(attrs) = 2 AND element_at(attrs, 'a') = 11
         AND contains(map_keys(attrs), 'b') AND element_at(attrs, 'b') IS NULL) AS zero_tuple,
       count_if(CAST(measurement AS VARCHAR) = 'NaN' AND amount IS NULL AND observed_at IS NULL
         AND details IS NULL AND attrs IS NULL) AS nan_tuple,
       count_if(measurement = 7.0 AND amount IS NULL AND observed_at IS NULL
         AND details IS NULL AND attrs IS NULL) AS retained_tuple,
       count_if(measurement IS NULL AND amount IS NULL AND observed_at IS NULL
         AND details IS NULL AND attrs IS NULL) AS null_tuple
FROM all_rows GROUP BY origin, label ORDER BY origin, label NULLS FIRST;

-- query 5
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS visible_bag;
DROP TABLE IF EXISTS right_input;
DROP TABLE IF EXISTS left_input;
DROP SCHEMA IF EXISTS ivm_visible_bag_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ivm_visible_bag_${uuid0};
