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
-- @tags=optimizer,mv,rewrite,iceberg
-- A selective MV may remove only rows already excluded by the query prefix.
-- Disable Scan pushdown to exercise MV authorization and compensation directly.

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvrw_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvrw_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
USE mvrw_${uuid0}.ns_${uuid0};

-- query 4
-- @skip_result_check=true
CREATE TABLE ordered_predicates (id BIGINT NOT NULL, x BIGINT, s VARCHAR)
WITH (format_version = 3);

-- query 5
-- @skip_result_check=true
INSERT INTO ordered_predicates
SELECT number,
       CASE WHEN number % 100 = 99 THEN BIGINT '1' ELSE BIGINT '0' END,
       CASE WHEN number % 100 = 99 THEN '7' ELSE 'bad' END
FROM TABLE(sequence(1, 12000)) t(number);

-- query 6
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ordered_predicates_mv AS
SELECT id, x, s FROM ordered_predicates WHERE x > 0;

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW ordered_predicates_mv;

-- query 8
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = 'PushDownPredicateScan';

-- query 9
-- @skip_result_check=true
SET SESSION enable_materialized_view_rewrite = true;

-- query 10
-- @skip_result_check=true
-- @result_not_contains=rewritten with mv:
EXPLAIN SELECT count(*) AS selected_rows
FROM ordered_predicates WHERE CAST(s AS BIGINT) > 0 AND x > 0;

-- query 11
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT count(*) AS selected_rows
FROM ordered_predicates WHERE CAST(s AS BIGINT) > 0 AND x > 0;

-- query 12
-- @explain_contains=rewritten with mv: ordered_predicates_mv
SELECT count(*) AS selected_rows
FROM ordered_predicates WHERE x > 0 AND CAST(s AS BIGINT) > 0;

-- query 13
-- @skip_result_check=true
SET SESSION enable_materialized_view_rewrite = false;

-- query 14
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT count(*) AS selected_rows
FROM ordered_predicates WHERE CAST(s AS BIGINT) > 0 AND x > 0;

-- query 15
-- @explain_not_contains=rewritten with mv:
SELECT count(*) AS selected_rows
FROM ordered_predicates WHERE x > 0 AND CAST(s AS BIGINT) > 0;

-- query 16
-- @skip_result_check=true
SET SESSION enable_materialized_view_rewrite = true;

-- query 17
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = '';

-- query 18
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ordered_predicates_mv;

-- query 19
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS mvrw_${uuid0}.ns_${uuid0}.ordered_predicates;

-- query 20
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS mvrw_${uuid0}.ns_${uuid0};

-- query 21
-- @cleanup=true
-- @skip_result_check=true
DROP CATALOG IF EXISTS mvrw_${uuid0};
