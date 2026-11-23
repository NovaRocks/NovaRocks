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

-- @order_sensitive=true
-- @tags=iceberg,ctas,write,location,statistics

-- query 1
-- @skip_result_check=true
CREATE CATALOG "no_wh_${uuid0}" USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA no_wh_${uuid0}.ctas_contract_${uuid0}
WITH (location = '${iceberg_rest_warehouse}/ctas-contract-${uuid0}');
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows (id INT, v INT);
INSERT INTO no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows VALUES (1, 10), (2, 20), (3, 10);

-- query 2
-- @skip_result_check=true
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.default_format
AS SELECT id, v FROM no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows;

-- query 3
-- @skip_result_check=true
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.empty_result
AS SELECT id, v FROM no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows WHERE FALSE;

-- query 4
-- @skip_result_check=true
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.version_one WITH (format_version = 1)
AS SELECT id FROM no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows;
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.version_two WITH (format_version = 2)
AS SELECT id FROM no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows;
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.version_three WITH (format_version = 3)
AS SELECT id FROM no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows;

-- query 5
-- @skip_result_check=true
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.explicit_location
WITH (location = '${iceberg_rest_warehouse}/ctas-contract-${uuid0}/fixed-location')
AS SELECT id FROM no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows;

-- query 6
-- @result_contains=RWM17A_CREATE_PREFIX_CAPTURED
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_ctas_contracts.py" capture '${uuid0}' '${iceberg_rest_warehouse}'

-- query 7
-- @expect_trino_error=ICEBERG_FILESYSTEM_ERROR,EXTERNAL
-- @expect_error=Cannot create a table on a non-empty location:
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.rejected_ctas
WITH (location = '${iceberg_rest_warehouse}/ctas-contract-${uuid0}/fixed-location')
AS SELECT id FROM no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows;

-- query 8
-- @expect_trino_error=ICEBERG_FILESYSTEM_ERROR,EXTERNAL
-- @expect_error=Cannot create a table on a non-empty location:
CREATE TABLE no_wh_${uuid0}.ctas_contract_${uuid0}.rejected_create (id INT)
WITH (location = '${iceberg_rest_warehouse}/ctas-contract-${uuid0}/fixed-location');

-- query 9
SELECT id, v FROM no_wh_${uuid0}.ctas_contract_${uuid0}.default_format ORDER BY id;

-- query 10
SHOW STATS FOR no_wh_${uuid0}.ctas_contract_${uuid0}.default_format;

-- query 11
-- @result_contains=RWM17A_CREATE_CONTRACTS_OK
shell: python3 "${NOVAROCKS_WORKSPACE_ROOT:-.}/tests/sql/correctness/lake-publication/verify_ctas_contracts.py" verify '${uuid0}' '${iceberg_rest_warehouse}'

-- query 12
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.rejected_create;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.rejected_ctas;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.version_three;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.version_two;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.version_one;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.explicit_location;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.empty_result;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.default_format;
DROP TABLE IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0}.source_rows;
DROP SCHEMA IF EXISTS no_wh_${uuid0}.ctas_contract_${uuid0};
DROP CATALOG IF EXISTS no_wh_${uuid0};
