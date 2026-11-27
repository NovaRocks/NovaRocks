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
-- @tags=mv,iceberg,storage_contract,metadata_only,publication
-- A filtered-out base insert advances the MV watermark through a new P
-- without changing target data files. The runner checks the exact REST
-- document graph and counts every target mutation between publications.

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvmeta_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvmeta_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvmeta_${uuid0}.ns_${uuid0}.fact (k VARCHAR, v BIGINT, payload VARCHAR) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
-- Keep unchanged filtered rows in files outside the small refresh window.
INSERT INTO mvmeta_${uuid0}.ns_${uuid0}.fact
SELECT 'excluded', -id, to_hex(sha256(to_utf8(CAST(id AS VARCHAR))))
FROM TABLE(sequence(1, 4096)) AS cold(id);
INSERT INTO mvmeta_${uuid0}.ns_${uuid0}.fact (k, v) VALUES ROW('east', 10);

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_filtered,publications=0,slot-changes=0
USE mvmeta_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW mv_filtered WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact WHERE v > 0;

-- query 6
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_filtered,publications=1,slot-changes=0,full-refreshes=1
REFRESH MATERIALIZED VIEW mv_filtered;

-- query 7
-- @skip_result_check=true
INSERT INTO mvmeta_${uuid0}.ns_${uuid0}.fact (k, v) VALUES ROW('west', -1);

-- query 8
-- @result_contains=MV refresh: ContentLocate
EXPLAIN REFRESH MATERIALIZED VIEW mv_filtered;

-- query 9
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.mv_filtered,publications=2,slot-changes=0,metadata-only-last=true,full-refreshes=1
REFRESH MATERIALIZED VIEW mv_filtered;

-- query 10
-- @skip_result_check=true
-- @result_contains=east
-- @result_contains=10
-- @result_not_contains=west
SELECT k, v FROM mv_filtered ORDER BY k, v;

-- query 11
-- @cleanup=true
-- @skip_result_check=true
USE mvmeta_${uuid0}.ns_${uuid0};
DROP MATERIALIZED VIEW mv_filtered;
DROP TABLE mvmeta_${uuid0}.ns_${uuid0}.fact ;
DROP SCHEMA mvmeta_${uuid0}.ns_${uuid0};
DROP CATALOG mvmeta_${uuid0};
