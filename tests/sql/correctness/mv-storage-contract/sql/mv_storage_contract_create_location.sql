-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract
-- Managed MV CREATE publishes metadata at the final explicit or generated
-- location. It owns no snapshot, staged data, or private CTAS metadata.

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvcreate_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvcreate_${uuid0}.ns_${uuid0} WITH (location = '${iceberg_rest_warehouse}/a18-default-${uuid0}');

-- query 3
-- @skip_result_check=true
CREATE TABLE mvcreate_${uuid0}.ns_${uuid0}.fact (k VARCHAR, v BIGINT) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.explicit_mv,publications=0,slot-changes=0,table-commits=1
USE mvcreate_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW explicit_mv WITH (location = '${iceberg_rest_warehouse}/a18-explicit-${uuid0}') AS SELECT k, v FROM fact;

-- query 5
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.default_mv,publications=0,slot-changes=0,table-commits=1
CREATE MATERIALIZED VIEW default_mv AS SELECT k, v FROM fact;

-- query 6
-- @result_contains=MV_CREATE_FINAL_LOCATIONS_OK
shell: set -eu
rest='${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables'
warehouse='${iceberg_rest_warehouse}'
explicit="$(curl --silent --show-error --fail "$rest/explicit_mv")"
generated="$(curl --silent --show-error --fail "$rest/default_mv")"
python3 - "$warehouse" '${uuid0}' "$explicit" "$generated" <<'PYTHON'
import json
import re
import sys

warehouse, token = sys.argv[1:3]
explicit, generated = map(json.loads, sys.argv[3:5])
assert explicit["metadata"]["location"] == f"{warehouse}/a18-explicit-{token}"
generated_prefix = f"{warehouse}/a18-default-{token}/default_mv-"
assert re.fullmatch(re.escape(generated_prefix) + r"[0-9a-f]{32}", generated["metadata"]["location"])
for result in explicit, generated:
    metadata = result["metadata"]
    assert result["metadata-location"].startswith(metadata["location"] + "/metadata/")
    assert not metadata["snapshots"]
    assert metadata.get("current-snapshot-id") in (None, -1)
    assert "main" not in metadata.get("refs", {})
print("MV_CREATE_FINAL_LOCATIONS_OK")
PYTHON

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.explicit_mv;
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.default_mv;
DROP TABLE IF EXISTS mvcreate_${uuid0}.ns_${uuid0}.fact;
DROP SCHEMA IF EXISTS mvcreate_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS mvcreate_${uuid0};
