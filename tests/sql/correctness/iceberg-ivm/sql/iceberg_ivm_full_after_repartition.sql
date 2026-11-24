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
-- @tags=mv,iceberg,ivm,projection_filter,partitioned,repartition
-- Test Point: An explicit FULL after MV repartition uses the installed
-- default spec, preserves the old chain, and appends new row lineage.

-- query 1
-- @skip_result_check=true
CREATE CATALOG ice_ivm_after_repart_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA ice_ivm_after_repart_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_after_repart_${uuid0}.ns_${uuid0}.orders (
  id BIGINT NOT NULL,
  region VARCHAR,
  amount BIGINT,
  category VARCHAR
) WITH (format_version = 3);
USE ice_ivm_after_repart_${uuid0}.ns_${uuid0};

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_after_repart_${uuid0}.ns_${uuid0}.orders VALUES
  ROW(1, 'east', 10, 'books'),
  ROW(2, 'west', 20, 'games'),
  ROW(3, 'east', 30, 'books'),
  ROW(4, 'north', 5, 'games');
CREATE MATERIALIZED VIEW pf_repart_mv_${uuid0} WITH (partitioning = ARRAY['bucket(id, 4)']) AS SELECT id, region, amount, category
FROM ice_ivm_after_repart_${uuid0}.ns_${uuid0}.orders
WHERE amount >= 10;
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};

-- query 3
-- @skip_result_check=true
ALTER MATERIALIZED VIEW pf_repart_mv_${uuid0} SET PROPERTIES partitioning = ARRAY['truncate(region, 2)'];

-- query 4
-- Capture the installed repartition before forcing the next full refresh.
-- @result_contains=MV_REPARTITION_BASE_READY
shell: set -eu
target='${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/pf_repart_mv_${uuid0}'
before="${TMPDIR:-/tmp}/rwm17a-repartition-${uuid0}.json"
curl --silent --show-error --fail "$target" > "$before"
python3 - "$before" <<'PYTHON'
import json
import sys

with open(sys.argv[1]) as source:
    metadata = json.load(source)["metadata"]
specs = {spec["spec-id"]: spec for spec in metadata["partition-specs"]}
default = specs[metadata["default-spec-id"]]
assert any(field["transform"] == "truncate[2]" for field in default["fields"]), default
assert any(
    spec_id != metadata["default-spec-id"]
    and any(field["transform"] == "bucket[4]" for field in spec["fields"])
    for spec_id, spec in specs.items()
), specs
assert metadata["current-snapshot-id"] == metadata["refs"]["main"]["snapshot-id"]
print("MV_REPARTITION_BASE_READY")
PYTHON

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_after_repart_${uuid0}.ns_${uuid0}.orders VALUES ROW(9, 'east', 90, 'books');
SET SESSION "ice_ivm_after_repart_${uuid0}".incremental_refresh_enabled = false;
REFRESH MATERIALIZED VIEW pf_repart_mv_${uuid0};
RESET SESSION "ice_ivm_after_repart_${uuid0}".incremental_refresh_enabled;

-- query 6
SELECT id, region, amount, category
FROM pf_repart_mv_${uuid0}
ORDER BY id;

-- query 7
-- @result_contains=MV_FULL_AFTER_REPARTITION_OK
shell: set -eu
target='${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/pf_repart_mv_${uuid0}'
before="${TMPDIR:-/tmp}/rwm17a-repartition-${uuid0}.json"
after="$(mktemp "${TMPDIR:-/tmp}/rwm17a-repartition-after-XXXXXX.json")"
trap 'rm -f "$after"' EXIT
curl --silent --show-error --fail "$target" > "$after"
python3 - "$before" "$after" <<'PYTHON'
import json
import sys

with open(sys.argv[1]) as source:
    before = json.load(source)["metadata"]
with open(sys.argv[2]) as source:
    after = json.load(source)["metadata"]
assert after["default-spec-id"] == before["default-spec-id"]
assert after["partition-specs"] == before["partition-specs"]
old = {s["snapshot-id"]: s for s in before["snapshots"]}
new = {s["snapshot-id"]: s for s in after["snapshots"]}
assert all(new.get(snapshot_id) == item for snapshot_id, item in old.items())
assert len(new) == len(old) + 2
append = new[after["current-snapshot-id"]]
delete = new[append["parent-snapshot-id"]]
assert delete["parent-snapshot-id"] == before["current-snapshot-id"]
assert after["refs"]["main"]["snapshot-id"] == append["snapshot-id"]
assert append["sequence-number"] == delete["sequence-number"] + 1
assert delete["summary"]["operation"] == "delete"
assert append["summary"]["operation"] == "append"
assert all(
    delete["summary"][key] == "0"
    for key in ("total-records", "total-data-files", "total-delete-files")
)
assert not any(key.startswith("novarocks.") for key in delete["summary"])
assert append["summary"]["total-records"] == "4"
assert append["summary"]["novarocks.doc.v2.record.novarocks.mv~publication"]
marker_key = "novarocks.write.session.v1"
marker = append["summary"][marker_key]
assert marker and sum(s["summary"].get(marker_key) == marker for s in new.values()) == 1
assert append["first-row-id"] == before["next-row-id"]
assert append["added-rows"] == 4
assert after["next-row-id"] == before["next-row-id"] + 4
print("MV_FULL_AFTER_REPARTITION_OK")
PYTHON

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ns_${uuid0}.pf_repart_mv_${uuid0};
DROP TABLE IF EXISTS ice_ivm_after_repart_${uuid0}.ns_${uuid0}.orders;
DROP SCHEMA IF EXISTS ice_ivm_after_repart_${uuid0}.ns_${uuid0};
DROP CATALOG IF EXISTS ice_ivm_after_repart_${uuid0};

-- query 9
-- @cleanup=true
-- @skip_result_check=true
shell: rm -f "${TMPDIR:-/tmp}/rwm17a-repartition-${uuid0}.json"
