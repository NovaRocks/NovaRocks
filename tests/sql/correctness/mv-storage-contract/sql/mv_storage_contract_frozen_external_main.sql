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
-- @tags=mv,iceberg,rest,minio,storage-contract,concurrency
-- The external REST request freezes its original nonempty main condition
-- before the MV publishes again. Sending those same bytes after the MV commit
-- must not overwrite or rebase the exact MV output.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvfrozen_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE mvfrozen_${uuid0}.ns_${uuid0};
CREATE TABLE mvfrozen_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO mvfrozen_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10);
SET CATALOG mvfrozen_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW target_mv
DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW target_mv WITH SYNC MODE;

-- query 2
-- @skip_result_check=true
INSERT INTO fact VALUES ('west', 20);

-- query 3
-- @result_contains=MV_EXTERNAL_REQUEST_FROZEN
shell: set -eu
rest_uri='${iceberg_rest_uri}'
request_file="${TMPDIR:-/tmp}/novarocks-mv-frozen-main-${uuid0}.json"
curl --silent --show-error --fail \
  "$rest_uri/v1/namespaces/ns_${uuid0}/tables/target_mv" \
  | python3 -c '
import json
import sys

metadata = json.load(sys.stdin)["metadata"]
snapshot = metadata["current-snapshot-id"]
if not isinstance(snapshot, int) or snapshot <= 0:
    sys.exit(f"expected a published main snapshot, got {snapshot!r}")
request = {
    "requirements": [{"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": snapshot}],
    "updates": [{"action": "set-properties", "updates": {"uea7.frozen-external-main": "stale"}}],
}
with open(sys.argv[1], "w", encoding="utf-8") as output:
    json.dump(request, output)
print("MV_EXTERNAL_REQUEST_FROZEN", snapshot)
' \
      "$request_file"

-- query 4
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.target_mv,publications=2,slot-changes=0
REFRESH MATERIALIZED VIEW target_mv FULL WITH SYNC MODE;

-- query 5
-- @result_contains=MV_FROZEN_EXTERNAL_REJECTED
shell: set -eu
rest_uri='${iceberg_rest_uri}'
request_file="${TMPDIR:-/tmp}/novarocks-mv-frozen-main-${uuid0}.json"
response_file="${TMPDIR:-/tmp}/novarocks-mv-frozen-main-${uuid0}.response.json"
status="$(curl --silent --show-error --output "$response_file" --write-out '%{http_code}' \
  --request POST --header 'Content-Type: application/json' \
  --data-binary "@$request_file" \
  "$rest_uri/v1/namespaces/ns_${uuid0}/tables/target_mv")"
test "$status" = 409
curl --silent --show-error --fail \
  "$rest_uri/v1/namespaces/ns_${uuid0}/tables/target_mv" \
  | python3 -c '
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    request = json.load(source)
with open(sys.argv[2], encoding="utf-8") as source:
    response = json.load(source)
metadata = json.load(sys.stdin)["metadata"]
expected = request["requirements"][0]["snapshot-id"]
actual = metadata["current-snapshot-id"]
message = response["error"]["message"]
if actual == expected or f"expected id {expected} != {actual}" not in message:
    sys.exit(f"frozen main condition was not rejected: expected={expected}, actual={actual}, error={message}")
if "uea7.frozen-external-main" in metadata["properties"]:
    sys.exit("stale external mutation changed target properties")
if len(metadata["snapshots"]) != 2:
    sys.exit("stale external mutation changed target snapshot count")
print("MV_FROZEN_EXTERNAL_REJECTED", expected, actual)
' \
      "$request_file" "$response_file"
rm -f "$request_file" "$response_file"

-- query 6
SELECT k, v FROM target_mv ORDER BY k;

-- query 7
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW target_mv;
DROP TABLE mvfrozen_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE mvfrozen_${uuid0}.ns_${uuid0};
DROP CATALOG mvfrozen_${uuid0};

-- query 8
-- @cleanup=true
-- @skip_result_check=true
shell: rm -f "${TMPDIR:-/tmp}/novarocks-mv-frozen-main-${uuid0}.json" "${TMPDIR:-/tmp}/novarocks-mv-frozen-main-${uuid0}.response.json"
