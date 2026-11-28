-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0.
--
-- @sequential=true
-- @tags=mv,iceberg,rest,minio,storage-contract,slot-change,concurrency

-- query 1
-- @skip_result_check=true
CREATE CATALOG mvslot_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'rest', "uri" = '${iceberg_rest_uri}', "warehouse" = '${iceberg_rest_warehouse}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.endpoint" = '${oss_endpoint}', "aws.s3.region" = '${oss_region}', "aws.s3.enable_path_style_access" = '${oss_path_style}');

-- query 2
-- @skip_result_check=true
CREATE SCHEMA mvslot_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvslot_${uuid0}.ns_${uuid0}.fact (k VARCHAR, v BIGINT) WITH (format_version = 3);

-- query 4
-- @skip_result_check=true
INSERT INTO mvslot_${uuid0}.ns_${uuid0}.fact VALUES ROW('east', 10);

-- query 5
-- @skip_result_check=true
USE mvslot_${uuid0}.ns_${uuid0};
CREATE MATERIALIZED VIEW first_slot_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;

-- query 6
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=0,slot-changes=1
ALTER MATERIALIZED VIEW first_slot_mv SET PROPERTIES refresh_paused = true;

-- query 7
-- @result_contains=SAME_VALUE_SLOT_BASELINE
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/first_slot_mv' | python3 -c 'import json,sys; m=json.load(sys.stdin)["metadata"]; c=json.loads(m["properties"]["novarocks.doc.v2.slot.novarocks.mv~configuration"]); assert c["writer"]==m["current-snapshot-id"]; json.dump(c,open(sys.argv[1],"w")); print("SAME_VALUE_SLOT_BASELINE")' "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"


-- query 8
-- Same-value PAUSE still advances main and the configuration writer.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=0,slot-changes=2
ALTER MATERIALIZED VIEW first_slot_mv SET PROPERTIES refresh_paused = true;

-- query 9
-- @result_contains=SAME_VALUE_SLOT_ADVANCED
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/first_slot_mv' | python3 -c 'import json,sys; m=json.load(sys.stdin)["metadata"]; old=json.load(open(sys.argv[1])); c=json.loads(m["properties"]["novarocks.doc.v2.slot.novarocks.mv~configuration"]); assert c["content"]==old["content"],"same-value PAUSE changed C content"; assert c["writer"]!=old["writer"] and c["writer"]==m["current-snapshot-id"],"same-value PAUSE did not advance writer"; print("SAME_VALUE_SLOT_ADVANCED")' "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"
rm -f "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"


-- query 10
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=0,slot-changes=3
ALTER MATERIALIZED VIEW first_slot_mv SET PROPERTIES refresh_paused = false;

-- query 11
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=1,slot-changes=3,full-refreshes=1
REFRESH MATERIALIZED VIEW first_slot_mv;

-- query 12
-- @skip_result_check=true
CREATE MATERIALIZED VIEW conflict_slot_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW conflict_slot_mv;

-- query 13
-- Freeze the parent, the exact P frame, and the configuration writer.
-- Prepare a native zero-file external replacement that carries P and leaves C alone.
-- @result_contains=SLOT_BEFORE_SAVED
shell: set -eu
rest="${NOVAROCKS_ICEBERG_REST_URI:?missing direct fixture endpoint}"
base="${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json"
curl --silent --show-error --fail "$rest/v1/namespaces/ns_${uuid0}/tables/conflict_slot_mv" \
  | python3 -c '
import json
import sys

meta = json.load(sys.stdin)["metadata"]
parent = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
new_id = max(s["snapshot-id"] for s in meta["snapshots"]) + 1
publication = "novarocks.doc.v2.record.novarocks.mv~publication"
config = "novarocks.doc.v2.slot.novarocks.mv~configuration"
summary = {k: v for k, v in parent["summary"].items() if k.startswith("total-")}
summary.update({"operation": "replace", publication: parent["summary"][publication]})
external = {
    "snapshot-id": new_id, "parent-snapshot-id": parent["snapshot-id"],
    "sequence-number": meta["last-sequence-number"] + 1,
    "timestamp-ms": max(meta["last-updated-ms"], parent["timestamp-ms"]) + 1,
    "manifest-list": parent["manifest-list"], "summary": summary,
    "schema-id": meta["current-schema-id"], "first-row-id": meta["next-row-id"], "added-rows": 0,
}
request = {
    "requirements": [
        {"type": "assert-table-uuid", "uuid": meta["table-uuid"]},
        {"type": "assert-ref-snapshot-id", "ref": "main", "snapshot-id": parent["snapshot-id"]},
    ],
    "updates": [
        {"action": "add-snapshot", "snapshot": external},
        {"action": "set-snapshot-ref", "ref-name": "main", "type": "branch", "snapshot-id": new_id},
    ],
}
with open(sys.argv[1], "w") as out:
    json.dump({"slot": json.loads(meta["properties"][config]), "external": external,
               "snapshots": len(meta["snapshots"])}, out)
with open(sys.argv[1] + ".request", "w") as out:
    json.dump(request, out)
print("SLOT_BEFORE_SAVED")
' "$base"

-- query 14
-- The first main condition fails. The unchanged affected slot writer lets
-- ALTER construct a new request on the true current parent.
-- @skip_result_check=true
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=base="${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json"; curl --silent --show-error --fail --request POST --header 'Content-Type: application/json' --data-binary "@$base.request" "${NOVAROCKS_ICEBERG_REST_URI:?missing direct fixture endpoint}/v1/namespaces/ns_${uuid0}/tables/conflict_slot_mv" > /dev/null
USE mvslot_${uuid0}.ns_${uuid0};
ALTER MATERIALIZED VIEW conflict_slot_mv SET PROPERTIES refresh_paused = true;

-- query 15
-- @result_contains=SLOT_CONFLICT_REBASED
shell: set -eu
rest="${NOVAROCKS_ICEBERG_REST_URI:?missing direct fixture endpoint}"
base="${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json"
trap 'rm -f "$base" "$base.request"' EXIT
curl --silent --show-error --fail "$rest/v1/namespaces/ns_${uuid0}/tables/conflict_slot_mv" \
  | python3 -c '
import json
import sys

meta = json.load(sys.stdin)["metadata"]
old = json.load(open(sys.argv[1]))
external = old["external"]
current = next(s for s in meta["snapshots"] if s["snapshot-id"] == meta["current-snapshot-id"])
key = "novarocks.doc.v2.slot.novarocks.mv~configuration"
slot = json.loads(meta["properties"][key])
assert slot["writer"] == current["snapshot-id"] != old["slot"]["writer"]
assert slot["content"] != old["slot"]["content"]
assert current["parent-snapshot-id"] == external["snapshot-id"]
assert current["summary"]["operation"] == "replace"
assert len(meta["snapshots"]) == old["snapshots"] + 2
prefix = "novarocks.doc.v2.record."
assert {k: v for k, v in current["summary"].items() if k.startswith(prefix)} == {
    k: v for k, v in external["summary"].items() if k.startswith(prefix)}
print("SLOT_CONFLICT_REBASED")
' "$base"

-- query 16
-- @skip_result_check=true
DROP MATERIALIZED VIEW conflict_slot_mv;
CREATE MATERIALIZED VIEW unknown_slot_mv WITH (refresh_policy = 'MANUAL') AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW unknown_slot_mv;

-- query 17
-- A same-value configuration write stays MANUAL and unpaused. Losing its
-- response must not freeze the subsequent refresh.
-- @publication_catalog_fault=table-commit,after-commit-before-response
-- @expect_trino_error=ICEBERG_COMMIT_ERROR,EXTERNAL
-- @expect_error=Failed to commit during alter materialized view: [CommitUnknown]
ALTER MATERIALIZED VIEW unknown_slot_mv SET PROPERTIES refresh_policy = 'MANUAL';

-- query 18
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.unknown_slot_mv,publications=2,slot-changes=1,table-commits=4,full-refreshes=1
REFRESH MATERIALIZED VIEW unknown_slot_mv;

-- query 19
-- @skip_result_check=true
-- @result_rows_where=Property=LatestSlotChangeConclusion
-- @result_rows_count=1
-- @result_rows_expect=Value=Committed
-- @result_not_contains=AWAITING_EFFECT_SETTLEMENT
CALL novarocks_mv_management_status('mvslot_${uuid0}', 'ns_${uuid0}', 'unknown_slot_mv');

-- query 20
-- @skip_result_check=true
-- @result_rows_where=Property=PendingSlotChanges
-- @result_rows_count=1
-- @result_rows_expect=Value=0
-- @result_not_contains=AWAITING_EFFECT_SETTLEMENT
CALL novarocks_mv_management_status('mvslot_${uuid0}', 'ns_${uuid0}', 'unknown_slot_mv');

-- query 21
-- @cleanup=true
-- @skip_result_check=true
USE mvslot_${uuid0}.ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS first_slot_mv;
DROP MATERIALIZED VIEW IF EXISTS conflict_slot_mv;
DROP MATERIALIZED VIEW IF EXISTS unknown_slot_mv;
DROP TABLE mvslot_${uuid0}.ns_${uuid0}.fact ;
DROP SCHEMA mvslot_${uuid0}.ns_${uuid0};

-- query 22
-- @cleanup=true
-- @skip_result_check=true
shell: rm -f "${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json" "${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json.request" "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"

-- query 23
-- Detach even if earlier object cleanup failed, before another case restarts FE.
-- @cleanup=true
-- @skip_result_check=true
DROP CATALOG mvslot_${uuid0};
