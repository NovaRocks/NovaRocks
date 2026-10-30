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
CREATE EXTERNAL CATALOG mvslot_${uuid0}
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

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvslot_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvslot_${uuid0}.ns_${uuid0}.fact (k STRING, v BIGINT)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
INSERT INTO mvslot_${uuid0}.ns_${uuid0}.fact VALUES ('east', 10);

-- query 5
-- @skip_result_check=true
SET CATALOG mvslot_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW first_slot_mv
DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k, v FROM fact;

-- query 6
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=0,slot-changes=1
ALTER MATERIALIZED VIEW first_slot_mv PAUSE REFRESH;

-- query 7
-- @result_contains=SAME_VALUE_SLOT_BASELINE
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/first_slot_mv' | python3 -c 'import json,sys; m=json.load(sys.stdin)["metadata"]; c=json.loads(m["properties"]["novarocks.doc.v2.slot.novarocks.mv~configuration"]); assert c["writer"]==m["current-snapshot-id"]; json.dump(c,open(sys.argv[1],"w")); print("SAME_VALUE_SLOT_BASELINE")' "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"


-- query 8
-- Same-value PAUSE still advances main and the configuration writer.
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=0,slot-changes=2
ALTER MATERIALIZED VIEW first_slot_mv PAUSE REFRESH;

-- query 9
-- @result_contains=SAME_VALUE_SLOT_ADVANCED
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/first_slot_mv' | python3 -c 'import json,sys; m=json.load(sys.stdin)["metadata"]; old=json.load(open(sys.argv[1])); c=json.loads(m["properties"]["novarocks.doc.v2.slot.novarocks.mv~configuration"]); assert c["content"]==old["content"],"same-value PAUSE changed C content"; assert c["writer"]!=old["writer"] and c["writer"]==m["current-snapshot-id"],"same-value PAUSE did not advance writer"; print("SAME_VALUE_SLOT_ADVANCED")' "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"
rm -f "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"


-- query 10
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=0,slot-changes=3
ALTER MATERIALIZED VIEW first_slot_mv RESUME REFRESH;

-- query 11
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.first_slot_mv,publications=1,slot-changes=3
REFRESH MATERIALIZED VIEW first_slot_mv WITH SYNC MODE;

-- query 12
-- @skip_result_check=true
CREATE MATERIALIZED VIEW conflict_slot_mv
DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW conflict_slot_mv WITH SYNC MODE;

-- query 13
-- Freeze the exact prior C bytes before holding ALTER. The concurrent Spark
-- write then advances main, so this immutable slot request must be rejected.
-- @result_contains=SLOT_BEFORE_SAVED
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/conflict_slot_mv' | python3 -c 'import json,sys; m=json.load(sys.stdin)["metadata"]; p=m["properties"]; key="novarocks.doc.v2.slot.novarocks.mv~configuration"; json.dump({"slot":p[key],"main":m["current-snapshot-id"]},open(sys.argv[1],"w")); print("SLOT_BEFORE_SAVED")' "${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json"

-- query 14
-- @publication_catalog_fault=table-commit,before-requirement-check-hold-for-concurrent-shell
-- @publication_catalog_concurrent_shell=tmp_sql=$(mktemp "${TMPDIR:-/tmp}/novarocks-slot-conflict-XXXXXX.sql"); trap 'rm -f "$tmp_sql"' EXIT; printf '%s\n' "INSERT INTO ice_rest.ns_${uuid0}.conflict_slot_mv SELECT * FROM ice_rest.ns_${uuid0}.conflict_slot_mv LIMIT 1;" > "$tmp_sql"; "${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
-- @expect_error=disposition=KnownUncommitted
SET CATALOG mvslot_${uuid0};
USE ns_${uuid0};
ALTER MATERIALIZED VIEW conflict_slot_mv PAUSE REFRESH;

-- query 15
-- @result_contains=SLOT_CONFLICT_UNCHANGED
shell: set -eu
curl --silent --show-error --fail '${iceberg_rest_uri}/v1/namespaces/ns_${uuid0}/tables/conflict_slot_mv' | python3 -c 'import json,sys; m=json.load(sys.stdin)["metadata"]; old=json.load(open(sys.argv[1])); key="novarocks.doc.v2.slot.novarocks.mv~configuration"; assert m["properties"][key]==old["slot"],"rejected ALTER changed C bytes/writer"; assert m["current-snapshot-id"]!=old["main"],"external writer did not advance main"; assert len(m["snapshots"])==2,"ALTER rebased or retried"; print("SLOT_CONFLICT_UNCHANGED")' "${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json"
rm -f "${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json"

-- query 16
-- @skip_result_check=true
DROP MATERIALIZED VIEW conflict_slot_mv;
CREATE MATERIALIZED VIEW unknown_slot_mv
DISTRIBUTED BY HASH(k) BUCKETS 1 REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg') AS SELECT k, v FROM fact;
REFRESH MATERIALIZED VIEW unknown_slot_mv WITH SYNC MODE;

-- query 17
-- A same-value configuration write stays MANUAL and unpaused. Losing its
-- response must not freeze the subsequent refresh.
-- @publication_catalog_fault=table-commit,after-commit-before-response
-- @expect_error=unknown
ALTER MATERIALIZED VIEW unknown_slot_mv SET REFRESH DEFERRED MANUAL;

-- query 18
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.unknown_slot_mv,publications=2,slot-changes=1,table-commits=4
REFRESH MATERIALIZED VIEW unknown_slot_mv WITH SYNC MODE;

-- query 19
-- @skip_result_check=true
-- @result_rows_where=Property=LatestSlotChangeConclusion
-- @result_rows_count=1
-- @result_rows_expect=Value=Committed
CALL novarocks_mv_management_status('mvslot_${uuid0}', 'ns_${uuid0}', 'unknown_slot_mv');

-- query 20
-- @skip_result_check=true
-- @result_rows_where=Property=PendingSlotChanges
-- @result_rows_count=1
-- @result_rows_expect=Value=0
CALL novarocks_mv_management_status('mvslot_${uuid0}', 'ns_${uuid0}', 'unknown_slot_mv');

-- query 21
-- @cleanup=true
-- @skip_result_check=true
SET CATALOG mvslot_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS first_slot_mv;
DROP MATERIALIZED VIEW IF EXISTS conflict_slot_mv;
DROP MATERIALIZED VIEW IF EXISTS unknown_slot_mv;
DROP TABLE mvslot_${uuid0}.ns_${uuid0}.fact FORCE;
DROP DATABASE mvslot_${uuid0}.ns_${uuid0};

-- query 22
-- @cleanup=true
-- @skip_result_check=true
shell: rm -f "${TMPDIR:-/tmp}/novarocks-slot-before-${uuid0}.json" "${TMPDIR:-/tmp}/novarocks-slot-same-${uuid0}.json"

-- query 23
-- Detach even if earlier object cleanup failed, before another case restarts FE.
-- @cleanup=true
-- @skip_result_check=true
DROP CATALOG mvslot_${uuid0};
