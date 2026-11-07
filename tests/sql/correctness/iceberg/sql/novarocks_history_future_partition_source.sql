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
-- @tags=iceberg,schema_history,partition_evolution,write_authority
-- Official SDK commits write snapshot S0 under schema0 (`id BIGINT` only) and
-- tag it; afterwards `future INT` is added and identity(future) becomes the
-- default partition spec. Both evolution steps are metadata-only, so S0 stays
-- the only and current snapshot. Historical reads of S0 return schema0 and the
-- complete S0 bag instead of resolving the current default spec against
-- schema0; Current UPDATE/MERGE retain their existing supported partition-moving
-- behavior and commit using the current schema and default spec. The SDK
-- independently checks current values, partition placement and retained S0.
-- S0's snapshot id is assigned by the SDK at run time and the runner has no
-- variable capture, so the numeric selector is reached through
-- FOR SYSTEM_TIME AS OF: S0 is the only snapshot-log entry, which resolves to
-- the same SnapshotId(S0) selector the tag and a literal id produce.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG futuresource_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);
CREATE DATABASE futuresource_${uuid0}.${case_db};
USE futuresource_${uuid0}.${case_db};
SET enable_materialized_view_rewrite = false;

-- query 2
-- @skip_result_check=true
-- @result_contains=FUTURE_PARTITION_SOURCE_READY
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:?resolved fixture publication required}")"
uea_receipts="$uea_workspace/tests/sql/.runtime/iceberg-read-policy/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/evolved.scala"
uea_log="$uea_receipts/evolved.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/iceberg-read-policy/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
    DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "${case_db}", "future_partition_source")
    MetadataHistoryFixture.futureSourceInitialize("${case_db}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FUTURE_PARTITION_SOURCE_READY$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/evolved.jsonl"
printf 'FUTURE_PARTITION_SOURCE_READY\n'

-- query 3
-- Tag read of S0: the relation is schema0, whose only visible column is `id`.
SELECT * FROM future_partition_source FOR VERSION AS OF 'future_before_partition';

-- query 4
SELECT id, typeof(id) AS id_type FROM future_partition_source FOR VERSION AS OF 'future_before_partition' ORDER BY id;

-- query 5
-- Snapshot-log resolution of the same SnapshotId(S0) selector.
SELECT * FROM future_partition_source FOR SYSTEM_TIME AS OF '2999-12-31 00:00:00';

-- query 6
-- The Current relation reads S0 through the current schema.
SELECT id, future FROM future_partition_source ORDER BY id;

-- query 7
SELECT count(*) AS snapshot_count FROM future_partition_source$snapshots;

-- query 8
-- @skip_result_check=true
UPDATE future_partition_source SET future = 7 WHERE id = 1;

-- query 9
-- @skip_result_check=true
MERGE INTO future_partition_source AS t
USING (SELECT 1 AS id, 9 AS future) AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET future = s.future;

-- query 10
SELECT count(*) AS snapshot_count FROM future_partition_source$snapshots;

-- query 11
SELECT id, future FROM future_partition_source ORDER BY id;

-- query 12
SELECT * FROM future_partition_source FOR VERSION AS OF 'future_before_partition';

-- query 13
-- @skip_result_check=true
-- @result_contains=FUTURE_PARTITION_SOURCE_CURRENT
shell: set -eu
uea_workspace="${NOVAROCKS_WORKSPACE_ROOT:-.}"
export NOVA_ENV_REST_ENV_FILE="$(python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "${NOVA_ENV_REST_ENV_FILE:?resolved fixture publication required}")"
uea_receipts="$uea_workspace/tests/sql/.runtime/iceberg-read-policy/${uuid0}"
mkdir -p "$uea_receipts"
uea_scala="$uea_receipts/current.scala"
uea_log="$uea_receipts/current.log"
cat "$uea_workspace/tests/sql/fixtures/iceberg-delete-applicability/generate.scala" "$uea_workspace/tests/sql/fixtures/iceberg-read-policy/fixture.scala" > "$uea_scala"
cat >> "$uea_scala" <<'SCALA'
try {
    DeleteApplicabilityFixture.initialize(org.apache.spark.sql.SparkSession.active, "${case_db}", "future_partition_source")
    MetadataHistoryFixture.futureSourceObserveCurrent("${case_db}")
} catch { case failure: Throwable => failure.printStackTrace(); System.exit(1) }
SCALA
if ! "$uea_workspace/docker/iceberg-rest/spark-shell.sh" "$uea_scala" > "$uea_log" 2>&1; then tail -80 "$uea_log" >&2; exit 1; fi
grep -q '^FUTURE_PARTITION_SOURCE_CURRENT$' "$uea_log" || { tail -80 "$uea_log" >&2; exit 1; }
grep '^UEA4G_RECEIPT ' "$uea_log" > "$uea_receipts/current.jsonl"
python3 - "$uea_receipts/evolved.jsonl" "$uea_receipts/current.jsonl" <<'PY'
import json, sys
def pointer(path, record):
    rows = [json.loads(line.split(" ", 1)[1]) for line in open(path) if line.startswith("UEA4G_RECEIPT ")]
    rows = [row for row in rows if row.get("record") == record]
    if len(rows) != 1:
        sys.exit(f"expected exactly one {record} receipt in {path}, found {len(rows)}")
    return rows[0]
before = pointer(sys.argv[1], "future_partition_source_evolved")
after = pointer(sys.argv[2], "future_partition_source_current")
if before["table_uuid"] != after["table_uuid"] or before["historical_snapshot"] != after["historical_snapshot"]:
    sys.exit("partition-moving DML changed the table or historical snapshot identity")
if before["metadata_file"] == after["metadata_file"] or before["snapshot"] == after["snapshot"]:
    sys.exit("partition-moving DML did not produce a new committed state")
PY
printf 'FUTURE_PARTITION_SOURCE_CURRENT\n'

-- query 14
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS futuresource_${uuid0}.${case_db}.future_partition_source FORCE;
DROP DATABASE futuresource_${uuid0}.${case_db};
DROP CATALOG futuresource_${uuid0};
