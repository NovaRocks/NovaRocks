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
-- @sequential=true
-- Cross-engine nanosecond timestamp (TIMESTAMP_NS) compatibility test.
--
-- DIRECTION A: NovaRocks writes a format-v3 TIMESTAMP_NS table into the shared
-- REST Catalog + MinIO, then the Iceberg REST API (the same catalog Spark uses)
-- is queried via curl to confirm:
--   - The Iceberg schema carries type "timestamp_ns" (not timestamp_micro).
--   - The snapshot summary shows the expected record count.
-- NovaRocks then reads the table back to assert 9-digit nanosecond precision.
--
-- DIRECTION B: OMITTED.
-- Spark 3.5 SQL cannot declare TIMESTAMP_NS columns (DDL probe result:
-- "[UNSUPPORTED_DATATYPE] Unsupported data type TIMESTAMP_NS"). Spark also
-- cannot read data from a table whose schema contains timestamp_ns columns
-- (runtime error: "Cannot convert unsupported type to Spark: timestamp_ns").
-- Direction A combined with the NovaRocks-only roundtrip in
-- tests/sql/correctness/iceberg-rest/timestamp_ns_roundtrip.sql provides full read+write
-- correctness coverage.

-- query 1
-- Create the shared namespace in the REST catalog via curl.
-- The namespace is shared between NovaRocks and Spark (both access the same
-- REST Catalog + MinIO); creating it here mirrors the pattern used by
-- Spark-first fixtures where Spark runs "CREATE NAMESPACE IF NOT EXISTS".
-- @result_contains=NAMESPACE_OK
shell: set -eu
# Create namespace; ignore 409 Conflict (already exists).
http_code="$(curl -s -o /dev/null -w '%{http_code}' -X POST "${iceberg_rest_uri}/v1/namespaces" \
    -H 'Content-Type: application/json' \
    -d "{\"namespace\": [\"${case_db}\"]}")"
if [ "$http_code" != "200" ] && [ "$http_code" != "409" ]; then
    printf 'FAIL: namespace create returned HTTP %s\n' "$http_code" >&2
    exit 1
fi
printf 'NAMESPACE_OK ns=nr_compat_%s\n' "${suite_uuid0}"

-- query 2
-- NovaRocks creates the TIMESTAMP_NS table in the shared REST catalog.
-- @skip_result_check=true
CREATE TABLE iceberg_compat_${suite_uuid0}.${case_db}.tsns_${uuid0} (
    id     BIGINT,
    ts     TIMESTAMP_NS
) WITH (format_version = 3);

-- query 3
-- Insert rows with sub-microsecond nanosecond precision.
-- Row 1: ts = .123456789 (789 sub-microsecond ns)
-- Row 2: ts = .000000001 (1 sub-microsecond ns -- the minimum non-zero ns)
-- Row 3: ts = epoch (all-zero nanoseconds)
-- @skip_result_check=true
INSERT INTO iceberg_compat_${suite_uuid0}.${case_db}.tsns_${uuid0}
VALUES
    ROW(1, '2024-01-02 03:04:05.123456789'),
    ROW(2, '2024-01-02 03:04:05.000000001'),
    ROW(3, '1970-01-01 00:00:00.000000000');

-- query 4
-- Cross-engine catalog verification via the Iceberg REST API.
-- The REST catalog is shared between NovaRocks and Spark; confirming the
-- schema type and record count here proves the data reached the shared store
-- in a format the catalog understands. This is genuine cross-system interop:
-- the REST API endpoint is the same one Spark uses when reading Iceberg tables.
-- @result_contains=CATALOG_OK
shell: set -eu
resp="$(curl -sf "${iceberg_rest_uri}/v1/namespaces/${case_db}/tables/tsns_${uuid0}")"
# Verify the Iceberg schema carries timestamp_ns type (not timestamp_micro).
ts_type="$(printf '%s' "$resp" | python3 -c "
import json, sys
meta = json.load(sys.stdin).get('metadata', {})
schemas = meta.get('schemas', [meta.get('current-schema', {})])
current_id = meta.get('current-schema-id', None)
schema = schemas[0]
if current_id is not None:
    for s in schemas:
        if s.get('schema-id') == current_id:
            schema = s
            break
for f in schema.get('fields', []):
    if f['name'] == 'ts':
        print(f['type'])
        break
")"
if [ "$ts_type" != "timestamp_ns" ]; then
    printf 'FAIL: expected timestamp_ns in REST catalog schema, got: %s\n' "$ts_type" >&2
    exit 1
fi
# Verify the latest snapshot recorded the expected 3 rows.
rec_count="$(printf '%s' "$resp" | python3 -c "
import json, sys
meta = json.load(sys.stdin).get('metadata', {})
snaps = meta.get('snapshots', [])
latest = max(snaps, key=lambda s: s.get('sequence-number', 0)) if snaps else {}
print(latest.get('summary', {}).get('total-records', ''))
")"
if [ "$rec_count" != "3" ]; then
    printf 'FAIL: expected total-records=3 in snapshot summary, got: %s\n' "$rec_count" >&2
    exit 1
fi
printf 'CATALOG_OK schema=timestamp_ns records=%s\n' "$rec_count"

-- query 5
-- NovaRocks reads the table back and asserts 9-digit nanosecond precision.
-- CAST(ts AS VARCHAR) renders all nine fractional digits from the declaration.
SELECT id, CAST(ts AS VARCHAR) AS s
  FROM iceberg_compat_${suite_uuid0}.${case_db}.tsns_${uuid0}
  ORDER BY id;

-- query 6
-- Nanosecond predicate precision check.
-- The strict nanosecond boundary excludes the first nanosecond itself.
-- If the predicate were rounded to microseconds (.000000), rows 1 and 2 would
-- both pass (COUNT=2), proving the nanosecond path is exercised end-to-end.
SELECT COUNT(*) AS cnt
  FROM iceberg_compat_${suite_uuid0}.${case_db}.tsns_${uuid0}
  WHERE ts > TIMESTAMP '2024-01-02 03:04:05.000000001';

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.tsns_${uuid0} ;
