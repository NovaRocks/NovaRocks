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
-- Spark writes one Parquet data file with deliberately small row groups. The
-- BE must expose each row group as a sealed scan unit without whole-file
-- reopening.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-row-group-units-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};
DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.row_group_units_${uuid0};
CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.row_group_units_${uuid0} (id BIGINT, payload STRING)
USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet',
  'write.parquet.row-group-size-bytes' = '65536');
INSERT INTO ice_rest.nr_compat_${suite_uuid0}.row_group_units_${uuid0}
SELECT id, sha2(CAST(id AS STRING), 256)
FROM range(0, 50000, 1, 1) AS source(id);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT count(*) AS n_data_files
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.row_group_units_${uuid0}$files
WHERE content = 0;

-- query 3
-- @be_log_contains=NOVAROCKS_CONNECTOR_UNIT_SET_PREPARED
-- @be_log_contains=shape=one_to_many leaf_kind=row_group
-- @be_log_contains=facts_exact_units=
-- @be_log_contains=facts_available_columns=
-- @be_log_contains=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN
-- @be_log_contains=NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE
SELECT count(*) AS rows
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.row_group_units_${uuid0};

-- query 4
-- @skip_result_check=true
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.row_group_units_${uuid0} FORCE;
