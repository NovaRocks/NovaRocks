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
-- Validate Spark DROP COLUMN plus re-ADD same column name preserves Iceberg field-id semantics.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-readd-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.spark_v3_readd_${uuid0};

CREATE TABLE ice_rest.${case_db}.spark_v3_readd_${uuid0} (
  id BIGINT,
  note STRING
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.${case_db}.spark_v3_readd_${uuid0} VALUES
  (1, 'old-a'),
  (2, 'old-b');

ALTER TABLE ice_rest.${case_db}.spark_v3_readd_${uuid0}
  DROP COLUMN note;

ALTER TABLE ice_rest.${case_db}.spark_v3_readd_${uuid0}
  ADD COLUMN note STRING;

INSERT INTO ice_rest.${case_db}.spark_v3_readd_${uuid0} VALUES
  (3, 'new-c');
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id, note
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_readd_${uuid0}
ORDER BY id;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_readd_${uuid0} ;
