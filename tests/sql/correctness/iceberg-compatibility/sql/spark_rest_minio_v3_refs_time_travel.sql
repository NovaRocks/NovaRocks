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
-- Validate NovaRocks reads Spark-created Iceberg refs through FOR VERSION AS OF.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-refs-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.spark_v3_refs_${uuid0};

CREATE TABLE ice_rest.${case_db}.spark_v3_refs_${uuid0} (
  id BIGINT,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.${case_db}.spark_v3_refs_${uuid0} VALUES
  (1, 10),
  (2, 20);

ALTER TABLE ice_rest.${case_db}.spark_v3_refs_${uuid0}
  CREATE BRANCH backup;

INSERT INTO ice_rest.${case_db}.spark_v3_refs_${uuid0} VALUES
  (3, 30);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT ref_name, row_count, metric_sum
FROM (
  SELECT 'backup' AS ref_name, COUNT(*) AS row_count, SUM(metric) AS metric_sum
  FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_refs_${uuid0}
  FOR VERSION AS OF 'backup'
  UNION ALL
  SELECT 'main' AS ref_name, COUNT(*) AS row_count, SUM(metric) AS metric_sum
  FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_refs_${uuid0}
) refs
ORDER BY ref_name;

-- query 3
DROP BRANCH backup IN TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_refs_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_refs_${uuid0} ;
