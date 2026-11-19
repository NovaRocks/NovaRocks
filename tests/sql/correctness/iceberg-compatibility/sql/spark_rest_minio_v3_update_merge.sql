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
-- Validate NovaRocks reads Spark-written Iceberg v3 UPDATE and MERGE results.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-update-merge-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.spark_v3_update_merge_${uuid0};

CREATE TABLE ice_rest.${case_db}.spark_v3_update_merge_${uuid0} (
  id BIGINT,
  label STRING,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet',
  'write.update.mode' = 'merge-on-read',
  'write.merge.mode' = 'merge-on-read'
);

INSERT INTO ice_rest.${case_db}.spark_v3_update_merge_${uuid0} VALUES
  (1, 'base-a', 10),
  (2, 'base-b', 20),
  (3, 'base-c', 30);

UPDATE ice_rest.${case_db}.spark_v3_update_merge_${uuid0}
SET label = 'updated-b', metric = metric + 100
WHERE id = 2;

MERGE INTO ice_rest.${case_db}.spark_v3_update_merge_${uuid0} AS target
USING (
  SELECT 3 AS id, 'merged-c' AS label, 330 AS metric
  UNION ALL
  SELECT 4 AS id, 'inserted-d' AS label, 40 AS metric
) AS source
ON target.id = source.id
WHEN MATCHED THEN UPDATE SET label = source.label, metric = source.metric
WHEN NOT MATCHED THEN INSERT (id, label, metric) VALUES (source.id, source.label, source.metric);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id, label, metric
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_update_merge_${uuid0}
ORDER BY id;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_update_merge_${uuid0} ;
