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
-- Validate NovaRocks reads Spark-written data across Iceberg partition spec evolution.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-part-evo-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.spark_v3_part_evo_${uuid0};

CREATE TABLE ice_rest.${case_db}.spark_v3_part_evo_${uuid0} (
  id BIGINT,
  region STRING,
  metric INT
) USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.${case_db}.spark_v3_part_evo_${uuid0} VALUES
  (1, 'us', 10),
  (2, 'eu', 20),
  (3, 'us', 30);

ALTER TABLE ice_rest.${case_db}.spark_v3_part_evo_${uuid0}
  DROP PARTITION FIELD region;

ALTER TABLE ice_rest.${case_db}.spark_v3_part_evo_${uuid0}
  ADD PARTITION FIELD bucket(4, id);

INSERT INTO ice_rest.${case_db}.spark_v3_part_evo_${uuid0} VALUES
  (4, 'eu', 40),
  (5, 'apac', 50);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT region, COUNT(*) AS cnt, SUM(metric) AS total_metric
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_part_evo_${uuid0}
GROUP BY region
ORDER BY region;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_part_evo_${uuid0} ;
