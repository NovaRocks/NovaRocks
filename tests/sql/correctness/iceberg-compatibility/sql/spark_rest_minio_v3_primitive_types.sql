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
-- Validate Spark-written Iceberg v3 primitive, date/time, decimal, and NULL values.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-types-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.spark_v3_types_${uuid0};

CREATE TABLE ice_rest.${case_db}.spark_v3_types_${uuid0} (
  id BIGINT,
  c_bool BOOLEAN,
  c_int INT,
  c_bigint BIGINT,
  c_double DOUBLE,
  c_decimal DECIMAL(10, 2),
  c_date DATE,
  c_ts TIMESTAMP,
  c_string STRING
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.${case_db}.spark_v3_types_${uuid0} VALUES
  (1, TRUE, 7, CAST(7000000000 AS BIGINT), 2.50, CAST(12.34 AS DECIMAL(10, 2)), DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05', 'alpha'),
  (2, FALSE, -8, CAST(-9000000000 AS BIGINT), -2.25, CAST(-56.78 AS DECIMAL(10, 2)), DATE '1970-01-01', TIMESTAMP '2025-12-31 23:59:58', 'beta'),
  (3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT COUNT() AS rows_total,
       COUNT(c_bool) AS bool_nonnull,
       SUM(CASE WHEN c_bool THEN 1 ELSE 0 END) AS bool_true,
       SUM(c_int) AS sum_int,
       SUM(c_bigint) AS sum_bigint,
       SUM(c_decimal) AS sum_decimal,
       MIN(c_date) AS min_date,
       MAX(c_ts) AS max_ts,
       COUNT(c_string) AS string_nonnull
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_types_${uuid0};

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_types_${uuid0} FORCE;
