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
-- Validate Spark-written Iceberg v3 ARRAY, MAP, STRUCT, and nested ARRAY<STRUCT> reads.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-complex-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.${case_db};

DROP TABLE IF EXISTS ice_rest.${case_db}.spark_v3_complex_${uuid0};

CREATE TABLE ice_rest.${case_db}.spark_v3_complex_${uuid0} (
  id BIGINT,
  profile STRUCT<name: STRING, city: STRING>,
  tags ARRAY<STRING>,
  attrs MAP<STRING, INT>,
  events ARRAY<STRUCT<kind: STRING, weight: INT>>
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.${case_db}.spark_v3_complex_${uuid0} VALUES
  (
    1,
    named_struct('name', 'ann', 'city', 'sf'),
    array('blue', 'green'),
    map('score', 10, 'level', 2),
    array(named_struct('kind', 'click', 'weight', 3), named_struct('kind', 'view', 'weight', 4))
  ),
  (
    2,
    named_struct('name', 'bob', 'city', 'ny'),
    array('red'),
    map('score', 20, 'level', 1),
    array(named_struct('kind', 'click', 'weight', 7))
  );
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT id,
       profile.city AS city,
       cardinality(tags) AS tag_count,
       tags[1] AS first_tag,
       attrs['score'] AS score,
       events[1].weight AS first_event_weight
FROM iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_complex_${uuid0}
ORDER BY id;

-- query 3
DROP TABLE iceberg_compat_${suite_uuid0}.${case_db}.spark_v3_complex_${uuid0} ;
