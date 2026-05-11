-- @order_sensitive=true
-- @sequential=true
-- Validate Iceberg MV refresh sees Spark-written base-table snapshots.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-mv-refresh-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_mv_base_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_mv_base_${uuid0} (
  id BIGINT,
  metric INT
) USING iceberg
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_mv_base_${uuid0} VALUES
  (1, 10),
  (2, 20),
  (3, 30);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
-- @skip_result_check=true
SET CATALOG iceberg_compat_${suite_uuid0};
USE nr_compat_${suite_uuid0};
CREATE MATERIALIZED VIEW mv_spark_refresh_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, metric
FROM spark_v3_mv_base_${uuid0}
WHERE metric >= 20;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_spark_refresh_${uuid0};

-- query 4
SELECT id, metric
FROM mv_spark_refresh_${uuid0}
ORDER BY id;

-- query 5
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-mv-refresh-append-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_mv_base_${uuid0} VALUES
  (4, 40),
  (5, 5),
  (6, 60);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_spark_refresh_${uuid0};

-- query 7
SELECT id, metric
FROM mv_spark_refresh_${uuid0}
ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW mv_spark_refresh_${uuid0};
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_mv_base_${uuid0} FORCE;
