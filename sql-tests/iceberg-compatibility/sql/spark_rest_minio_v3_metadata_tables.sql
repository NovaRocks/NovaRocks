-- @order_sensitive=true
-- @sequential=true
-- Validate NovaRocks metadata-table reads over Spark-written Iceberg v3 snapshots.

-- query 1
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-spark-v3-meta-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
CREATE NAMESPACE IF NOT EXISTS ice_rest.nr_compat_${suite_uuid0};

DROP TABLE IF EXISTS ice_rest.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0};

CREATE TABLE ice_rest.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0} (
  id BIGINT,
  region STRING,
  metric INT
) USING iceberg
PARTITIONED BY (region)
TBLPROPERTIES (
  'format-version' = '3',
  'write.format.default' = 'parquet'
);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0} VALUES
  (1, 'us', 10),
  (2, 'eu', 20);

INSERT INTO ice_rest.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0} VALUES
  (3, 'us', 30);
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 2
SELECT COUNT(*) AS snapshot_count
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$snapshots;

-- query 3
SELECT COUNT(*) AS history_count
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$history;

-- query 4
SELECT count(*) AS n_data_files
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$files
WHERE content = 0;

-- query 5
SELECT count(*) > 0 AS has_manifests
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$manifests;

-- query 6
SELECT count(*) > 0 AS has_added_entries
FROM iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0}$entries
WHERE status = 1 AND sequence_number IS NOT NULL;

-- query 7
DROP TABLE iceberg_compat_${suite_uuid0}.nr_compat_${suite_uuid0}.spark_v3_meta_${uuid0} FORCE;
