-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,row_lineage,join,a11,base_type_change,referenced,error
-- Test Point: Join IMV blocks refresh when a referenced column on either base changes type.
-- Method: Create a two-base join MV, widen a left-base projected column through Spark, then refresh.
-- Scope: Multi-base A11 schema contract, current Iceberg schema validation, metadata-only evolution.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_a11_type_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} (
  id INT NOT NULL,
  rid INT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_right_${uuid0} (
  rid INT NOT NULL,
  label STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  (1, 10, 100),
  (2, 20, 200);
INSERT INTO ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_right_${uuid0} VALUES
  (10, 'old-a'),
  (20, 'old-b');

-- query 2
-- @skip_result_check=true
SET CATALOG ice_ivm_join_a11_type_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT l.id, l.amount, r.label
FROM join_left_${uuid0} AS l
JOIN join_right_${uuid0} AS r ON l.rid = r.rid;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 4
SELECT id, amount, label
FROM join_mv_${uuid0}
ORDER BY id, label;

-- query 5
-- @result_contains=SPARK_SQL_OK
shell: set -eu
tmp_sql="$(mktemp "${TMPDIR:-/tmp}/novarocks-join-a11-type-XXXXXX.sql")"
trap 'rm -f "$tmp_sql"' EXIT
cat > "$tmp_sql" <<'SPARK_SQL'
ALTER TABLE ice_rest.ns_${uuid0}.join_left_${uuid0} ALTER COLUMN amount TYPE bigint;
SPARK_SQL
"${NOVAROCKS_WORKSPACE_ROOT:-.}/docker/iceberg-rest/spark-sql.sh" "$tmp_sql"
printf 'SPARK_SQL_OK\n'

-- query 6
-- @skip_result_check=true
INSERT INTO ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  (3, 20, 300);

-- query 7
-- @expect_error=changed type from
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_mv_${uuid0};
DROP TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_left_${uuid0} FORCE;
DROP TABLE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0}.join_right_${uuid0} FORCE;
DROP DATABASE ice_ivm_join_a11_type_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_a11_type_${uuid0};
