-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,row_lineage,join,negative
-- Test Point: Unsupported join IMV shapes fail at CREATE time.
-- Method: Try outer join, non-equi join, and three-table join.
-- Scope: Iceberg-backed join IMV shape validation.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_reject_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_join_reject_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_reject_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} (
  id BIGINT NOT NULL,
  rid BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} (
  id BIGINT NOT NULL,
  rid BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_extra_${uuid0} (
  id BIGINT NOT NULL,
  rid BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_reject_${uuid0};
USE ns_${uuid0};

-- query 2
-- @expect_error=incremental join MV supports only two-table inner equi-join
CREATE MATERIALIZED VIEW reject_outer_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.id
FROM ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} AS l
LEFT JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} AS r ON l.rid = r.rid;

-- query 3
-- @expect_error=incremental join MV supports only AND-combined equi-join predicates
CREATE MATERIALIZED VIEW reject_nonequi_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.id
FROM ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} AS l
JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} AS r ON l.rid > r.rid;

-- query 4
-- @expect_error=incremental join MV requires exactly two Iceberg base tables
CREATE MATERIALIZED VIEW reject_three_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT l.id
FROM ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} AS l
JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} AS r ON l.rid = r.rid
JOIN ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_extra_${uuid0} AS x ON x.rid = r.rid;

-- query 5
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS reject_outer_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS reject_nonequi_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS reject_three_${uuid0};
DROP TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_left_${uuid0} FORCE;
DROP TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_right_${uuid0} FORCE;
DROP TABLE ice_ivm_join_reject_${uuid0}.ns_${uuid0}.reject_extra_${uuid0} FORCE;
DROP DATABASE ice_ivm_join_reject_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_reject_${uuid0};
