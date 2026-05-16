-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,row_lineage,join,two_base_delta
-- Test Point: Iceberg-backed join IMV supports two-table inner equi-join with both bases changing in one refresh.
-- Method: Create two Iceberg v3 row-lineage base tables, create join MV, mutate both bases, refresh, and compare MV result with base join.
-- Scope: Iceberg v3 row-lineage, two-table inner equi-join, telescoping delta, composite join row key.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_join_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_${uuid0}.ns_${uuid0}.join_left_${uuid0} (
  id BIGINT NOT NULL,
  rid BIGINT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_${uuid0}.ns_${uuid0}.join_right_${uuid0} (
  rid BIGINT NOT NULL,
  label STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT l.id, l.amount, r.label
FROM ice_ivm_join_${uuid0}.ns_${uuid0}.join_left_${uuid0} AS l
JOIN ice_ivm_join_${uuid0}.ns_${uuid0}.join_right_${uuid0} AS r ON l.rid = r.rid
WHERE l.amount >= 100;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_join_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  (1, 10, 100),
  (2, 20, 200);
INSERT INTO ice_ivm_join_${uuid0}.ns_${uuid0}.join_right_${uuid0} VALUES
  (10, 'old-a'),
  (20, 'old-b');

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 4
SELECT id, amount, label
FROM join_mv_${uuid0}
ORDER BY id, label;

-- query 5
-- @skip_result_check=true
INSERT INTO ice_ivm_join_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES (3, 30, 300);
INSERT INTO ice_ivm_join_${uuid0}.ns_${uuid0}.join_right_${uuid0} VALUES (30, 'new-c');
DELETE FROM ice_ivm_join_${uuid0}.ns_${uuid0}.join_left_${uuid0} WHERE id = 1;
UPDATE ice_ivm_join_${uuid0}.ns_${uuid0}.join_right_${uuid0}
SET label = 'new-b'
WHERE rid = 20;

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 7
SELECT id, amount, label
FROM join_mv_${uuid0}
ORDER BY id, label;

-- query 8
SELECT l.id, l.amount, r.label
FROM ice_ivm_join_${uuid0}.ns_${uuid0}.join_left_${uuid0} AS l
JOIN ice_ivm_join_${uuid0}.ns_${uuid0}.join_right_${uuid0} AS r ON l.rid = r.rid
WHERE l.amount >= 100
ORDER BY l.id, r.label;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_mv_${uuid0};
DROP TABLE ice_ivm_join_${uuid0}.ns_${uuid0}.join_left_${uuid0} FORCE;
DROP TABLE ice_ivm_join_${uuid0}.ns_${uuid0}.join_right_${uuid0} FORCE;
DROP DATABASE ice_ivm_join_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_${uuid0};
