-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,row_lineage,join,multiplicity,key_update
-- Test Point: Join IMV preserves multiplicity across join-key updates and both-side changes.
-- Method: Refresh a two-base join MV after one-to-many and many-to-many changes on both bases.
-- Scope: Iceberg v3 row-lineage, inner equi-join, composite join row key, left/right delta windows.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_join_mult_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_join_mult_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_join_mult_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0} (
  order_id BIGINT NOT NULL,
  rid BIGINT,
  amount INT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
CREATE TABLE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0} (
  dim_id BIGINT NOT NULL,
  rid BIGINT,
  label STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
SET CATALOG ice_ivm_join_mult_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW join_mv_${uuid0}
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS
SELECT l.order_id, l.amount, r.dim_id, r.label
FROM ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0} AS l
JOIN ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0} AS r ON l.rid = r.rid
WHERE l.amount >= 50;

-- query 2
-- @skip_result_check=true
INSERT INTO ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  (1, 10, 100),
  (2, 20, 200),
  (3, 20, 300);
INSERT INTO ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0} VALUES
  (101, 10, 'A1'),
  (201, 20, 'B1'),
  (202, 20, 'B2');

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 4
SELECT order_id, amount, dim_id, label
FROM join_mv_${uuid0}
ORDER BY order_id, dim_id, label;

-- query 5
-- @skip_result_check=true
UPDATE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0}
SET rid = 20
WHERE order_id = 1;
UPDATE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0}
SET label = 'B2x'
WHERE dim_id = 202;
DELETE FROM ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0}
WHERE dim_id = 201;
INSERT INTO ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0} VALUES
  (203, 20, 'B3');

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 7
SELECT order_id, amount, dim_id, label
FROM join_mv_${uuid0}
ORDER BY order_id, dim_id, label;

-- query 8
SELECT l.order_id, l.amount, r.dim_id, r.label
FROM ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0} AS l
JOIN ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0} AS r ON l.rid = r.rid
WHERE l.amount >= 50
ORDER BY l.order_id, r.dim_id, r.label;

-- query 9
-- @skip_result_check=true
DELETE FROM ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0}
WHERE order_id = 2;
UPDATE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0}
SET amount = 90
WHERE order_id = 3;
UPDATE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0}
SET rid = 30
WHERE dim_id = 203;
INSERT INTO ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0} VALUES
  (4, 30, 400);

-- query 10
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW join_mv_${uuid0};

-- query 11
SELECT order_id, amount, dim_id, label
FROM join_mv_${uuid0}
ORDER BY order_id, dim_id, label;

-- query 12
SELECT l.order_id, l.amount, r.dim_id, r.label
FROM ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0} AS l
JOIN ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0} AS r ON l.rid = r.rid
WHERE l.amount >= 50
ORDER BY l.order_id, r.dim_id, r.label;

-- query 13
-- @skip_result_check=true
DROP MATERIALIZED VIEW join_mv_${uuid0};
DROP TABLE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_left_${uuid0} FORCE;
DROP TABLE ice_ivm_join_mult_${uuid0}.ns_${uuid0}.join_right_${uuid0} FORCE;
DROP DATABASE ice_ivm_join_mult_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_join_mult_${uuid0};
