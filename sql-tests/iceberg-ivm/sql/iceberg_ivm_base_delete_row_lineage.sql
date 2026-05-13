-- @sequential=true
-- @order_sensitive=true
-- @tags=mv,iceberg,ivm,row_lineage,target_apply,base_delete,projection,filter,equality_delete
-- Test Objective:
-- 1. Validate Iceberg-backed projection/filter MV incremental refresh uses the
--    base Iceberg v3 _row_id as the hidden target apply key.
-- 2. Validate base DELETE, UPDATE, predicate in/out, and equality-delete
--    changes are applied to the Iceberg MV target without a PRIMARY KEY.
-- 3. Validate the hidden apply-key column is not exposed to users.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ice_ivm_row_lineage_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_ivm_row_lineage_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE ice_ivm_row_lineage_${uuid0}.ns_${uuid0};
CREATE TABLE ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0} (
  id INT NOT NULL,
  amount BIGINT,
  customer STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0} VALUES
  (1, 100, 'Alice'),
  (2, 40, 'Bob'),
  (3, 70, 'Carol');
SET CATALOG ice_ivm_row_lineage_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount
FROM ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0}
WHERE amount >= 50;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 3
SELECT * FROM orders_mv_${uuid0} ORDER BY id;

-- query 4
-- @expect_error=__nova_base_row_id
SELECT __nova_base_row_id FROM orders_mv_${uuid0};

-- query 5
-- @skip_result_check=true
INSERT INTO orders_${uuid0} VALUES (4, 60, 'Dave');
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 6
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 7
-- @skip_result_check=true
DELETE FROM orders_${uuid0} WHERE id = 1;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 8
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 9
-- @skip_result_check=true
UPDATE orders_${uuid0} SET amount = 90 WHERE id = 3;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 10
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 11
-- @skip_result_check=true
UPDATE orders_${uuid0} SET amount = 30 WHERE id = 4;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 12
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 13
-- @skip_result_check=true
UPDATE orders_${uuid0} SET amount = 55 WHERE id = 2;
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 14
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 15
-- @skip_result_check=true
ALTER TABLE orders_${uuid0} ADD EQUALITY DELETE (id) VALUES (3);
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 16
SELECT id, amount FROM orders_mv_${uuid0} ORDER BY id;

-- query 17
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv_${uuid0};
DROP TABLE ice_ivm_row_lineage_${uuid0}.ns_${uuid0}.orders_${uuid0} FORCE;
DROP DATABASE ice_ivm_row_lineage_${uuid0}.ns_${uuid0};
DROP CATALOG ice_ivm_row_lineage_${uuid0};
