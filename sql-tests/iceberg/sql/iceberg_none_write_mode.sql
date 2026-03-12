-- @order_sensitive=true
-- Validate Iceberg writes when write.metadata.metrics.default is set to none.
CREATE DATABASE iceberg_cat_${uuid0}.iceberg_none_db_${uuid0};
CREATE TABLE iceberg_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0} (
  k1 INT
) PROPERTIES ("write.metadata.metrics.default" = "none");
INSERT INTO iceberg_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0}
SELECT 1;
SELECT k1
FROM iceberg_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0};
SET catalog default_catalog;
DROP TABLE iceberg_cat_${uuid0}.iceberg_none_db_${uuid0}.iceberg_none_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_cat_${uuid0}.iceberg_none_db_${uuid0};
