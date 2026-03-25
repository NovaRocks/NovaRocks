-- @order_sensitive=true
-- Validate Iceberg TIME round-trip through the Parquet write/read path.
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_time_db_${uuid0};
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} (
  col_id INT,
  col_time TIME
);
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} VALUES
  (1, '01:02:03'),
  (2, '20:20:20'),
  (3, NULL);
SELECT col_id, col_time
FROM iceberg_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0}
ORDER BY col_id;
SET catalog default_catalog;
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_time_db_${uuid0}.iceberg_time_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_time_db_${uuid0};
