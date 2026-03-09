-- @order_sensitive=true
-- Validate Iceberg min/max optimization correctness against the non-optimized path.
-- query 1
CREATE EXTERNAL CATALOG iceberg_mm_cat_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "${iceberg_catalog_type}",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}",
  "enable_iceberg_metadata_cache" = "true",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE iceberg_mm_cat_${uuid0}.iceberg_mm_db_${uuid0};
CREATE TABLE iceberg_mm_cat_${uuid0}.iceberg_mm_db_${uuid0}.iceberg_mm_tbl_${uuid0} (
  c_tinyint TINYINT,
  c_smallint SMALLINT,
  c_int INT,
  c_bigint BIGINT,
  c_bool BOOLEAN,
  c_float FLOAT,
  c_double DOUBLE,
  c_date DATE
);
INSERT INTO iceberg_mm_cat_${uuid0}.iceberg_mm_db_${uuid0}.iceberg_mm_tbl_${uuid0} VALUES
  (1, 10, 100, 1000, true, 1.1, 2.2, '2025-06-29'),
  (2, 20, 200, 2000, false, 2.2, 3.3, '2025-06-30'),
  (3, 30, 300, 3000, true, 3.3, 4.4, '2025-07-01');
SET enable_min_max_optimization = true;
SELECT
  MIN(c_tinyint) AS min_tinyint,
  MAX(c_tinyint) AS max_tinyint,
  MIN(c_smallint) AS min_smallint,
  MAX(c_smallint) AS max_smallint,
  MIN(c_int) AS min_int,
  MAX(c_int) AS max_int,
  MIN(c_bigint) AS min_bigint,
  MAX(c_bigint) AS max_bigint,
  MIN(c_bool) AS min_bool,
  MAX(c_bool) AS max_bool,
  MIN(c_float) AS min_float,
  MAX(c_float) AS max_float,
  MIN(c_double) AS min_double,
  MAX(c_double) AS max_double,
  MIN(c_date) AS min_date,
  MAX(c_date) AS max_date
FROM iceberg_mm_cat_${uuid0}.iceberg_mm_db_${uuid0}.iceberg_mm_tbl_${uuid0};

-- query 2
USE iceberg_mm_cat_${uuid0}.iceberg_mm_db_${uuid0};
SET enable_min_max_optimization = false;
SELECT
  MIN(c_tinyint) AS min_tinyint,
  MAX(c_tinyint) AS max_tinyint,
  MIN(c_smallint) AS min_smallint,
  MAX(c_smallint) AS max_smallint,
  MIN(c_int) AS min_int,
  MAX(c_int) AS max_int,
  MIN(c_bigint) AS min_bigint,
  MAX(c_bigint) AS max_bigint,
  MIN(c_bool) AS min_bool,
  MAX(c_bool) AS max_bool,
  MIN(c_float) AS min_float,
  MAX(c_float) AS max_float,
  MIN(c_double) AS min_double,
  MAX(c_double) AS max_double,
  MIN(c_date) AS min_date,
  MAX(c_date) AS max_date
FROM iceberg_mm_tbl_${uuid0};
SET catalog default_catalog;
DROP TABLE iceberg_mm_cat_${uuid0}.iceberg_mm_db_${uuid0}.iceberg_mm_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_mm_cat_${uuid0}.iceberg_mm_db_${uuid0};
DROP CATALOG iceberg_mm_cat_${uuid0};
