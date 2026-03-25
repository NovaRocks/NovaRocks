-- @order_sensitive=true
-- Validate representative IN/NOT IN predicate semantics on Iceberg string columns.
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0};
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0} (
  col_str STRING,
  col_int INT
);
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0} VALUES
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000),
  ('abc', 2000),
  (NULL, 2000),
  ('ab1d8cf2a2c0e14fa89d8117792be6eb6f', 2001),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2001),
  ('abc', 2001),
  (NULL, 2001);
SELECT
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND col_str NOT IN (md5('1d8cf2a2c0e14fa89d8117792be6eb6f'), 'abc')
  ), '') AS not_in_mixed_literal,
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND col_str NOT IN (md5('1d8cf2a2c0e14fa89d8117792be6eb6f'), md5('abc'))
  ), '') AS not_in_all_hash,
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND col_str NOT IN ('1d8cf2a2c0e14fa89d8117792be6eb6f', md5('abc'))
  ), '') AS not_in_literal_hash,
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND col_str IN (md5('1d8cf2a2c0e14fa89d8117792be6eb6f'), 'abc')
  ), '') AS in_hash_literal,
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND col_str IN (md5('1d8cf2a2c0e14fa89d8117792be6eb6f'), md5('abc'))
  ), '') AS in_all_hash,
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND col_str IN ('1d8cf2a2c0e14fa89d8117792be6eb6f', md5('abc'))
  ), '') AS in_literal_hash,
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND (col_str NOT IN (md5('1d8cf2a2c0e14fa89d8117792be6eb6f'), 'abc') OR col_str IS NULL)
  ), '') AS not_in_or_null,
  COALESCE((
    SELECT GROUP_CONCAT(IFNULL(col_str, 'NULL') ORDER BY IF(col_str IS NULL, 1, 0), col_str SEPARATOR ',')
    FROM iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0}
    WHERE col_int = 2000
      AND (col_str IN ('1d8cf2a2c0e14fa89d8117792be6eb6f', md5('abc')) OR col_str IS NULL)
  ), '') AS in_or_null;
SET catalog default_catalog;
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0} FORCE;
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0};
