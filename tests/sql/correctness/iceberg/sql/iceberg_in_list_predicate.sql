-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- Validate representative IN/NOT IN predicate semantics on Iceberg string columns.
-- query 1
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0};
-- query 2
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0} (
  col_str STRING,
  col_int INT
);
-- query 3
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0} VALUES
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000),
  ('abc', 2000),
  (NULL, 2000),
  ('ab1d8cf2a2c0e14fa89d8117792be6eb6f', 2001),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2001),
  ('abc', 2001),
  (NULL, 2001);
-- query 4
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
-- query 5
-- query 6
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0}.iceberg_in_tbl_${uuid0} FORCE;
-- query 7
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_in_db_${uuid0};
