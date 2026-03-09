-- @order_sensitive=true
-- Validate Iceberg bucket partition evolution query correctness and pruning explain text.
-- query 1
CREATE EXTERNAL CATALOG iceberg_part_cat_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "${iceberg_catalog_type}",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0};
USE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0};
DROP TABLE IF EXISTS test_users_bucketed;
CREATE TABLE test_users_bucketed (
  user_id BIGINT,
  country STRING,
  score DOUBLE
)
PARTITION BY bucket(user_id, 16);
INSERT INTO test_users_bucketed VALUES
  (1, 'US', 1.0),
  (2, 'US', 2.0),
  (3, 'CN', 3.0),
  (4, 'CN', 4.0),
  (5, 'JP', 5.0);
ALTER TABLE test_users_bucketed DROP PARTITION COLUMN bucket(user_id, 16);
ALTER TABLE test_users_bucketed ADD PARTITION COLUMN bucket(user_id, 32);
INSERT INTO test_users_bucketed VALUES
  (6, 'US', 6.0),
  (7, 'CN', 7.0),
  (16, 'US', 16.0),
  (32, 'CN', 32.0);
SELECT COUNT(*) AS cnt, SUM(score) AS sum_score
FROM test_users_bucketed;

-- query 2
USE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0};
SELECT country, COUNT(*) AS country_cnt, SUM(score) AS score_sum
FROM test_users_bucketed
GROUP BY country
ORDER BY country;

-- query 3
USE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0};
SELECT COUNT(*) AS cnt, SUM(score) AS sum_score
FROM test_users_bucketed
WHERE user_id IN (1, 2, 3, 4);

-- query 4
-- @result_contains=partitions=7/8
USE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0};
EXPLAIN VERBOSE
SELECT COUNT(*), SUM(score)
FROM test_users_bucketed
WHERE user_id IN (1, 2, 3, 4, 5, 6, 7, 16, 32);

-- query 5
-- @result_contains=partitions=3/8
USE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0};
EXPLAIN VERBOSE
SELECT COUNT(*), SUM(score)
FROM test_users_bucketed
WHERE user_id IN (1, 2, 3, 4);
SET catalog default_catalog;
DROP TABLE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0}.test_users_bucketed FORCE;
DROP DATABASE iceberg_part_cat_${uuid0}.iceberg_part_db_${uuid0};
DROP CATALOG iceberg_part_cat_${uuid0};
