-- Validate UK/FK-based table prune and aggregate elimination on Iceberg tables.
-- query 1
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.txn
-- @result_not_contains=TABLE: iceberg_pkfk_db_${uuid0}.payment
CREATE EXTERNAL CATALOG iceberg_pkfk_cat_${uuid0}
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
CREATE DATABASE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0};
USE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0};
CREATE TABLE payment (
  id INT,
  created_at DATE,
  status STRING
);
CREATE TABLE txn (
  id INT,
  created_at DATE,
  payment_id INT
);
INSERT INTO payment VALUES
  (1, '2025-01-01', 'COMPLETED'),
  (2, '2025-01-02', 'PENDING'),
  (3, '2025-01-03', 'FAILED'),
  (4, '2025-01-04', 'COMPLETED'),
  (5, '2025-01-05', 'REFUNDED');
INSERT INTO txn VALUES
  (101, '2025-01-01', 1),
  (102, '2025-01-02', 2),
  (103, '2025-01-03', 3),
  (104, '2025-01-04', 4),
  (105, '2025-01-05', 5),
  (106, '2025-01-06', 1);
ALTER TABLE payment SET ("unique_constraints" = "id");
ALTER TABLE txn SET ("foreign_key_constraints" = "(payment_id) REFERENCES payment(id)");
SET enable_ukfk_opt = false;
SET enable_rbo_table_prune = true;
SET enable_cbo_table_prune = true;
SET enable_table_prune_on_update = true;
EXPLAIN
SELECT txn.id, txn.created_at, txn.payment_id
FROM txn
LEFT JOIN payment
  ON payment.id = txn.payment_id;

-- query 2
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.payment
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.txn
USE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0};
EXPLAIN
SELECT txn.id, txn.created_at, txn.payment_id
FROM txn
INNER JOIN payment
  ON payment.id = txn.payment_id;

-- query 3
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.txn
-- @result_contains=payment_id IS NOT NULL
-- @result_not_contains=TABLE: iceberg_pkfk_db_${uuid0}.payment
USE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0};
SET enable_ukfk_opt = true;
SET enable_rbo_table_prune = false;
SET enable_cbo_table_prune = false;
SET enable_table_prune_on_update = false;
EXPLAIN
SELECT txn.id, txn.created_at, txn.payment_id
FROM txn
INNER JOIN payment
  ON payment.id = txn.payment_id;

-- query 4
-- @result_contains=AGGREGATE
USE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0};
SET enable_eliminate_agg = false;
EXPLAIN
SELECT COUNT(1) AS cnt, payment.id
FROM payment
GROUP BY payment.id;

-- query 5
-- @result_not_contains=AGGREGATE
USE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0};
SET enable_eliminate_agg = true;
EXPLAIN
SELECT COUNT(1) AS cnt, payment.id
FROM payment
GROUP BY payment.id;
SET catalog default_catalog;
DROP TABLE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0}.txn FORCE;
DROP TABLE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0}.payment FORCE;
DROP DATABASE iceberg_pkfk_cat_${uuid0}.iceberg_pkfk_db_${uuid0};
DROP CATALOG iceberg_pkfk_cat_${uuid0};
