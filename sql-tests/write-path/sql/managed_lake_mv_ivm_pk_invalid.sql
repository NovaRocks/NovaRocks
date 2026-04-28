-- @order_sensitive=true
-- @tags=write_path,managed_lake,mv,iceberg,ivm,validation
-- Test Objective:
-- 1. Validate that CREATE MATERIALIZED VIEW with PRIMARY KEY rejects DDL
--    that violates the IVM Phase-2 contract before any catalog mutation.
-- 2. Cover: missing column, nullable column (two variants), empty PK
--    list (parser-level), duplicate PK columns (parser-level).
-- 3. Confirm that omitting PRIMARY KEY is unchanged behavior.
-- Note: unhashable-type rejection (PrimaryKeyTypeUnsupported) is covered
--   by unit tests in validate_ivm_primary_key; the SQL path cannot reach
--   it because all iceberg columns created via SQL are marked optional
--   (nullable), so the nullable check fires first.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_ivm_pk_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${managed_lake_warehouse}/iceberg_ivm_pk_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_ivm_pk_${uuid0}.ns_${uuid0};
CREATE TABLE mv_ivm_pk_${uuid0}.ns_${uuid0}.orders (
  order_id BIGINT NOT NULL,
  customer STRING,
  amount DOUBLE,
  tags ARRAY<STRING>
);
INSERT INTO mv_ivm_pk_${uuid0}.ns_${uuid0}.orders VALUES
  (1, 'A', 100.0, ['x']),
  (2, 'B', 200.0, ['y']);

-- query 2
-- @expect_error=PRIMARY KEY column `bogus` does not exist
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_missing
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (bogus)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 3
-- @expect_error=PRIMARY KEY column `customer` must be NOT NULL
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_nullable
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (customer)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 4
-- @expect_error=PRIMARY KEY column `amount` must be NOT NULL
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_nullable2
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (amount)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 5
-- @expect_error=PRIMARY KEY clause requires at least one column
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_empty
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY ()
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 6
-- @expect_error=duplicate column `order_id` in PRIMARY KEY clause
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_dupe
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (order_id, order_id)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ${case_db}.mv_pk_ok
DISTRIBUTED BY HASH(customer) BUCKETS 2
PRIMARY KEY (order_id)
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;

-- query 8
-- @skip_result_check=true
CREATE MATERIALIZED VIEW ${case_db}.mv_no_pk
DISTRIBUTED BY HASH(customer) BUCKETS 2
AS SELECT customer, count(*) AS c
FROM mv_ivm_pk_${uuid0}.ns_${uuid0}.orders
GROUP BY customer;
