-- @order_sensitive=true
-- Validate Iceberg MV target catalog semantics against REST catalog:
-- - target is created as a normal Iceberg table in the current catalog/database
-- - CREATE does not write data; REFRESH populates the target
-- - SHOW/REFRESH/DROP use NovaRocks relationship metadata
-- - existing target and non-Iceberg current catalog fail fast

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.analytics_${uuid0};
CREATE DATABASE iceberg_rest_${suite_uuid0}.sales_${uuid0};
CREATE TABLE iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0} (
  id INT,
  name STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0}
VALUES (1, 'a'), (2, 'b');
SET CATALOG iceberg_rest_${suite_uuid0};
USE analytics_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE MATERIALIZED VIEW mv_orders_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, name
FROM iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0};

-- query 3
SELECT COUNT(*) AS n FROM mv_orders_${uuid0};

-- query 4
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW mv_orders_${uuid0};

-- query 5
SELECT id, name FROM mv_orders_${uuid0} ORDER BY id;

-- query 6
-- @result_contains=mv_orders_
-- @result_contains=iceberg
SHOW MATERIALIZED VIEWS;

-- query 7
-- @expect_error=Iceberg MV target table iceberg_rest_${suite_uuid0}.analytics_${uuid0}.mv_conflict_${uuid0} already exists
CREATE TABLE mv_conflict_${uuid0} (id INT);
CREATE MATERIALIZED VIEW mv_conflict_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id
FROM iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0};

-- query 8
-- @skip_result_check=true
DROP TABLE mv_conflict_${uuid0};

-- query 9
-- @expect_error=requires current catalog to be an Iceberg catalog
SET CATALOG default_catalog;
USE default;
CREATE MATERIALIZED VIEW mv_bad_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id
FROM iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0};

-- query 10
-- @skip_result_check=true
SET CATALOG iceberg_rest_${suite_uuid0};
USE analytics_${uuid0};
DROP MATERIALIZED VIEW mv_orders_${uuid0};

-- query 11
-- @expect_error=does not exist
SELECT COUNT(*) FROM mv_orders_${uuid0};

-- query 12
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.sales_${uuid0}.orders_${uuid0};
DROP DATABASE iceberg_rest_${suite_uuid0}.analytics_${uuid0};
DROP DATABASE iceberg_rest_${suite_uuid0}.sales_${uuid0};
