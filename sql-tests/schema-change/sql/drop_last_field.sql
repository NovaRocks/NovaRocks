-- Test Objective:
-- 1. Preserve the negative coverage for dropping the final remaining nested field.
-- query 1
-- @expect_error=can not drop any more
DROP DATABASE IF EXISTS sc_last_field_${uuid0} FORCE;
CREATE DATABASE sc_last_field_${uuid0};
USE sc_last_field_${uuid0};
CREATE TABLE tab1 (
  c0 INT NULL,
  c1 STRUCT<v1 INT>
) ENGINE=OLAP
DUPLICATE KEY(c0)
DISTRIBUTED BY HASH(c0) BUCKETS 1
PROPERTIES (
  "compression" = "LZ4",
  "fast_schema_evolution" = "true",
  "replicated_storage" = "true",
  "replication_num" = "1"
);
ALTER TABLE tab1 MODIFY COLUMN c1 DROP FIELD v1;
