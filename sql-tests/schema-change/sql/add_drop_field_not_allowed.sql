-- Test Objective:
-- 1. Preserve coverage for add/drop field under a table that declares fast_schema_evolution=false.
-- 2. NovaRocks intentionally does not enforce disabling fast schema evolution, so these operations should still succeed.
-- query 1
USE ${case_db};
CREATE TABLE tab1 (
  c0 INT NULL,
  c1 ARRAY<STRUCT<v1 INT, v2 INT>>
) ENGINE=OLAP
DUPLICATE KEY(c0)
DISTRIBUTED BY HASH(c0) BUCKETS 1
PROPERTIES (
  "compression" = "LZ4",
  "fast_schema_evolution" = "false",
  "replicated_storage" = "true",
  "replication_num" = "1"
);
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD [*].val1 INT;

-- query 2
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 DROP FIELD [*].v1;
