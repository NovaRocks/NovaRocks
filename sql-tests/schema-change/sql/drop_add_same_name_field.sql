-- Test Objective:
-- 1. Preserve drop-then-add-same-name semantics for nested struct fields.
-- 2. Verify old rows are backfilled to NULL after re-adding the field with a new type.
-- query 1
DROP DATABASE IF EXISTS sc_same_name_${uuid0} FORCE;
CREATE DATABASE sc_same_name_${uuid0};
USE sc_same_name_${uuid0};
CREATE TABLE t (
  c1 INT NULL,
  c2 STRUCT<v2_1 INT> NULL
) ENGINE=OLAP
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES (
  "compression" = "LZ4",
  "fast_schema_evolution" = "true",
  "replicated_storage" = "true",
  "replication_num" = "1"
);
INSERT INTO t VALUES (1, row(1)), (2, row(2));
ALTER TABLE t MODIFY COLUMN c2 ADD FIELD v2_2 STRING;
SET @a = sleep(2);
SELECT * FROM t;

-- query 2
USE sc_same_name_${uuid0};
INSERT INTO t VALUES (3, row(3, 'Beijing')), (4, row(4, 'Shanghai'));
ALTER TABLE t MODIFY COLUMN c2 DROP FIELD v2_2;
SET @a = sleep(2);
ALTER TABLE t MODIFY COLUMN c2 ADD FIELD v2_2 DATE;
SET @a = sleep(2);
SELECT * FROM t;
