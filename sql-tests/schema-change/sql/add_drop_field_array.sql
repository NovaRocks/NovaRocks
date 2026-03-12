-- Test Objective:
-- 1. Preserve ARRAY<STRUCT> add/drop field coverage from StarRocks.
-- 2. Verify invalid field targets still fail with analyzer errors.
-- 3. Verify backfill and re-add semantics across array element structs.
-- 4. Verify materialized view refresh still observes evolved array rows.
-- query 1
DROP DATABASE IF EXISTS sc_array_${uuid0} FORCE;
CREATE DATABASE sc_array_${uuid0};
USE sc_array_${uuid0};
CREATE TABLE tab1 (
  c0 INT NULL,
  c1 ARRAY<STRUCT<v1 INT, v2 INT>>
) ENGINE=OLAP
DUPLICATE KEY(c0)
DISTRIBUTED BY HASH(c0) BUCKETS 1
PROPERTIES (
  "compression" = "LZ4",
  "fast_schema_evolution" = "true",
  "replicated_storage" = "true",
  "replication_num" = "1"
);
INSERT INTO tab1 VALUES
  (1, [row(1, 1), row(1, 2)]),
  (2, [row(2, 1), row(2, 2)]);
SELECT * FROM tab1;

-- query 2
-- @expect_error=Target Field is not struct
USE sc_array_${uuid0};
ALTER TABLE tab1 MODIFY COLUMN c1 DROP FIELD [*];

-- query 3
-- @expect_error=Drop field v3 is not found
USE sc_array_${uuid0};
ALTER TABLE tab1 MODIFY COLUMN c1 DROP FIELD [*].v3;

-- query 4
-- @expect_error=Target Field is not struct
USE sc_array_${uuid0};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD val1 INT;

-- query 5
USE sc_array_${uuid0};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD [*].val1 INT;
SET @a = sleep(2);
SELECT * FROM tab1;

-- query 6
USE sc_array_${uuid0};
INSERT INTO tab1 VALUES (3, [row(3, 1, 1), row(3, 2, 1)]);
SELECT * FROM tab1;

-- query 7
USE sc_array_${uuid0};
CREATE MATERIALIZED VIEW mv1
  DISTRIBUTED BY HASH(c0) AS
  SELECT * FROM tab1;
SET @a = sleep(5);
SELECT count(*) FROM mv1;

-- query 8
USE sc_array_${uuid0};
SELECT * FROM tab1;

-- query 9
USE sc_array_${uuid0};
ALTER TABLE tab1 MODIFY COLUMN c1 DROP FIELD [*].v1;
SET @a = sleep(2);
SELECT * FROM tab1;

-- query 10
USE sc_array_${uuid0};
INSERT INTO tab1 VALUES (4, [row(4, 4), row(4, 5)]);
SELECT * FROM tab1;

-- query 11
USE sc_array_${uuid0};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD [*].v1 INT;
SET @a = sleep(2);
SELECT * FROM tab1;

-- query 12
USE sc_array_${uuid0};
INSERT INTO tab1 VALUES (5, [row(5, 5, 5), row(5, 6, 6)]);
SELECT * FROM tab1;
