-- Test Objective:
-- 1. Preserve complex STRUCT add/drop field semantics from StarRocks.
-- 2. Verify existing rows are backfilled with NULL for newly added fields.
-- 3. Verify invalid nested field operations still fail with analyzer errors.
-- 4. Verify materialized view refresh still sees the evolved schema.
-- query 1
USE ${case_db};
CREATE TABLE tab1 (
  c0 INT NULL,
  c1 STRUCT<v1 INT, v2 STRUCT<v3 INT, v4 INT>>
) ENGINE=OLAP
DUPLICATE KEY(c0)
DISTRIBUTED BY HASH(c0) BUCKETS 1
PROPERTIES (
  "compression" = "LZ4",
  "fast_schema_evolution" = "true",
  "replicated_storage" = "true",
  "replication_num" = "1"
);
INSERT INTO tab1 VALUES (1, row(1, row(1, 1))), (2, row(2, row(2, 2)));
SELECT * FROM tab1;

-- query 2
-- @expect_error=Field v1 type INT is not valid
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD v1.v5 INT;

-- query 3
-- @expect_error=Field v2 is already exist
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD v2 INT;

-- query 4
-- @expect_error=Field v3 is already exist
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD v2.v3 INT;

-- query 5
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD val1 INT;
SET @a = sleep(2);
SELECT * FROM tab1;

-- query 6
USE ${case_db};
INSERT INTO tab1 VALUES (3, row(3, row(3, 3), 3));
SELECT * FROM tab1;

-- query 7
USE ${case_db};
CREATE MATERIALIZED VIEW mv1
  DISTRIBUTED BY HASH(c0) AS
  SELECT * FROM tab1;
SET @a = sleep(5);
SELECT count(*) FROM mv1;

-- query 8
USE ${case_db};
SELECT * FROM tab1;

-- query 9
-- @expect_error=Drop field v5 is not found
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 DROP FIELD v2.v5;

-- query 10
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 DROP FIELD v1;
SET @a = sleep(2);
SELECT * FROM tab1;

-- query 11
USE ${case_db};
ALTER TABLE tab1 MODIFY COLUMN c1 ADD FIELD v1 INT AFTER v2;
SET @a = sleep(2);
SELECT * FROM tab1;

-- query 12
USE ${case_db};
INSERT INTO tab1 VALUES (4, row(row(4, 4), 4, 4));
SELECT * FROM tab1;
