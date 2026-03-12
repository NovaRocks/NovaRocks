-- Test Objective:
-- 1. Preserve the StarRocks count(*) regression coverage for ADD COLUMN.
-- 2. Verify count(*) remains stable before and after schema change completion.
-- 3. Verify the new column is visible with NULL backfill after schema change.
-- query 1
ADMIN SET FRONTEND CONFIG ("enable_fast_schema_evolution"="false");
DROP DATABASE IF EXISTS sc_add_col_${uuid0} FORCE;
CREATE DATABASE sc_add_col_${uuid0};
USE sc_add_col_${uuid0};
CREATE TABLE t0(
    k1 INT,
    c1 INT
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;
INSERT INTO t0 VALUES (1, 1);
SELECT count(*) FROM t0;

-- query 2
USE sc_add_col_${uuid0};
SELECT * FROM t0 ORDER BY k1;

-- query 3
USE sc_add_col_${uuid0};
ALTER TABLE t0 ADD COLUMN b1 BOOLEAN;
SELECT count(*) FROM t0;

-- query 4
USE sc_add_col_${uuid0};
SET @a = sleep(2);
SELECT count(*) FROM t0;

-- query 5
USE sc_add_col_${uuid0};
SELECT count(*) FROM t0;

-- query 6
USE sc_add_col_${uuid0};
SELECT count(*) FROM t0;

-- query 7
USE sc_add_col_${uuid0};
SET @a = sleep(2);
SELECT count(*) FROM t0;

-- query 8
USE sc_add_col_${uuid0};
SELECT count(*) FROM t0;

-- query 9
USE sc_add_col_${uuid0};
SELECT count(*) FROM t0;

-- query 10
USE sc_add_col_${uuid0};
SELECT * FROM t0 ORDER BY k1;
ADMIN SET FRONTEND CONFIG ("enable_fast_schema_evolution"="true");
