-- Test Objective:
-- 1. Preserve the StarRocks count(*) regression coverage for ADD COLUMN.
-- 2. Verify count(*) remains stable before and after schema change completion.
-- 3. Verify the new column is visible with NULL backfill after schema change.
-- query 1
ADMIN SET FRONTEND CONFIG ("enable_fast_schema_evolution"="false");
USE ${case_db};
CREATE TABLE t0(
    k1 INT,
    c1 INT
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 3;
INSERT INTO t0 VALUES (1, 1);
SELECT count(*) FROM t0;

-- query 2
USE ${case_db};
SELECT * FROM t0 ORDER BY k1;

-- query 3
USE ${case_db};
ALTER TABLE t0 ADD COLUMN b1 BOOLEAN;
SELECT count(*) FROM t0;

-- query 4
USE ${case_db};
SET @a = sleep(2);
SELECT count(*) FROM t0;

-- query 5
USE ${case_db};
SELECT count(*) FROM t0;

-- query 6
USE ${case_db};
SELECT count(*) FROM t0;

-- query 7
USE ${case_db};
SET @a = sleep(2);
SELECT count(*) FROM t0;

-- query 8
USE ${case_db};
SELECT count(*) FROM t0;

-- query 9
USE ${case_db};
SELECT count(*) FROM t0;

-- query 10
USE ${case_db};
SELECT * FROM t0 ORDER BY k1;
ADMIN SET FRONTEND CONFIG ("enable_fast_schema_evolution"="true");
