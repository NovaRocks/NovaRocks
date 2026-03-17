-- @order_sensitive=true
-- Test Objective:
-- 1. Validate DROP PARTITION on range partitioned table.
-- 2. Verify RECOVER PARTITION restores dropped partition and data.
-- 3. Verify DROP PARTITION FORCE prevents recovery.
-- 4. Verify error on DROP/RECOVER non-existent partition.

-- query 1
CREATE TABLE ${case_db}.t0(k1 DATE, v1 INT)
DUPLICATE KEY(k1)
PARTITION BY RANGE(k1) (
  PARTITION p1 VALUES LESS THAN('2024-01-01'),
  PARTITION p2 VALUES LESS THAN('2024-02-01'),
  PARTITION p3 VALUES LESS THAN('2024-03-01'),
  PARTITION p4 VALUES LESS THAN('2024-04-01')
)
DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO t0 VALUES('2023-12-31', 0),('2024-01-01', 1),('2024-01-31', 2),('2024-02-02', 3),('2024-03-03', 4);
ALTER TABLE t0 DROP PARTITION p1;
SELECT * FROM t0 ORDER BY k1;

-- query 2
RECOVER PARTITION p1 FROM t0;
SELECT * FROM t0 ORDER BY k1;

-- query 3
INSERT INTO t0 VALUES('2023-12-01', 10);
SELECT * FROM t0 ORDER BY k1;

-- query 4
ALTER TABLE t0 DROP PARTITION p2 FORCE;
SELECT * FROM t0 ORDER BY k1;

-- query 5
-- @expect_error=No partition named 'p2' in recycle bin
RECOVER PARTITION p2 FROM t0;

-- query 6
SELECT * FROM t0 ORDER BY k1;

-- query 7
-- @expect_error=Error in list of partitions
ALTER TABLE t0 DROP PARTITION pxxxx;

-- query 8
-- @expect_error=No partition named 'pyyyy' in recycle bin
RECOVER PARTITION pyyyy FROM t0;
