-- @order_sensitive=true
-- Test Objective:
-- 1. Validate DROP PARTITION on list partitioned table.
-- 2. Verify RECOVER is not supported for list partitions.
-- 3. Verify DROP FORCE on list partition.

-- query 1
CREATE TABLE ${case_db}.t1(k1 VARCHAR(100) NOT NULL, v1 INT)
DUPLICATE KEY(k1)
PARTITION BY LIST(k1) (
  PARTITION p1 VALUES IN ('beijing', 'shanghai'),
  PARTITION p2 VALUES IN ('guangzhou'),
  PARTITION p3 VALUES IN ('shenzhen'),
  PARTITION p4 VALUES IN ('jinan','hefei')
)
DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO t1 VALUES('beijing', 1),('shanghai', 2),('shanghai', 3),('guangzhou', 4),('shenzhen', 5),('jinan', 6),('hefei', 7);
ALTER TABLE t1 DROP PARTITION p1;
SELECT * FROM t1 ORDER BY k1;

-- query 2
-- @expect_error=Does not support recover list partition
RECOVER PARTITION p1 FROM t1;

-- query 3
ALTER TABLE t1 DROP PARTITION p2 FORCE;
SELECT * FROM t1 ORDER BY k1;

-- query 4
-- @expect_error=No partition named 'p2' in recycle bin
RECOVER PARTITION p2 FROM t1;
