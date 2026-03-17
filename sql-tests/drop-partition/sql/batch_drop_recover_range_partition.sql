-- @order_sensitive=true
-- Test Objective:
-- 1. Validate batch DROP PARTITIONS with START/END/EVERY syntax.
-- 2. Verify RECOVER after batch drop.
-- 3. Verify batch DROP FORCE prevents recovery.

-- query 1
CREATE TABLE ${case_db}.t0(k1 date, v1 int)
DUPLICATE KEY(k1)
PARTITION BY RANGE(k1) (
  PARTITION p20240401 VALUES [('2024-04-01'), ('2024-04-02')),
  PARTITION p20240402 VALUES [('2024-04-02'), ('2024-04-03')),
  PARTITION p20240403 VALUES [('2024-04-03'), ('2024-04-04')),
  PARTITION p20240404 VALUES [('2024-04-04'), ('2024-04-05')),
  PARTITION p20240405 VALUES [('2024-04-05'), ('2024-04-06'))
)
DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES('replication_num'='1');
INSERT INTO t0 VALUES('2024-04-01', 0),('2024-04-02', 1),('2024-04-03', 2),('2024-04-04', 3),('2024-04-05', 4);
ALTER TABLE t0 DROP PARTITIONS IF EXISTS START("2024-04-01") END("2024-04-03") EVERY (INTERVAL 1 DAY);
SELECT * FROM t0 ORDER BY k1;

-- query 2
RECOVER PARTITION p20240401 FROM t0;
RECOVER PARTITION p20240402 FROM t0;
SELECT * FROM t0 ORDER BY k1;

-- query 3
INSERT INTO t0 VALUES('2024-04-01', 10);
SELECT * FROM t0 ORDER BY k1;

-- query 4
ALTER TABLE t0 DROP PARTITIONS IF EXISTS START("2024-04-01") END("2024-04-03") EVERY (INTERVAL 1 DAY) FORCE;
SELECT * FROM t0 ORDER BY k1;

-- query 5
-- @expect_error=No partition named 'p20240401' in recycle bin
RECOVER PARTITION p20240401 FROM t0;

-- query 6
-- @expect_error=No partition named 'p20240402' in recycle bin
RECOVER PARTITION p20240402 FROM t0;

-- query 7
SELECT * FROM t0 ORDER BY k1;

-- query 8
-- @expect_error=Error in list of partitions
ALTER TABLE t0 DROP PARTITION pxxxx;

-- query 9
-- @expect_error=No partition named 'pyyyy' in recycle bin
RECOVER PARTITION pyyyy FROM t0;
