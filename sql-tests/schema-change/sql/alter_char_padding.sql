-- Test Objective:
-- 1. Verify char column data is correctly queryable before and after schema change.
-- 2. Validate that MODIFY COLUMN CHAR(4) -> CHAR(10) completes via non-fast schema evolution.
-- 3. Verify the WHERE clause on the char column still works after padding change.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_alter_table_char_padding)

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE test3
(
    k1 BIGINT,
    v1 LARGEINT,
    v2 CHAR(4)
)
ENGINE=olap
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH (k1) BUCKETS 3
PROPERTIES(
    "replication_num"="1"
);
INSERT INTO test3 VALUES (1, 1, 'a');

-- query 2
-- Verify data is accessible before schema change.
-- @order_sensitive=true
USE ${case_db};
SELECT * FROM test3 WHERE v2 = 'a';

-- query 3
-- Trigger non-fast schema evolution: CHAR(4) -> CHAR(10).
-- @skip_result_check=true
USE ${case_db};
ALTER TABLE test3 MODIFY COLUMN v2 CHAR(10);

-- query 4
-- Wait for the schema change job to complete.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE ${case_db};
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY CreateTime DESC LIMIT 1;

-- query 5
-- Verify data is still accessible after schema change. Retry because the table
-- state may still be transitioning even after SHOW ALTER reports FINISHED.
-- @retry_count=30
-- @retry_interval_ms=2000
-- @order_sensitive=true
USE ${case_db};
SELECT * FROM test3 WHERE v2 = 'a';

-- query 6
-- @skip_result_check=true
