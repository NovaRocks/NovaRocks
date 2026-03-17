-- Migrated from dev/test/sql/test_function/T/test_iceberg_transform_func
-- Test Objective:
-- 1. Validate __iceberg_transform_truncate() on decimal column and literal.
-- 2. Validate __iceberg_transform_bucket() on decimal column and literal.

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t0 (c1 decimal(4,2))
    DUPLICATE KEY(c1)
    DISTRIBUTED BY HASH(c1) BUCKETS 1
    PROPERTIES ("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t0 VALUES (10.65);

-- query 3
USE ${case_db};
SELECT __iceberg_transform_truncate(c1, 50) FROM t0;

-- query 4
USE ${case_db};
SELECT __iceberg_transform_truncate(10.65, 50) FROM t0;

-- query 5
USE ${case_db};
SELECT __iceberg_transform_bucket(c1, 8) FROM t0;

-- query 6
USE ${case_db};
SELECT __iceberg_transform_bucket(10.65, 8) FROM t0;
