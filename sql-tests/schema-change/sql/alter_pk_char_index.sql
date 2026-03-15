-- Test Objective:
-- 1. Validate adding a bitmap index on a primary key table does not break CHAR column queries.
-- 2. Verify SELECT with CHAR filter returns correct results before and after index creation.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_pk_alter_char)

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE test2
(
    k1 BIGINT,
    v1 LARGEINT,
    v2 CHAR(4)
)
ENGINE=olap
PRIMARY KEY(k1)
DISTRIBUTED BY HASH (k1) BUCKETS 3
PROPERTIES(
    "replication_num"="1"
);
INSERT INTO test2 VALUES (1, 1, '1');

-- query 2
-- Verify char filter works before adding index.
USE ${case_db};
SELECT * FROM test2 WHERE v2 = '1';

-- query 3
-- Add bitmap index on v1.
-- @skip_result_check=true
USE ${case_db};
ALTER TABLE test2 ADD INDEX test_bitmap(v1);

-- query 4
-- Wait for index creation to finish.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE ${case_db};
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY CreateTime DESC LIMIT 1;

-- query 5
-- Verify char filter still works after index creation.
USE ${case_db};
SELECT * FROM test2 WHERE v2 = '1';

-- query 6
-- @skip_result_check=true
