-- Test Objective:
-- 1. Validate adding a bitmap index on a primary key table does not break CHAR column queries.
-- 2. Verify SELECT with CHAR filter returns correct results before and after index creation.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_pk_alter_char)

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_pk_char_idx_${uuid0} FORCE;
CREATE DATABASE sc_pk_char_idx_${uuid0};
USE sc_pk_char_idx_${uuid0};
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
USE sc_pk_char_idx_${uuid0};
SELECT * FROM test2 WHERE v2 = '1';

-- query 3
-- Add bitmap index on v1.
-- @skip_result_check=true
USE sc_pk_char_idx_${uuid0};
ALTER TABLE test2 ADD INDEX test_bitmap(v1);

-- query 4
-- Wait for index creation to finish.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE sc_pk_char_idx_${uuid0};
SHOW ALTER TABLE COLUMN FROM sc_pk_char_idx_${uuid0} ORDER BY CreateTime DESC LIMIT 1;

-- query 5
-- Verify char filter still works after index creation.
USE sc_pk_char_idx_${uuid0};
SELECT * FROM test2 WHERE v2 = '1';

-- query 6
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_pk_char_idx_${uuid0} FORCE;
