-- Test Objective:
-- 1. Validate various invalid ALTER TABLE operations on a unique key table.
-- 2. Covers: add column with aggregation, add duplicate columns, drop key column, etc.
-- 3. Verify DESC and SELECT remain correct after failed operations.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_alter_unique_table_abnormal)

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_uniq_abn_${uuid0} FORCE;
CREATE DATABASE sc_alter_uniq_abn_${uuid0};
USE sc_alter_uniq_abn_${uuid0};
CREATE TABLE t1(k0 BIGINT, k1 DATETIME, v0 BIGINT, v1 VARCHAR(100))
 unique key(k0, k1)
 distributed by hash(k0) buckets 1
 properties('replication_num'='1');
INSERT INTO t1 VALUES(0, '2024-01-01 00:00:00', 10, '100');

-- query 2
-- Cannot add key column with SUM aggregation.
-- @expect_error=Column definition is wrong
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 ADD column k2 SMALLINT KEY SUM;

-- query 3
-- Cannot assign aggregation method on column in Unique table.
-- @expect_error=Can not assign aggregation method
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 ADD column v2 BIGINT MIN;

-- query 4
-- @expect_error=Repeatedly add same column
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 ADD COLUMN v2 BIGINT, ADD COLUMN v2 FLOAT;

-- query 5
-- @expect_error=Repeatedly add same column
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 ADD COLUMN v2 BIGINT, ADD COLUMN v2 BIGINT KEY;

-- query 6
-- Add + drop same column in one statement succeeds.
-- @skip_result_check=true
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 ADD COLUMN v2 BIGINT, DROP COLUMN v2;

-- query 7
-- Verify table structure.
USE sc_alter_uniq_abn_${uuid0};
DESC t1;

-- query 8
-- @expect_error=already exists
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 ADD COLUMN v2 BIGINT, ADD COLUMN v0 BIGINT;

-- query 9
-- Cannot drop key column in Unique table.
-- @expect_error=Can not drop key column
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 DROP COLUMN k1;

-- query 10
-- @expect_error=Can not drop key column
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 DROP COLUMN v0, DROP COLUMN k1;

-- query 11
-- @expect_error=Column does not exists
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 DROP COLUMN v0, DROP COLUMN v100;

-- query 12
-- Cannot assign aggregation method on Unique table.
-- @expect_error=Can not assign aggregation method
USE sc_alter_uniq_abn_${uuid0};
ALTER TABLE t1 MODIFY COLUMN v1 VARCHAR(100) MAX;

-- query 13
-- Verify table structure is intact.
USE sc_alter_uniq_abn_${uuid0};
DESC t1;

-- query 14
-- Verify data is intact.
USE sc_alter_uniq_abn_${uuid0};
SELECT * from t1;

-- query 15
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_uniq_abn_${uuid0} FORCE;
