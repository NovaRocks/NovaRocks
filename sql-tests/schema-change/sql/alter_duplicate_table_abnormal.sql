-- Test Objective:
-- 1. Validate various invalid ALTER TABLE operations on a duplicate key table.
-- 2. Covers: add column with aggregation, add duplicate columns, drop non-existent column, etc.
-- 3. Verify DESC and SELECT remain correct after failed operations.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_alter_duplicate_table_abnormal)

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_dup_abn_${uuid0} FORCE;
CREATE DATABASE sc_alter_dup_abn_${uuid0};
USE sc_alter_dup_abn_${uuid0};
CREATE TABLE t2(k0 BIGINT, k1 DATETIME, v0 BIGINT, v1 VARCHAR(100))
 duplicate key(k0, k1)
 distributed by hash(k0) buckets 1
 properties('replication_num'='1');
INSERT INTO t2 VALUES(0, '2024-01-01 00:00:00', 10, '100');

-- query 2
-- Cannot add key column with SUM aggregation.
-- @expect_error=Column definition is wrong
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 ADD column k2 SMALLINT KEY SUM;

-- query 3
-- Cannot assign aggregation method on Duplicate table.
-- @expect_error=Can not assign aggregation method
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 ADD column v2 BIGINT MIN;

-- query 4
-- @expect_error=Repeatedly add same column
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 ADD COLUMN v2 BIGINT, ADD COLUMN v2 FLOAT;

-- query 5
-- @expect_error=Repeatedly add same column
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 ADD COLUMN v2 BIGINT, ADD COLUMN v2 BIGINT KEY;

-- query 6
-- Add + drop same column in one statement succeeds.
-- @skip_result_check=true
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 ADD COLUMN v2 BIGINT, DROP COLUMN v2;

-- query 7
-- @expect_error=already exists
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 ADD COLUMN v2 BIGINT, ADD COLUMN v0 BIGINT;

-- query 8
-- @expect_error=Column does not exists
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 DROP COLUMN v0, DROP COLUMN v100;

-- query 9
-- Cannot assign aggregation method on Duplicate table.
-- @expect_error=Can not assign aggregation method
USE sc_alter_dup_abn_${uuid0};
ALTER TABLE t2 MODIFY COLUMN v1 VARCHAR(100) MAX;

-- query 10
-- Verify table structure is intact.
USE sc_alter_dup_abn_${uuid0};
DESC t2;

-- query 11
-- Verify data is intact.
USE sc_alter_dup_abn_${uuid0};
SELECT * from t2;

-- query 12
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_dup_abn_${uuid0} FORCE;
