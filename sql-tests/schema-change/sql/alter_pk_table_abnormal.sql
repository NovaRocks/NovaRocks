-- Test Objective:
-- 1. Validate various invalid ALTER TABLE operations on a primary key table.
-- 2. Covers: add duplicate column, drop key column, modify key/sort column,
--    add column with KEY, add duplicate columns, add+drop same column, etc.
-- 3. Verify DESC and SELECT remain correct after failed operations.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_table_abnormal (test_alter_pk_table_abnormal)

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_pk_abn_${uuid0} FORCE;
CREATE DATABASE sc_alter_pk_abn_${uuid0};
USE sc_alter_pk_abn_${uuid0};
CREATE TABLE t0(k0 BIGINT, k1 DATETIME, v0 INT, v1 VARCHAR(100))
 primary key(k0, k1)
 distributed by hash(k0)
 buckets 1
 order by (v0)
 properties('replication_num'='1');
INSERT INTO t0 VALUES(0, '2024-01-01 00:00:00', 10, '100');

-- query 2
-- Cannot add column with same name as existing column.
-- @expect_error=already exists
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD COLUMN k0 FLOAT;

-- query 3
-- @expect_error=already exists
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD COLUMN v0 BIGINT;

-- query 4
-- Cannot drop key column in primary key table.
-- @expect_error=Can not drop key column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 DROP COLUMN k1;

-- query 5
-- @expect_error=Can not drop key column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 DROP COLUMN k0;

-- query 6
-- Cannot modify key column in primary key table.
-- @expect_error=Can not modify key column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 MODIFY COLUMN k0 INT;

-- query 7
-- @expect_error=Can not modify key column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 MODIFY COLUMN k0 LARGEINT;

-- query 8
-- Cannot modify sort column in primary key table.
-- @expect_error=Can not modify sort column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 MODIFY COLUMN v0 BIGINT;

-- query 9
-- Multi-column modify including sort column fails.
-- @expect_error=Can not modify sort column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 MODIFY COLUMN v1 VARCHAR(200), MODIFY COLUMN v0 BIGINT;

-- query 10
-- Modify with aggregation function succeeds for non-key non-sort column (MAX on v1).
-- @skip_result_check=true
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 MODIFY COLUMN v1 VARCHAR(100) MAX;

-- query 11
-- Cannot add column with KEY attribute.
-- @expect_error=Column definition is wrong
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD column k2 SMALLINT KEY;

-- query 12
-- Cannot add two columns with same name but different types.
-- @expect_error=Repeatedly add same column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD COLUMN v2 BIGINT, ADD COLUMN v2 FLOAT;

-- query 13
-- @expect_error=Column definition is wrong
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD COLUMN v2 BIGINT, ADD COLUMN v2 BIGINT KEY;

-- query 14
-- @expect_error=Column definition is wrong
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD COLUMN v2 BIGINT, ADD COLUMN k2 BIGINT KEY;

-- query 15
-- Add + drop same column in one statement succeeds.
-- @skip_result_check=true
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD COLUMN v2 BIGINT, DROP COLUMN v2;

-- query 16
-- Verify table structure unchanged after all invalid operations.
USE sc_alter_pk_abn_${uuid0};
DESC t0;

-- query 17
-- @expect_error=already exists
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 ADD COLUMN v2 BIGINT, ADD COLUMN v0 BIGINT;

-- query 18
-- @expect_error=Can not drop key column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 DROP COLUMN k0;

-- query 19
-- @expect_error=Can not drop key column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 DROP COLUMN v1, DROP COLUMN k0;

-- query 20
-- Cannot drop sort column.
-- @expect_error=Can not drop sort column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 DROP COLUMN v0;

-- query 21
-- @expect_error=Can not drop sort column
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 DROP COLUMN v1, DROP COLUMN v0;

-- query 22
-- @expect_error=Column does not exists
USE sc_alter_pk_abn_${uuid0};
ALTER TABLE t0 DROP COLUMN v1, DROP COLUMN v100;

-- query 23
-- Verify table structure still intact.
USE sc_alter_pk_abn_${uuid0};
DESC t0;

-- query 24
-- Verify data is intact.
USE sc_alter_pk_abn_${uuid0};
SELECT * from t0;

-- query 25
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_pk_abn_${uuid0} FORCE;
