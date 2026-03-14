-- Test Objective:
-- 1. Validate that renaming a partition on an auto-partitioned table is not allowed.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_partition

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_part_${uuid0} FORCE;
CREATE DATABASE sc_alter_part_${uuid0};
USE sc_alter_part_${uuid0};
CREATE TABLE t(k datetime) PARTITION BY date_trunc('day',k);
INSERT INTO t VALUES('2020-01-01');

-- query 2
-- @expect_error=automatic partitioned
USE sc_alter_part_${uuid0};
ALTER TABLE t RENAME PARTITION p20200101 pp;

-- query 3
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_part_${uuid0} FORCE;
