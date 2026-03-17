-- Test Objective:
-- 1. Validate DELETE on DUPLICATE KEY table.
-- 2. Verify DELETE works correctly after RENAME COLUMN.

-- query 1
CREATE TABLE ${case_db}.test_delete_dup_rename (
    `name1` varchar(255)
) ENGINE=OLAP
DUPLICATE KEY(`name1`)
DISTRIBUTED BY HASH(`name1`) BUCKETS 2
PROPERTIES ("replication_num" = "1");
INSERT INTO test_delete_dup_rename VALUES ("mon"), ("tue");
SELECT name1 FROM test_delete_dup_rename;

-- query 2
DELETE FROM test_delete_dup_rename WHERE name1 = "mon";
SELECT name1 FROM test_delete_dup_rename;

-- query 3
-- @skip_result_check=true
ALTER TABLE test_delete_dup_rename RENAME COLUMN name1 TO name2;
INSERT INTO test_delete_dup_rename VALUES ("wed"), ("thu");

-- query 4
DELETE FROM test_delete_dup_rename WHERE name2 = "tue";
SELECT * FROM test_delete_dup_rename;

-- query 5
DELETE FROM test_delete_dup_rename WHERE name2 = "wed";
SELECT * FROM test_delete_dup_rename;
