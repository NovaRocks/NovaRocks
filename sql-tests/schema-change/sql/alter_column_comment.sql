-- Test Objective:
-- 1. Validate ALTER TABLE MODIFY COLUMN ... COMMENT works for primary key tables.
-- 2. Validate ALTER TABLE MODIFY COLUMN ... COMMENT works for duplicate key tables.
-- 3. Verify SHOW CREATE TABLE reflects updated comments.
-- Migrated from: dev/test/sql/test_alter_table/T/test_alter_column_comment

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_comment_${uuid0} FORCE;
CREATE DATABASE sc_alter_comment_${uuid0};
USE sc_alter_comment_${uuid0};
CREATE TABLE t(k int, v int) PRIMARY KEY(k);

-- query 2
-- Verify initial column comments are empty.
-- @result_contains=COMMENT ""
-- @skip_result_check=true
USE sc_alter_comment_${uuid0};
SHOW CREATE TABLE t;

-- query 3
-- @skip_result_check=true
USE sc_alter_comment_${uuid0};
ALTER TABLE t MODIFY COLUMN k COMMENT 'k';
ALTER TABLE t MODIFY COLUMN v COMMENT 'v';

-- query 4
-- Verify updated comments on primary key table.
-- @result_contains=COMMENT "k"
-- @skip_result_check=true
USE sc_alter_comment_${uuid0};
SHOW CREATE TABLE t;

-- query 5
-- @skip_result_check=true
USE sc_alter_comment_${uuid0};
CREATE TABLE d(k int, v int) DUPLICATE KEY(k);

-- query 6
-- Verify initial column comments are empty on duplicate key table.
-- @result_contains=COMMENT ""
-- @skip_result_check=true
USE sc_alter_comment_${uuid0};
SHOW CREATE TABLE d;

-- query 7
-- @skip_result_check=true
USE sc_alter_comment_${uuid0};
ALTER TABLE d MODIFY COLUMN k COMMENT 'k';
ALTER TABLE d MODIFY COLUMN v COMMENT 'v';

-- query 8
-- Verify updated comments on duplicate key table.
-- @result_contains=COMMENT "k"
-- @skip_result_check=true
USE sc_alter_comment_${uuid0};
SHOW CREATE TABLE d;

-- query 9
-- @skip_result_check=true
DROP DATABASE IF EXISTS sc_alter_comment_${uuid0} FORCE;
