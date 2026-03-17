-- Migrated from dev/test/sql/test_function/T/test_date_format
-- Test Objective:
-- 1. Validate DATE_FORMAT with microsecond format specifier (%f) on varchar-stored datetime values.
-- 2. Validate LENGTH of DATE_FORMAT output with long literal strings in format.
-- 3. Validate IFNULL wrapping of LENGTH(DATE_FORMAT(...)) for edge-case format strings.

-- query 1
-- Setup: create table and insert test data
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE IF NOT EXISTS test_order (
    id varchar(150) NOT NULL COMMENT '',
    reset_period_data varchar(32) NULL COMMENT ""
) ENGINE=olap PRIMARY KEY (id) COMMENT ''
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("enable_persistent_index" = "true", "replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_order VALUES('1','2023-10-11 00:00:01.030');

-- query 3
-- DATE_FORMAT with %f microsecond specifier
USE ${case_db};
SELECT DATE_FORMAT(reset_period_data, '%Y-%m-%d %H:%i:%s.%f') FROM test_order WHERE id='1';

-- query 4
-- LENGTH of DATE_FORMAT with long literal padding in format string
USE ${case_db};
SELECT LENGTH(DATE_FORMAT(reset_period_data, '%Y-%m-%d %H:%i:%s.tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt%f'))
FROM test_order WHERE id='1';

-- query 5
-- IFNULL wrapping LENGTH of DATE_FORMAT with extra-long literal in format
USE ${case_db};
SELECT IFNULL(LENGTH(DATE_FORMAT(reset_period_data, '%Y-%m-%d %H:%i:%s.tttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt%f')),-1)
FROM test_order WHERE id='1';

-- query 6
-- IFNULL wrapping LENGTH of DATE_FORMAT with even longer literal and trailing chars after %f
USE ${case_db};
SELECT IFNULL(LENGTH(DATE_FORMAT(reset_period_data, '%Y-%m-%d %H:%i:%s.ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt%ftttttttt')),-1)
FROM test_order WHERE id='1';
