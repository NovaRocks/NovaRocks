-- Migrated from dev/test/sql/test_datetime/T/test_datetime
-- Test Objective:
-- 1. Validate DATETIME stores sub-second fractional precision up to microseconds (6 digits).
-- 2. Validate DELETE by exact DATETIME value including sub-second comparisons.
-- 3. Cover: truncation of excess digits, zero-padding on display, same value via different literal forms.
-- Key semantics:
--   '2020-01-01 00:00:00'    == '2020-01-01 00:00:00.0' (both stored as 00:00:00)
--   '2020-01-01 00:00:00.1'  == '2020-01-01 00:00:00.100000'
--   '2020-01-01 00:00:00.123450' == '2020-01-01 00:00:00.12345' (trailing zero trimmed)

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_datetime;
CREATE TABLE ${case_db}.t_datetime (
    c1 int,
    c2 datetime
) DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 3
PROPERTIES("replication_num" = "1");
INSERT INTO ${case_db}.t_datetime VALUES
(1, '2020-01-01 00:00:00'),
(2, '2020-01-01 00:00:00.0'),
(3, '2020-01-01 00:00:00.01'),
(4, '2020-01-01 00:00:00.012'),
(5, '2020-01-01 00:00:00.0123'),
(6, '2020-01-01 00:00:00.01234'),
(7, '2020-01-01 00:00:00.012345'),
(8, '2020-01-01 00:00:00.1'),
(9, '2020-01-01 00:00:00.12'),
(10, '2020-01-01 00:00:00.123'),
(11, '2020-01-01 00:00:00.1234'),
(12, '2020-01-01 00:00:00.12345'),
(13, '2020-01-01 00:00:00.123450');

-- query 2
-- @order_sensitive=true
-- c1=1,2 both display as '00:00:00'; c1=3..13 show padded microseconds
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 3
-- @skip_result_check=true
-- Delete by exact datetime: '00:00:00' matches c1=1 and c1=2 (same value after normalization)
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00';
-- '00:00:00.0' is same as '00:00:00'; rows already gone
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.0';
-- Delete c1=4: '00:00:00.012' → stored as '00:00:00.012000'
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.012';

-- query 4
-- @order_sensitive=true
-- Remaining: c1=3,5,6,7,8,9,10,11,12,13 (c1=1,2,4 deleted)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 5
-- @skip_result_check=true
-- Delete c1=8 ('00:00:00.1' = '00:00:00.100000')
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.1';
-- Delete c1=10 ('00:00:00.123' = '00:00:00.123000')
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.123';
-- Delete c1=12,13 ('00:00:00.123450' = '00:00:00.12345' with trailing zero)
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.123450';

-- query 6
-- @order_sensitive=true
-- Final remaining: c1=3,5,6,7,9,11 (6 rows)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;
