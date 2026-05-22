-- @tags=iceberg_dml,datetime,delete
-- Test Objective:
-- 1. Validate DATETIME stores sub-second fractional precision up to microseconds (6 digits).
-- 2. Validate DELETE by exact DATETIME value including sub-second comparisons on an Iceberg v3 table.
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
) TBLPROPERTIES ("format-version" = "3");
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
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 3
-- @skip_result_check=true
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00';
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.0';
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.012';

-- query 4
-- @order_sensitive=true
-- Remaining: c1=3,5,6,7,8,9,10,11,12,13 (c1=1,2,4 deleted)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;

-- query 5
-- @skip_result_check=true
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.1';
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.123';
DELETE FROM ${case_db}.t_datetime WHERE c2 = '2020-01-01 00:00:00.123450';

-- query 6
-- @order_sensitive=true
-- Final remaining: c1=3,5,6,7,9,11 (6 rows)
SELECT * FROM ${case_db}.t_datetime ORDER BY c1;
