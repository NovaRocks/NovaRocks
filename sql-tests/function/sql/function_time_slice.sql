-- Migrated from dev/test/sql/test_function/T/test_time_slice
-- Test Objective:
-- 1. Validate time_slice with ceil mode at max datetime boundary (9999-12-31 23:59:59).
-- 2. Validate time_slice with very large intervals (INT32_MAX) for all granularities.
-- 3. Validate time_slice rejects dates before 0001-01-01 00:00:00.
-- 4. Validate date_slice rejects non-integer interval values (3.2, -3.2).
-- 5. Cover both fold-constants-off and fold-constants-on modes.
-- 6. Validate millisecond and microsecond granularity slicing.

-- query 1
-- disable_function_fold_constants=off: ceil mode with max datetime
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 year, ceil);

-- query 2
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 month, ceil);

-- query 3
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 day, ceil);

-- query 4
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 quarter, ceil);

-- query 5
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 week, ceil);

-- query 6
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 hour, ceil);

-- query 7
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 minute, ceil);

-- query 8
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31 23:59:59',interval 5 second, ceil);

-- query 9
-- disable_function_fold_constants=off: large intervals
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 year);

-- query 10
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 month);

-- query 11
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 day);

-- query 12
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 quarter);

-- query 13
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 week);

-- query 14
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 hour);

-- query 15
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 minute);

-- query 16
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 second);

-- query 17
-- disable_function_fold_constants=off: date before 0001-01-01
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('0000-01-01',interval 5 year);

-- query 18
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('0000-01-01',interval 5 month);

-- query 19
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('0000-01-01',interval 5 day);

-- query 20
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('0000-01-01',interval 5 quarter);

-- query 21
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('0000-01-01',interval 5 week);

-- query 22
-- disable_function_fold_constants=off: ceil mode with max date
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31',interval 5 year, ceil);

-- query 23
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31',interval 5 month, ceil);

-- query 24
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31',interval 5 day, ceil);

-- query 25
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31',interval 5 quarter, ceil);

-- query 26
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('9999-12-31',interval 5 week, ceil);

-- query 27
-- disable_function_fold_constants=off: large intervals (date input, repeated)
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 year);

-- query 28
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 month);

-- query 29
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 day);

-- query 30
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 quarter);

-- query 31
USE ${case_db};
set disable_function_fold_constants=off;
select time_slice('2023-12-31 03:12:04',interval 2147483647 week);

-- query 32
-- disable_function_fold_constants=off: date_slice with non-integer intervals (should error)
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=off;
select date_slice('2023-12-31 03:12:04',interval 3.2 year);

-- query 33
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=off;
select date_slice('2023-12-31 03:12:04',interval 3.2 month);

-- query 34
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=off;
select date_slice('2023-12-31 03:12:04',interval 3.2 day);

-- query 35
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=off;
select date_slice('2023-12-31 03:12:04',interval 3.2 quarter);

-- query 36
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=off;
select date_slice('2023-12-31 03:12:04',interval 3.2 week);

-- query 37
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=off;
select date_slice('2023-12-31 03:12:04',interval -3.2 week);

-- query 38
-- disable_function_fold_constants=on: ceil mode with max datetime
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 year, ceil);

-- query 39
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 month, ceil);

-- query 40
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 day, ceil);

-- query 41
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 quarter, ceil);

-- query 42
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 week, ceil);

-- query 43
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 hour, ceil);

-- query 44
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 minute, ceil);

-- query 45
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31 23:59:59',interval 5 second, ceil);

-- query 46
-- disable_function_fold_constants=on: large intervals
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 year);

-- query 47
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 month);

-- query 48
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 day);

-- query 49
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 quarter);

-- query 50
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 week);

-- query 51
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 hour);

-- query 52
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 minute);

-- query 53
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 second);

-- query 54
-- disable_function_fold_constants=on: date before 0001-01-01
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('0000-01-01',interval 5 year);

-- query 55
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('0000-01-01',interval 5 month);

-- query 56
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('0000-01-01',interval 5 day);

-- query 57
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('0000-01-01',interval 5 quarter);

-- query 58
-- @expect_error=time used with time_slice can't before 0001-01-01 00:00:00
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('0000-01-01',interval 5 week);

-- query 59
-- disable_function_fold_constants=on: ceil mode with max date
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31',interval 5 year, ceil);

-- query 60
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31',interval 5 month, ceil);

-- query 61
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31',interval 5 day, ceil);

-- query 62
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31',interval 5 quarter, ceil);

-- query 63
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('9999-12-31',interval 5 week, ceil);

-- query 64
-- disable_function_fold_constants=on: large intervals (repeated)
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 year);

-- query 65
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 month);

-- query 66
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 day);

-- query 67
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 quarter);

-- query 68
USE ${case_db};
set disable_function_fold_constants=on;
select time_slice('2023-12-31 03:12:04',interval 2147483647 week);

-- query 69
-- disable_function_fold_constants=on: date_slice with non-integer intervals (should error)
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=on;
select date_slice('2023-12-31 03:12:04',interval 3.2 year);

-- query 70
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=on;
select date_slice('2023-12-31 03:12:04',interval 3.2 month);

-- query 71
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=on;
select date_slice('2023-12-31 03:12:04',interval 3.2 day);

-- query 72
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=on;
select date_slice('2023-12-31 03:12:04',interval 3.2 quarter);

-- query 73
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=on;
select date_slice('2023-12-31 03:12:04',interval 3.2 week);

-- query 74
-- @expect_error=date_slice requires second parameter must be a constant interval
USE ${case_db};
set disable_function_fold_constants=on;
select date_slice('2023-12-31 03:12:04',interval -3.2 week);

-- query 75
-- Millisecond granularity tests
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1 millisecond);

-- query 76
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1 millisecond, ceil);

-- query 77
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 17 millisecond);

-- query 78
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 17 millisecond, ceil);

-- query 79
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000 millisecond);

-- query 80
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000 millisecond, ceil);

-- query 81
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1001 millisecond);

-- query 82
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1001 millisecond, ceil);

-- query 83
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 86400000 millisecond);

-- query 84
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 86400000 millisecond, ceil);

-- query 85
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 172800000 millisecond);

-- query 86
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 172800000 millisecond, ceil);

-- query 87
-- Microsecond granularity tests
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1 microsecond);

-- query 88
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1 microsecond, ceil);

-- query 89
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 17 microsecond);

-- query 90
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 17 microsecond, ceil);

-- query 91
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000 microsecond);

-- query 92
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000 microsecond, ceil);

-- query 93
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1001 microsecond);

-- query 94
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1001 microsecond, ceil);

-- query 95
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000000 microsecond);

-- query 96
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000000 microsecond, ceil);

-- query 97
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000001 microsecond);

-- query 98
USE ${case_db};
select time_slice('2023-10-31 23:59:59',interval 1000001 microsecond, ceil);
