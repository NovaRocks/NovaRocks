-- Migrated from dev/test/sql/test_datetime/T/test_to_datetime
-- Test Objective:
-- 1. Validate to_datetime(ts, scale) converts Unix timestamps to DATETIME in the session timezone.
-- 2. Validate scale=0 (seconds), scale=3 (milliseconds), scale=6 (microseconds).
-- 3. Validate default call to_datetime(ts) without scale uses scale=0.
-- 4. Validate invalid scale (>6) returns NULL.
-- 5. Validate NULL input handling.
-- 6. Validate dynamic scale via inline VALUES table.
-- Results are timezone-aware: SET time_zone='Asia/Shanghai' (UTC+8).

-- query 1
-- @skip_result_check=true
set sql_dialect='StarRocks';

-- query 2
-- @skip_result_check=true
SET time_zone = 'Asia/Shanghai';

-- query 3
-- Session timezone is now Asia/Shanghai
SELECT @@time_zone;

-- query 4
-- scale=0: seconds precision; 1598306400 → 2020-08-25 06:00:00 in UTC+8
SELECT to_datetime(1598306400, 0);

-- query 5
-- Default (no scale): same as scale=0
SELECT to_datetime(1598306400);

-- query 6
-- scale=3: milliseconds; 1598306400123 → 2020-08-25 06:00:00.123000
SELECT to_datetime(1598306400123, 3);

-- query 7
-- scale=6: microseconds; 1598306400123456 → 2020-08-25 06:00:00.123456
SELECT to_datetime(1598306400123456, 6);

-- query 8
-- Unix epoch 0 → 1970-01-01 08:00:00 in UTC+8
SELECT to_datetime(0, 0);

-- query 9
-- Max valid timestamp: 253402243199 → 9999-12-31 15:59:59 in UTC+8
SELECT to_datetime(253402243199, 0);

-- query 10
-- Invalid scale=10 (>6) → NULL
SELECT to_datetime(1598306400, 10);

-- query 11
-- NULL scale → NULL
SELECT to_datetime(1598306400, null);

-- query 12
-- NULL timestamp → NULL
SELECT to_datetime(null, null);

-- query 13
-- @order_sensitive=true
-- Dynamic scale via inline VALUES: mix of scales, negative ts, NULL inputs
SELECT to_datetime(t.ts_val, t.sc_val) AS dyn_scale_timezone_aware
FROM (VALUES
        (1598306400123456, 6),
        (1598306400123,    3),
        (1598306400,       0),
        (-1001,            3),
        (NULL,             0),
        (1598306400,       NULL)
     ) AS t(ts_val, sc_val);
