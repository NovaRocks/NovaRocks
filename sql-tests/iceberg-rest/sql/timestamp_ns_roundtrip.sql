-- @order_sensitive=true
-- Validate nanosecond-precision timestamp (TIMESTAMP_NS) create/insert/read
-- round-trip through the Iceberg REST catalog:
-- - CREATE TABLE with a TIMESTAMP_NS column
-- - INSERT three rows with nanosecond-precision literals
-- - SELECT id and CAST(ts AS STRING) to assert 9 fractional digits survive
--   (the MySQL wire protocol truncates to microseconds; CAST to STRING bypasses
--   that and renders the full nanosecond value)

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0};

-- query 2
-- @skip_result_check=true
-- TIMESTAMP_NS requires Iceberg format-version 3 (timestamp_ntz / timestamp_tz
-- nanosecond precision was introduced in the v3 spec).
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0} (
  id BIGINT,
  ts TIMESTAMP_NS
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
VALUES
  (1, '2024-01-02 03:04:05.123456789'),
  (2, '2024-01-02 03:04:05.000000001'),
  (3, '1970-01-01 00:00:00.000000000');

-- query 4
-- Nanosecond-precision assertion: CAST to STRING renders 9 fractional digits.
-- The significant nanosecond digits (.789 and .001) must NOT be truncated to
-- microseconds (.123456 / .000000).
SELECT id, CAST(ts AS STRING) AS s
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
  ORDER BY id;

-- query 5
-- Nanosecond range predicate pushdown: count rows where ts > '2024-01-02 03:04:05.000000001'.
-- Row 1 has ts = .123456789 (passes), row 2 has ts = .000000001 (boundary, excluded by >),
-- row 3 is epoch (excluded). Correct nanosecond semantics yield COUNT=1.
-- If the predicate bound were rounded to microseconds (.000000), rows 1 and 2 would
-- both pass (COUNT=2), proving the nanosecond path is exercised.
SELECT COUNT(*) AS cnt
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
  WHERE ts > '2024-01-02 03:04:05.000000001';

-- query 6
-- CAST nanosecond timestamp to DATETIME (microsecond) demonstrates narrowing truncation.
-- ts .123456789 → .123456 (789 sub-microsecond ns discarded)
-- ts .000000001 → .000000 (1 sub-microsecond ns discarded)
-- ts epoch     → no fractional part shown
SELECT id, CAST(CAST(ts AS DATETIME) AS STRING) AS micros
  FROM iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0}
  ORDER BY id;

-- query 7
-- Fail-fast guard: a time-based partition transform on a nanosecond timestamp
-- column must error rather than silently mis-derive partitions (IV3-7.1).
-- @expect_error=nanosecond
CREATE TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_part_${uuid0} (
  id BIGINT,
  ts TIMESTAMP_NS
) PARTITION BY (day(ts))
TBLPROPERTIES ("format-version" = "3");

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0};

-- query 9
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0};
