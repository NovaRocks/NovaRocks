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
-- @skip_result_check=true
DROP TABLE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0}.t_tsns_${uuid0};

-- query 6
-- @skip_result_check=true
DROP DATABASE iceberg_rest_${suite_uuid0}.iceberg_rest_tsns_db_${uuid0};
