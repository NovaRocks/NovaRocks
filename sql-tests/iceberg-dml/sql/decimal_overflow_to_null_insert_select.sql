-- @order_sensitive=true
-- @tags=iceberg_dml,decimal,overflow
-- Test Objective:
-- 1. Validate overflow-range decimal rows can be sanitized to NULL before sink writes.
-- 2. Validate in-range boundary rows are still written after scale narrowing.
DROP TABLE IF EXISTS ${case_db}.t_decimal_overflow_sink;
CREATE TABLE ${case_db}.t_decimal_overflow_sink (
  id INT,
  v DECIMAL(10, 2)
);
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(1 AS INT),
  CASE
    WHEN ABS(CAST(99999999.9949 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(99999999.9949 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(2 AS INT),
  CASE
    WHEN ABS(CAST(-99999999.9949 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(-99999999.9949 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(3 AS INT),
  CASE
    WHEN ABS(CAST(100000000.0000 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(100000000.0000 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(4 AS INT),
  CASE
    WHEN ABS(CAST(-100000000.0000 AS DECIMAL(13, 4))) > 99999999.9999 THEN NULL
    ELSE CAST(-100000000.0000 AS DECIMAL(13, 4))
  END;
INSERT INTO ${case_db}.t_decimal_overflow_sink
SELECT
  CAST(5 AS INT),
  CAST(NULL AS DECIMAL(13, 4));
SELECT
  id,
  v,
  IF(v IS NULL, 1, 0) AS is_null_v
FROM ${case_db}.t_decimal_overflow_sink
ORDER BY id;
