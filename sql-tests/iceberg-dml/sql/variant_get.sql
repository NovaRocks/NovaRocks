-- @order_sensitive=true
-- Test Point: Spark-aligned variant_get / try_variant_get over a v3 iceberg
-- variant column: typed extraction, 2-arg variant return, try_ cast-failure
-- NULL, strict cast-failure error, missing-path NULL, WHERE usage.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_variant_get FORCE;
CREATE TABLE ${case_db}.t_variant_get (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO ${case_db}.t_variant_get VALUES
  (1, parse_json('{"a": 1, "b": "x"}')),
  (2, parse_json('{"a": 99, "b": "y"}')),
  (3, parse_json('{"b": "no-a"}')),
  (4, parse_json('{"a": 1.5}')),
  (5, NULL),
  (6, parse_json('{"a": "abc"}'));

-- query 2: typed extraction; missing path / SQL NULL / unconvertible-cast
-- rows are NULL; numeric narrowing (1.5) truncates per Spark CAST semantics
-- (IV3-6 cast-semantics decision 2026-06-11).
SELECT id, try_variant_get(v, '$.a', 'bigint') FROM ${case_db}.t_variant_get ORDER BY id;

-- query 3: strict extraction; row 4 truncates (1.5 -> 1), not an error.
SELECT id, variant_get(v, '$.a', 'bigint') FROM ${case_db}.t_variant_get WHERE id <= 4 ORDER BY id;

-- query 4: strict extraction over a genuinely unconvertible row must fail.
-- @expect_error=cast
SELECT variant_get(v, '$.a', 'bigint') FROM ${case_db}.t_variant_get WHERE id = 6;

-- query 5: 2-arg form returns variant; display via variant_typeof.
SELECT id, variant_typeof(variant_get(v, '$.a')) FROM ${case_db}.t_variant_get WHERE id <= 2 ORDER BY id;

-- query 6: predicate usage (the PR-4 pushdown target shape).
SELECT id FROM ${case_db}.t_variant_get WHERE try_variant_get(v, '$.a', 'bigint') > 5 ORDER BY id;

-- query 7: string extraction.
SELECT id, variant_get(v, '$.b', 'string') FROM ${case_db}.t_variant_get WHERE id <= 3 ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP TABLE ${case_db}.t_variant_get FORCE;
