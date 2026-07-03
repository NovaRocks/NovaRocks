-- @tags=low-cardinality,dictionary,observability
-- Verify EXPLAIN ANALYZE exposes dictionary carrier runtime counters without
-- restoring legacy native rewrite plan shapes.

CREATE TABLE ${case_db}.dict_observability_orders (
  order_id BIGINT,
  status STRING,
  amount INT
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.dict_observability_orders VALUES
  (1, 'NEW', 10),
  (2, 'PAID', 20),
  (3, 'PAID', 30),
  (4, 'CANCELLED', 40),
  (5, 'SHIPPED', 50),
  (6, NULL, 60);

ANALYZE FULL TABLE ${case_db}.dict_observability_orders;

-- @normalize_explain_timing=true
-- @result_contains=dict={in_rows=
-- @result_contains=kept_rows=
-- @result_contains=hydrated_rows=
-- @result_contains=in_cols=
-- @result_contains=kept_cols=
-- @result_contains=hydrated_cols=
-- @result_contains=unsupported_cols=
-- @result_not_contains=dict=[
-- @result_not_contains=DECODE
EXPLAIN ANALYZE
SELECT status, count(*) AS cnt
FROM ${case_db}.dict_observability_orders
WHERE status <> 'CANCELLED'
GROUP BY status
ORDER BY status;
