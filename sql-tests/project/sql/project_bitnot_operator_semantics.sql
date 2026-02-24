-- @order_sensitive=true
-- @tags=project,bit,bitnot
-- Test Objective:
-- 1. Validate unary bitwise NOT operator semantics.
-- 2. Prevent regressions where BITNOT is lowered as binary arithmetic.
-- Test Flow:
-- 1. Evaluate BITNOT on negative/zero/positive BIGINT literals.
-- 2. Assert scalar output matches StarRocks semantics.
SELECT
  ~CAST(-1 AS BIGINT) AS n_neg1,
  ~CAST(0 AS BIGINT) AS n_zero,
  ~CAST(1 AS BIGINT) AS n_pos1,
  ~CAST(1024 AS BIGINT) AS n_1024;
