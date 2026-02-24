-- @order_sensitive=true
-- @tags=project,string,int_args
-- Test Objective:
-- 1. Validate string functions with integer arguments (INT literals).
-- 2. Prevent regressions where INT-typed arguments are rejected or produce wrong NULL/empty semantics.
-- Test Flow:
-- 1. Evaluate string scalar functions in a single deterministic SELECT.
-- 2. Cover negative/zero/positive integer argument behavior for split_part, substring_index, repeat, space, left/right, lpad/rpad.
-- 3. Compare one-row output against expected values.
SELECT
  split_part('a,b,c', ',', -1) AS sp_neg1,
  split_part('a,b,c', ',', -2) AS sp_neg2,
  split_part('abc', '', 1) AS sp_empty_d1,
  split_part('abc', '', 4) AS sp_empty_d4,
  substring_index('a.b.c', '.', -1) AS si_neg1,
  substring_index('a.b.c', '.', 0) AS si_zero,
  repeat('ab', 2) AS rep2,
  repeat('ab', -1) AS rep_neg,
  space(-2) AS sp_neg,
  length(space(3)) AS space3_len,
  left('abc', 2) AS left2,
  left('abc', -1) AS left_neg,
  right('abc', 2) AS right2,
  right('abc', -1) AS right_neg,
  lpad('ab', 1, '') AS lpad_empty_shrink,
  lpad('ab', -1, 'x') AS lpad_neg,
  rpad('ab', 1, '') AS rpad_empty_shrink,
  rpad('ab', -1, 'x') AS rpad_neg;
