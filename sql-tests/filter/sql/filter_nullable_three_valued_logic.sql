-- @order_sensitive=true
-- @tags=filter,null,three_valued_logic
-- Test Objective:
-- 1. Validate three-valued boolean predicate behavior in WHERE filtering.
-- 2. Prevent regressions where NULL predicate results are treated as TRUE.
-- Test Flow:
-- 1. Create/reset source table with nullable numeric columns.
-- 2. Insert rows that produce TRUE/FALSE/NULL predicate outcomes.
-- 3. Filter with nullable predicate plus OR branch and assert deterministic output.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_nullable_three_valued_logic;
CREATE TABLE sql_tests_d04.t_filter_nullable_three_valued_logic (
  id INT,
  a INT,
  b INT,
  tag STRING
);
INSERT INTO sql_tests_d04.t_filter_nullable_three_valued_logic VALUES
  (1, 3, 1, 'n'),
  (2, 3, 0, 'n'),
  (3, NULL, 2, 'n'),
  (4, 5, NULL, 'n'),
  (5, 1, 1, 'force'),
  (6, NULL, NULL, 'force');
SELECT
  id,
  a,
  b,
  (a > b AND b > 0) AS pred
FROM sql_tests_d04.t_filter_nullable_three_valued_logic
WHERE (a > b AND b > 0) OR tag = 'force'
ORDER BY id;
