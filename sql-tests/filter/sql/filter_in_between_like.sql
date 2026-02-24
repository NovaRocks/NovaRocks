-- @order_sensitive=true
-- @tags=filter,string
-- Test Objective:
-- 1. Validate combined LIKE and BETWEEN predicates.
-- 2. Prevent regressions in mixed string+numeric predicate conjunctions.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic string and score rows.
-- 3. Apply LIKE + BETWEEN filter and assert ordered output.
CREATE DATABASE IF NOT EXISTS sql_tests_d04;
DROP TABLE IF EXISTS sql_tests_d04.t_filter_in_between_like;
CREATE TABLE sql_tests_d04.t_filter_in_between_like (
  id INT,
  name STRING,
  score INT
);
INSERT INTO sql_tests_d04.t_filter_in_between_like VALUES
  (1, 'apple', 12),
  (2, 'apricot', 20),
  (3, 'banana', 15),
  (4, 'azure', 9),
  (5, 'avocado', 21);
SELECT id, name, score
FROM sql_tests_d04.t_filter_in_between_like
WHERE name LIKE 'a%' AND score BETWEEN 10 AND 20
ORDER BY id;
