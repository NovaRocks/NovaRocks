-- @order_sensitive=true
-- @tags=project,expression
-- Test Objective:
-- 1. Validate arithmetic projection with nullable inputs.
-- 2. Validate explicit cast projection in the same operator pipeline.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows including NULL arithmetic operands.
-- 3. Project computed columns and assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_project_arithmetic_cast;
CREATE TABLE ${case_db}.t_project_arithmetic_cast (
  id INT,
  a INT,
  b INT
);
INSERT INTO ${case_db}.t_project_arithmetic_cast VALUES
  (1, 2, 3),
  (2, 5, NULL),
  (3, -4, 10);
SELECT
  id,
  a + IFNULL(b, 0) AS sum_ab,
  a * IFNULL(b, 1) AS mul_ab,
  CAST(a AS BIGINT) AS a_big
FROM ${case_db}.t_project_arithmetic_cast
ORDER BY id;
