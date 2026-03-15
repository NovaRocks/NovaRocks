-- @order_sensitive=true
-- @tags=project,case
-- Test Objective:
-- 1. Validate CASE-WHEN projection with nullable branches.
-- 2. Validate COALESCE normalization in the same projected row set.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert rows covering grade boundaries and NULL values.
-- 3. Project CASE and COALESCE expressions, then assert ordered output.
DROP TABLE IF EXISTS ${case_db}.t_project_case_coalesce;
CREATE TABLE ${case_db}.t_project_case_coalesce (
  id INT,
  score INT,
  note STRING
);
INSERT INTO ${case_db}.t_project_case_coalesce VALUES
  (1, 95, 'top'),
  (2, 82, NULL),
  (3, NULL, 'missing'),
  (4, 60, NULL);
SELECT
  id,
  CASE
    WHEN score >= 90 THEN 'A'
    WHEN score >= 80 THEN 'B'
    WHEN score IS NULL THEN 'N/A'
    ELSE 'C'
  END AS grade,
  COALESCE(note, 'EMPTY') AS note_norm
FROM ${case_db}.t_project_case_coalesce
ORDER BY id;
