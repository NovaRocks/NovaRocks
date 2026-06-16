-- @tags=optimizer,explain_analyze
-- Test Objective:
-- 1. EXPLAIN ANALYZE emits the canonical Planning/Execution/Rows header.
-- 2. The case uses text assertions because per-operator profile values are
--    intentionally runtime-dependent.
DROP TABLE IF EXISTS ${case_db}.t_analyze_header;
CREATE TABLE ${case_db}.t_analyze_header (k INT);
INSERT INTO ${case_db}.t_analyze_header VALUES (1), (2), (3);
ANALYZE TABLE ${case_db}.t_analyze_header;

-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Execution:
-- @result_contains=Rows: 1
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ${case_db}.t_analyze_header;
