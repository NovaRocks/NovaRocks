-- @tags=optimizer,explain_analyze
-- @normalize_explain_timing=true
-- Test Objective:
-- 1. EXPLAIN ANALYZE emits the canonical Planning/Execution/Rows header.
-- 2. Timing values are normalized to <MS> so the case is stable across runs.
DROP TABLE IF EXISTS ${case_db}.t_analyze_header;
CREATE TABLE ${case_db}.t_analyze_header (k INT);
INSERT INTO ${case_db}.t_analyze_header VALUES (1), (2), (3);
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ${case_db}.t_analyze_header;
