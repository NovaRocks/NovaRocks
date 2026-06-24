-- @tags=optimizer,explain_analyze
-- Test Objective:
-- 1. EXPLAIN ANALYZE executes the distributed plan and renders the summary header.
DROP TABLE IF EXISTS ${case_db}.t_analyze_header;
CREATE TABLE ${case_db}.t_analyze_header (k INT);
INSERT INTO ${case_db}.t_analyze_header VALUES (1), (2), (3);
ANALYZE TABLE ${case_db}.t_analyze_header;

-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Rows: 1
-- @result_contains=Profile: fragments=
-- @result_contains=PLAN FRAGMENT 0
-- @result_contains=HASH AGGREGATE
EXPLAIN ANALYZE
SELECT COUNT(*) FROM ${case_db}.t_analyze_header;
