-- @tags=optimizer,oq9,residual,or_side_filter
-- Test Objective:
-- Derive an opposite-side IN filter from an OR group on an inner join key
-- without expanding the original OR group into separate joins.
DROP TABLE IF EXISTS ${case_db}.residual_or_l;
DROP TABLE IF EXISTS ${case_db}.residual_or_r;
CREATE TABLE ${case_db}.residual_or_l (k INT, payload INT);
CREATE TABLE ${case_db}.residual_or_r (k INT, payload INT);

EXPLAIN VERBOSE
SELECT l.payload, r.payload
FROM ${case_db}.residual_or_l l
JOIN ${case_db}.residual_or_r r ON l.k = r.k
WHERE l.k = 1 OR l.k = 2;
