-- @tags=optimizer,g3
-- G3 passthrough (Filter/Project report the child distribution) over small
-- iceberg base tables. In distributed execution, the grouped aggregate above
-- the join is split into LOCAL/ShuffleAgg/GLOBAL so each group is finalized
-- on one node. Plan-shape golden.
DROP TABLE IF EXISTS ${case_db}.g3_pt_a;
DROP TABLE IF EXISTS ${case_db}.g3_pt_b;
CREATE TABLE ${case_db}.g3_pt_a (k INT, v INT);
CREATE TABLE ${case_db}.g3_pt_b (k INT, w INT);
INSERT INTO ${case_db}.g3_pt_a VALUES (1, 10), (2, 20);
INSERT INTO ${case_db}.g3_pt_b VALUES (1, 100), (2, 200);
ANALYZE TABLE ${case_db}.g3_pt_a;
ANALYZE TABLE ${case_db}.g3_pt_b;
EXPLAIN VERBOSE
SELECT a.k, SUM(a.v + b.w) AS s
FROM ${case_db}.g3_pt_a a
INNER JOIN ${case_db}.g3_pt_b b ON a.k = b.k
WHERE a.v > 0
GROUP BY a.k;
