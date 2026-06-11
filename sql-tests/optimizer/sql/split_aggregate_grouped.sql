-- OQ-4: grouped non-DISTINCT aggregate over an iceberg table. In distributed
-- execution, grouped aggregates use LOCAL/ShuffleAgg/GLOBAL even for small
-- inputs so each group is finalized on one node. Plan-shape golden.
CREATE TABLE ${case_db}.t_split_agg_grouped (k INT, v INT);
INSERT INTO ${case_db}.t_split_agg_grouped VALUES
    (1, 10), (1, 20), (1, 30),
    (2, 5),  (2, 15), (2, 25),
    (3, 7),  (3, 11), (3, 13),
    (4, 1),  (4, 2),  (4, 3);
ANALYZE TABLE ${case_db}.t_split_agg_grouped;
EXPLAIN VERBOSE
SELECT k, SUM(v) AS s
FROM ${case_db}.t_split_agg_grouped
GROUP BY k
ORDER BY k;
