-- @tags=optimizer,topn,compactness
-- Test Objective:
-- Lock in TopN pushdown behavior for UNION ALL while preserving fail-closed
-- guards for Aggregate and Join.
-- The SetOp positive case uses enough branch rows for the extractor to choose
-- the branch-pruning candidate, so the golden shows pushed branch TopN nodes.
DROP TABLE IF EXISTS ${case_db}.topn_compactness_left_src;
DROP TABLE IF EXISTS ${case_db}.topn_compactness_right_src;
CREATE TABLE ${case_db}.topn_compactness_left_src (id INT, score INT);
CREATE TABLE ${case_db}.topn_compactness_right_src (id INT, score INT);
INSERT INTO ${case_db}.topn_compactness_left_src
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.topn_compactness_right_src
    SELECT generate_series + 100000, 200000 - generate_series
    FROM TABLE(generate_series(1, 100000));

EXPLAIN VERBOSE
SELECT id, score
FROM (
    SELECT id, score
    FROM ${case_db}.topn_compactness_left_src
    UNION ALL
    SELECT id, score
    FROM ${case_db}.topn_compactness_right_src
) u
ORDER BY score DESC, id ASC
LIMIT 2;

EXPLAIN VERBOSE
SELECT id, SUM(score) AS total_score
FROM (
    SELECT id, score
    FROM ${case_db}.topn_compactness_left_src
    UNION ALL
    SELECT id, score
    FROM ${case_db}.topn_compactness_right_src
) u
GROUP BY id
ORDER BY total_score DESC
LIMIT 1;

-- @skip_result_check=true
-- @result_contains=MERGING-EXCHANGE
-- @result_contains=LOCAL TOP-N (limit=1, offset=0)
-- @result_contains=HASH JOIN (BROADCAST, INNER, eq:
EXPLAIN VERBOSE
SELECT l.id, l.score, r.score AS rhs_score
FROM ${case_db}.topn_compactness_left_src l
INNER JOIN ${case_db}.topn_compactness_right_src r ON l.id = r.id
ORDER BY l.score DESC
LIMIT 1;
