-- @tags=optimizer,derive_join_not_null,session_rule_disable
-- Test Objective:
-- SET disable_optimizer_rules='DeriveJoinNotNullPredicate' suppresses the
-- derivation. Keep JoinCommutativity disabled in both plans so this case only
-- tests the derived IS NOT NULL predicates instead of join orientation.
DROP TABLE IF EXISTS ${case_db}.t_dnn_dl;
DROP TABLE IF EXISTS ${case_db}.t_dnn_dr;
CREATE TABLE ${case_db}.t_dnn_dl (k INT, v INT);
CREATE TABLE ${case_db}.t_dnn_dr (k INT, v INT);
INSERT INTO ${case_db}.t_dnn_dl
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
INSERT INTO ${case_db}.t_dnn_dr
    SELECT CASE WHEN generate_series % 12 = 0 THEN generate_series ELSE NULL END, generate_series
    FROM TABLE(generate_series(1, 2000));
ANALYZE TABLE ${case_db}.t_dnn_dl;
ANALYZE TABLE ${case_db}.t_dnn_dr;

SET disable_optimizer_rules = 'JoinCommutativity';

-- @result_contains=predicates: l.k IS NOT NULL
-- @result_contains=predicates: r.k IS NOT NULL
EXPLAIN VERBOSE
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET disable_optimizer_rules = 'DeriveJoinNotNullPredicate,JoinCommutativity';

-- @result_not_contains=predicates: l.k IS NOT NULL
-- @result_not_contains=predicates: r.k IS NOT NULL
EXPLAIN VERBOSE
SELECT l.v, r.v
FROM ${case_db}.t_dnn_dl l
INNER JOIN ${case_db}.t_dnn_dr r ON l.k = r.k;

SET disable_optimizer_rules = '';
