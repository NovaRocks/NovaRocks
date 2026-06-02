-- OQ-5 Stage 2 + Stage 3: session variables widen the gating thresholds so a
-- SHUFFLE join's build RF survives, and the probe RF is pushed ACROSS the
-- shuffle exchange down to the base scan in the downstream fragment.
--
-- The outer join is planned as SHUFFLE because each inner join's estimated
-- output is large (local-table cardinality defaults dominate), so both of the
-- outer join's inputs exceed the broadcast row limit. By default that outer RF
-- is gated out (build side too large); raising build_max + dropping
-- probe_min_selectivity lets it through, exercising cross-exchange push-down.

CREATE TABLE ${case_db}.ra (k INT, v INT);
CREATE TABLE ${case_db}.rb (k INT, v INT);
INSERT INTO ${case_db}.ra VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.rb VALUES (1, 1), (2, 2), (3, 3);
ANALYZE TABLE ${case_db}.ra;
ANALYZE TABLE ${case_db}.rb;

SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;

-- @explain_contains=HASH JOIN (SHUFFLE
-- @explain_contains=build_expr = (t2.cv)
-- @explain_contains=probe_expr = (t1.av)
SELECT count(*) AS cnt
FROM (SELECT a.v AS av FROM ${case_db}.ra a JOIN ${case_db}.rb b ON a.k = b.k) t1
JOIN (SELECT c.v AS cv FROM ${case_db}.ra c JOIN ${case_db}.rb d ON c.k = d.k) t2
ON t1.av = t2.cv;
