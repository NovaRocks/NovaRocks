-- OQ-5: a hash join emits a build runtime filter on the build (right) side and
-- pushes a matching probe runtime filter down to the probe-side scan. The small
-- build side + broadcast distribution keep the filter past gating.

CREATE TABLE ${case_db}.rf_build (k INT, v INT);
CREATE TABLE ${case_db}.rf_probe (k INT, v INT);
INSERT INTO ${case_db}.rf_build VALUES (1, 1), (2, 2), (3, 3);
INSERT INTO ${case_db}.rf_probe
    SELECT generate_series, generate_series FROM TABLE(generate_series(1, 100000));
ANALYZE TABLE ${case_db}.rf_build;
ANALYZE TABLE ${case_db}.rf_probe;

-- @explain_contains=build runtime filters:
-- @explain_contains=build_expr = (b.k)
-- @explain_contains=probe runtime filters:
-- @explain_contains=probe_expr = (p.k)
SELECT count(*) FROM ${case_db}.rf_probe p JOIN ${case_db}.rf_build b ON p.k = b.k;
