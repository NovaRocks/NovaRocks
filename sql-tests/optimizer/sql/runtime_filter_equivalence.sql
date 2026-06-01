-- OQ-5: a runtime filter only reduces work, never changes results. The same
-- join is run with RF enabled (default) and disabled; both result blocks must
-- be identical in the golden.

CREATE TABLE ${case_db}.eq_b (k INT, v INT);
CREATE TABLE ${case_db}.eq_p (k INT, v INT);
INSERT INTO ${case_db}.eq_b VALUES (1, 10), (2, 20), (5, 50);
INSERT INTO ${case_db}.eq_p
    SELECT generate_series % 7, generate_series FROM TABLE(generate_series(1, 5000));
ANALYZE TABLE ${case_db}.eq_b;
ANALYZE TABLE ${case_db}.eq_p;

-- RF enabled (default).
SELECT b.k, count(*) AS c
FROM ${case_db}.eq_p p JOIN ${case_db}.eq_b b ON p.k = b.k
GROUP BY b.k ORDER BY b.k;

-- RF disabled: same result.
SET disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT b.k, count(*) AS c
FROM ${case_db}.eq_p p JOIN ${case_db}.eq_b b ON p.k = b.k
GROUP BY b.k ORDER BY b.k;
