-- @tags=optimizer,oq9,residual,range_envelope
-- Test Objective:
-- Derive conservative range envelopes from OR branches while preserving the
-- original OR predicate.
DROP TABLE IF EXISTS ${case_db}.residual_rng_l;
DROP TABLE IF EXISTS ${case_db}.residual_rng_r;
CREATE TABLE ${case_db}.residual_rng_l (k INT, score INT, payload INT);
CREATE TABLE ${case_db}.residual_rng_r (k INT, score INT, payload INT);

EXPLAIN VERBOSE
SELECT l.payload, r.payload
FROM ${case_db}.residual_rng_l l
JOIN ${case_db}.residual_rng_r r ON l.k = r.k
WHERE (l.score = r.score AND l.score BETWEEN 10 AND 20)
   OR (l.score = r.score AND l.score BETWEEN 30 AND 40);
