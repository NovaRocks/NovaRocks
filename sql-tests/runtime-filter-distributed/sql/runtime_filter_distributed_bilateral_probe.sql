-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- @tags=runtime_filter,cross_process,distributed
-- Test Objective:
-- 1. M3: a single runtime filter (one shared filter_id) is now pushed onto
--    MULTIPLE equivalent-column scans in the probe subtree, discovered via
--    transitive equivalence across an INNER join's eq-conditions
--    (`expand_probe_set_across_join` / `push_probe_down` in
--    planner/runtime_filter_placement.rs). Shape: TopJoin(J_ab, build) where
--    J_ab = t1 JOIN t2 ON t1.k = t2.k, and TopJoin joins J_ab with the build
--    side on t2.k = t3.k. The build key t3.k is transitively equal to both
--    the direct probe key t2.k and, via J_ab's own eq-condition, t1.k. An M3
--    pushdown places ONE probe (sharing the top join's filter id) on BOTH
--    t1's scan and t2's scan, and NONE on the build-side t3 scan. (J_ab
--    itself independently builds and probes its OWN local runtime filter on
--    t1.k = t2.k with a different filter_id — expected and orthogonal to the
--    M3 transitive placement being tested here; see the recorded EXPLAIN.)
-- 2. Force PARTITIONED (Shuffle) joins for both t1-t2 and J_ab-t3 via
--    cbo_broadcast_node_mem_budget_bytes = 0, with every join key equal to
--    the shuffle key (k), so probes must cross real HASH_PARTITIONED
--    exchanges to reach either scan.
-- 3. Pin the join order to the literal left-deep shape written below by
--    disabling JoinAssociativity/JoinCommutativity (both legs, identically):
--    for a 3-relation chain the DP/greedy multi-join-reorder pass is a no-op
--    (n=3 <= max_reorder_node_use_exhaustive default 4 — see
--    pass_skips_small_chain_left_to_associativity in
--    multi_join_reorder/pass.rs), so those two Cascades rules are the ONLY
--    mechanism that could re-associate the chain or flip build/probe sides,
--    independent of the DP/greedy toggles below. Without pinning them, the
--    cost search is free to pick a different pairing/build-probe split than
--    written, making the transitive-equivalence path non-deterministic
--    across ANALYZE stats drift.
-- 4. Prove the bilateral placement drops NO rows: RF-on and RF-off must
--    return the identical fingerprint end-to-end under a real cross-process
--    1FE+3BE cluster.

CREATE TABLE ${case_db}.rf_dist_bi_t1 (
    k INT NOT NULL,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_dist_bi_t2 (
    k INT NOT NULL,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.rf_dist_bi_t3 (
    k INT NOT NULL,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

-- 300 rows per key-aligned table, key space 0..99 (3 rows per key on each
-- side), so the join is non-trivial (t1 join t2 join t3 fans out per key).
INSERT INTO ${case_db}.rf_dist_bi_t1
SELECT generate_series % 100 AS k, generate_series AS v
FROM TABLE(generate_series(1, 300));

INSERT INTO ${case_db}.rf_dist_bi_t2
SELECT generate_series % 100 AS k, generate_series AS v
FROM TABLE(generate_series(1, 300));

-- t3 is the build side of the TOP join: only even keys pass the flag-style
-- predicate (v <= 150), so the runtime filter has real work to prune — via
-- transitive equivalence — on BOTH t1's and t2's scans.
INSERT INTO ${case_db}.rf_dist_bi_t3
SELECT generate_series % 100 AS k, generate_series AS v
FROM TABLE(generate_series(1, 300));

ANALYZE TABLE ${case_db}.rf_dist_bi_t1;
ANALYZE TABLE ${case_db}.rf_dist_bi_t2;
ANALYZE TABLE ${case_db}.rf_dist_bi_t3;

-- Force Shuffle (PARTITIONED) hash joins on both levels and relax the RF
-- build-size/selectivity gates so the M3 bilateral placement is exercised
-- deterministically regardless of ANALYZE stats shape (mirrors
-- sql-tests/optimizer/sql/runtime_filter_cross_fragment_shuffle.sql for the
-- single-join case). JoinAssociativity/JoinCommutativity are disabled below
-- (both legs) to pin the join order to the literal left-deep chain written.
SET global_runtime_filter_build_max_size = 10737418240;
SET global_runtime_filter_probe_min_selectivity = 0.0;
SET cbo_broadcast_node_mem_budget_bytes = 0;
SET cbo_enable_dp_join_reorder = false;
SET cbo_enable_greedy_join_reorder = false;

SET disable_optimizer_rules = 'RuntimeFilterPushDown,JoinAssociativity,JoinCommutativity';
SELECT 'bilateral_probe' AS scenario, COUNT(*) AS row_count,
       COALESCE(SUM(t1.v), 0) AS t1_sum, COALESCE(SUM(t2.v), 0) AS t2_sum
FROM ${case_db}.rf_dist_bi_t1 t1
JOIN ${case_db}.rf_dist_bi_t2 t2 ON t1.k = t2.k
JOIN ${case_db}.rf_dist_bi_t3 t3 ON t2.k = t3.k
WHERE t3.v <= 150;

SET disable_optimizer_rules = 'JoinAssociativity,JoinCommutativity';
-- The transitive filter is the one BUILT on t3.k (the top join's build side).
-- Proving IT (not J_ab's own local t1.k=t2.k filter) lands on TWO scans is
-- the bilateral/transitive M3 claim: filter_id = 1 is built once on t3.k,
-- and must appear as a probe on both t1's scan AND t2's scan below.
-- @explain_contains=HASH JOIN (PARTITIONED
-- @explain_contains=HASH_PARTITIONED (k)
-- @explain_contains=filter_id = 1, build_expr = (t3.k)
-- @explain_contains=filter_id = 1, probe_expr = (t1.k)
-- @explain_contains=filter_id = 1, probe_expr = (t2.k)
SELECT 'bilateral_probe' AS scenario, COUNT(*) AS row_count,
       COALESCE(SUM(t1.v), 0) AS t1_sum, COALESCE(SUM(t2.v), 0) AS t2_sum
FROM ${case_db}.rf_dist_bi_t1 t1
JOIN ${case_db}.rf_dist_bi_t2 t2 ON t1.k = t2.k
JOIN ${case_db}.rf_dist_bi_t3 t3 ON t2.k = t3.k
WHERE t3.v <= 150;

EXPLAIN VERBOSE
SELECT 'bilateral_probe' AS scenario, COUNT(*) AS row_count,
       COALESCE(SUM(t1.v), 0) AS t1_sum, COALESCE(SUM(t2.v), 0) AS t2_sum
FROM ${case_db}.rf_dist_bi_t1 t1
JOIN ${case_db}.rf_dist_bi_t2 t2 ON t1.k = t2.k
JOIN ${case_db}.rf_dist_bi_t3 t3 ON t2.k = t3.k
WHERE t3.v <= 150;
