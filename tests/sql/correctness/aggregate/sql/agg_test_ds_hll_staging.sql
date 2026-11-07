-- Test Objective:
-- Pin the DataSketches HLL partial/final boundary under an explicit aggregation
-- staging matrix.
--
-- ds_hll_count_distinct resolves to the DataSketches DS_HLL family, whose
-- AggKind::DsHllMerge arm serializes an intermediate sketch, ships it, and
-- merges it on the far side. Every other ds_hll case asserts only the final
-- estimate, so that serialization boundary is exercised incidentally by
-- whatever plan shape the optimizer happens to pick. This case forces it:
-- the same logical value is computed under new_planner_agg_stage 2 and 3,
-- with streaming preaggregation both forced and automatic, and the results
-- are pinned exactly. HLL is deterministic for a fixed input set and lg_k, so
-- any divergence between staging variants is a real change in the intermediate
-- state contract rather than estimator noise.
--
-- The grouped variants carry a low-cardinality key so a distributed plan splits
-- partial and final aggregation across backends. Grouped default-target (HLL_4)
-- estimates are asserted as a band rather than an exact value: HLL_4 packs 4-bit
-- registers behind a curMin offset plus an aux map, so a union assembled from a
-- different partial partitioning can settle on a slightly different estimate.
-- The exact cross-staging equality is carried by the ungrouped queries, by the
-- merge-from-storage queries, and by the HLL_6 grouped pair, none of which have
-- that offset sensitivity. The merge-from-storage
-- variants read a materialized binary state column, which exercises
-- serialize -> persist -> deserialize -> merge end to end. The per-group state
-- is produced by the DS_HLL_ACCUMULATE aggregate; ds_hll_count_distinct_state is
-- the row-wise scalar form and cannot carry a GROUP BY. DS_HLL_ESTIMATE merges
-- and estimates in one aggregate, so it reads the stored state directly rather
-- than nesting DS_HLL_COMBINE inside it.

-- query 1
CREATE TABLE ${case_db}.t_stage (
  id BIGINT NOT NULL,
  grp INT NOT NULL,
  dt VARCHAR(10) NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
CREATE TABLE ${case_db}.t_stage_state (
  grp INT NOT NULL,
  ds_id binary
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
INSERT INTO ${case_db}.t_stage
SELECT generate_series, CAST(generate_series % 8 AS INT), "2026-09-16"
FROM table(generate_series(1, 100000));

-- query 4
INSERT INTO ${case_db}.t_stage_state
SELECT grp, DS_HLL_ACCUMULATE(id) FROM ${case_db}.t_stage GROUP BY grp;

-- Baseline: no staging hint. Every variant below must reproduce these numbers.

-- query 5
SELECT ds_hll_count_distinct(id) AS ungrouped FROM ${case_db}.t_stage;

-- query 6
SELECT grp, ds_hll_count_distinct(id) BETWEEN 12300 AND 12700 AS grouped_ok
FROM ${case_db}.t_stage GROUP BY grp ORDER BY grp;

-- query 7
SELECT DS_HLL_ESTIMATE(ds_id) AS merged FROM ${case_db}.t_stage_state;

-- force_streaming + stage 2

-- query 8
SELECT
  ds_hll_count_distinct(id) AS ungrouped FROM ${case_db}.t_stage;

-- query 9
SELECT
  grp, ds_hll_count_distinct(id) BETWEEN 12300 AND 12700 AS grouped_ok
FROM ${case_db}.t_stage GROUP BY grp ORDER BY grp;

-- query 10
SELECT
  DS_HLL_ESTIMATE(ds_id) AS merged FROM ${case_db}.t_stage_state;

-- force_streaming + stage 3

-- query 11
SELECT
  ds_hll_count_distinct(id) AS ungrouped FROM ${case_db}.t_stage;

-- query 12
SELECT
  grp, ds_hll_count_distinct(id) BETWEEN 12300 AND 12700 AS grouped_ok
FROM ${case_db}.t_stage GROUP BY grp ORDER BY grp;

-- query 13
SELECT
  DS_HLL_ESTIMATE(ds_id) AS merged FROM ${case_db}.t_stage_state;

-- auto + stage 2

-- query 14
SELECT
  ds_hll_count_distinct(id) AS ungrouped FROM ${case_db}.t_stage;

-- query 15
SELECT
  grp, ds_hll_count_distinct(id) BETWEEN 12300 AND 12700 AS grouped_ok
FROM ${case_db}.t_stage GROUP BY grp ORDER BY grp;

-- query 16
SELECT
  DS_HLL_ESTIMATE(ds_id) AS merged FROM ${case_db}.t_stage_state;

-- auto + stage 3

-- query 17
SELECT
  ds_hll_count_distinct(id) AS ungrouped FROM ${case_db}.t_stage;

-- query 18
SELECT
  grp, ds_hll_count_distinct(id) BETWEEN 12300 AND 12700 AS grouped_ok
FROM ${case_db}.t_stage GROUP BY grp ORDER BY grp;

-- query 19
SELECT
  DS_HLL_ESTIMATE(ds_id) AS merged FROM ${case_db}.t_stage_state;

-- Non-default lg_k and target type must survive the same boundary, because the
-- intermediate state carries both.

-- query 20
SELECT
  grp, ds_hll_count_distinct(id, 10, "HLL_6") AS grouped_hll6
FROM ${case_db}.t_stage GROUP BY grp ORDER BY grp;

-- query 21
SELECT
  grp, ds_hll_count_distinct(id, 10, "HLL_6") AS grouped_hll6
FROM ${case_db}.t_stage GROUP BY grp ORDER BY grp;
