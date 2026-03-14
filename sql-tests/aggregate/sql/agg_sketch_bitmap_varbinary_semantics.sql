-- @order_sensitive=true
-- @tags=aggregate,bitmap,hll,theta,varbinary
-- Test Objective:
-- 1. Validate bitmap/HLL aggregate families on integer and VARBINARY inputs.
-- 2. Cover ds_hll state materialization and estimate over persisted binary state.
-- 3. Assert theta sketches fail fast with an explicit unsupported error.
-- Test Flow:
-- 1. Create/reset source and state tables.
-- 2. Insert deterministic grouped rows with duplicates and NULLs.
-- 3. Assert exact vs approximate distinct counts, bitmap aggregation, and ds_hll state estimation.
-- query 1
CREATE DATABASE IF NOT EXISTS sql_tests_d06;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_sketch_bitmap_source;
DROP TABLE IF EXISTS sql_tests_d06.t_agg_ds_hll_state;
CREATE TABLE sql_tests_d06.t_agg_sketch_bitmap_source (
    grp INT,
    id_int INT,
    name STRING,
    vb VARBINARY
);

CREATE TABLE sql_tests_d06.t_agg_ds_hll_state (
    grp INT,
    ds_vb BINARY
);

INSERT INTO sql_tests_d06.t_agg_sketch_bitmap_source VALUES
    (1, 1, 'alpha', to_binary('alpha', 'utf8')),
    (1, 2, 'beta', to_binary('beta', 'utf8')),
    (1, 2, 'beta-dup', to_binary('beta', 'utf8')),
    (1, 3, 'gamma', to_binary('gamma', 'utf8')),
    (2, 3, 'gamma-dup', to_binary('gamma', 'utf8')),
    (2, 4, 'delta', to_binary('delta', 'utf8')),
    (2, 5, 'epsilon', to_binary('epsilon', 'utf8')),
    (2, NULL, 'null-vb', NULL),
    (3, 6, 'zeta', to_binary('zeta', 'utf8')),
    (3, 6, 'zeta-dup', to_binary('zeta', 'utf8'));

INSERT INTO sql_tests_d06.t_agg_ds_hll_state
SELECT
    grp,
    ds_hll_count_distinct_state(vb)
FROM sql_tests_d06.t_agg_sketch_bitmap_source;
SELECT
    COUNT(DISTINCT id_int) AS exact_id,
    ndv(id_int) AS ndv_id,
    approx_count_distinct(id_int) AS approx_id,
    approx_count_distinct_hll_sketch(id_int) AS hll_sketch_id,
    ds_hll_count_distinct(id_int, 10, 'HLL_6') AS ds_hll_id
FROM sql_tests_d06.t_agg_sketch_bitmap_source;

-- query 2
SELECT
    grp,
    COUNT(DISTINCT vb) AS exact_vb,
    ndv(vb) AS ndv_vb,
    approx_count_distinct(vb) AS approx_vb,
    ds_hll_count_distinct(vb) AS ds_hll_vb
FROM sql_tests_d06.t_agg_sketch_bitmap_source
GROUP BY grp
ORDER BY grp;

-- query 3
SELECT
    bitmap_union_int(id_int) AS bitmap_union_cnt,
    bitmap_to_string(bitmap_agg(id_int)) AS bitmap_members
FROM sql_tests_d06.t_agg_sketch_bitmap_source
WHERE id_int IS NOT NULL;

-- query 4
SELECT
    grp,
    bitmap_to_string(bitmap_agg(id_int)) AS bitmap_members,
    hll_union_agg(hll_hash(id_int)) AS hll_union_cnt
FROM sql_tests_d06.t_agg_sketch_bitmap_source
WHERE id_int IS NOT NULL
GROUP BY grp
ORDER BY grp;

-- query 5
SELECT
    grp,
    ds_hll_estimate(ds_vb) AS est_vb
FROM sql_tests_d06.t_agg_ds_hll_state
GROUP BY grp
ORDER BY grp;

-- query 6
-- @expect_error=HLL_4/HLL_6/HLL_8
SELECT ds_hll_count_distinct(id_int, 10, 'INVALID')
FROM sql_tests_d06.t_agg_sketch_bitmap_source;

-- query 7
-- @expect_error=unsupported agg function: ds_theta_count_distinct
SELECT ds_theta_count_distinct(id_int)
FROM sql_tests_d06.t_agg_sketch_bitmap_source;
