-- Test Objective:
-- 1. Validate query correctness when GRF exceeds its size limit and is disabled.
-- 2. SET_VAR hint sets global_runtime_filter_build_max_size=1 to force GRF to
--    exceed its limit, so the filter falls back to no-filter mode.
-- 3. The count result must match the baseline without GRF.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 INT
)
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.t1 SELECT generate_series FROM TABLE(generate_series(1, 65535));
INSERT INTO ${case_db}.t1 SELECT k1 + 65535 FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT k1 + 65535 * 2 FROM ${case_db}.t1;
INSERT INTO ${case_db}.t1 SELECT k1 + 65535 * 3 FROM ${case_db}.t1;

-- query 2
-- GRF build size limit=1 forces the runtime filter to exceed limit and be disabled.
-- The result must still be correct (no rows dropped, no extra rows from GRF bypass).
with tw1 as (
    select t1.k1 from ${case_db}.t1 join [broadcast] ${case_db}.t1 t2 using(k1)
)
select /*+SET_VAR(global_runtime_filter_build_max_size=1,global_runtime_filter_probe_min_size=0)*/
    count(1) from tw1 join [broadcast] ${case_db}.t1 t3 using(k1);
