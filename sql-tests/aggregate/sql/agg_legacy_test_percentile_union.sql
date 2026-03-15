-- Migrated from dev/test/sql/test_agg_function/R/test_percentile_union
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_percentile_union
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 double
    )
DUPLICATE KEY(c1)
DISTRIBUTED BY HASH(c1)
BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, generate_series from table(generate_series(1, 1000));

-- query 4
-- @skip_result_check=true
USE ${case_db};
set pipeline_dop=1;

-- query 5
USE ${case_db};
select percentile_approx_raw(percentile_union(percentile_hash(c2)), 0.99) from t1;
