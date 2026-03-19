-- Migrated from dev/test/sql/test_array_fn/R/test_array_distinct
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_distinct @slow @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 INT,
    c2 ARRAY<BIGINT>
)
DUPLICATE KEY(C1)
DISTRIBUTED BY HASH(C1) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 3
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (
    c1 INT,
    c2 ARRAY<ARRAY<BIGINT>>
)
DUPLICATE KEY(C1)
DISTRIBUTED BY HASH(C1) BUCKETS 1
PROPERTIES("replication_num"="1");

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, array_append([], generate_series) from TABLE(generate_series(1, 5000000));

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t2 select 1, array_agg(c2) from t1;

-- query 6
USE ${case_db};
select array_length(array_distinct(c2)) from t2;
