-- Migrated from dev/test/sql/test_agg/R/test_agg_split_two_phase
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_split_two_phase @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    c0 STRING,
    c1 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series FROM TABLE(generate_series(1,  1500));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, NULL FROM TABLE(generate_series(1,  1500));

-- query 5
-- @skip_result_check=true
USE ${case_db};
update information_schema.be_configs set value = "0" where name= "two_level_memory_threshold";

-- query 6
USE ${case_db};
select c1 from t0 where c1 is null group by c1;

-- query 7
USE ${case_db};
select c1, count(*) from t0 where c1 is null group by c1;
