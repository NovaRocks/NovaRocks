-- Migrated from dev/test/sql/test_agg/R/test_serialize_key_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_serialize_key_agg @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    c0 STRING,
    c1 STRING,
    c2 STRING
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 3
USE ${case_db};
select distinct c0, c1 from t0 order by c0, c1 desc limit 10;

-- query 4
USE ${case_db};
select length(c0), length(c1) from (select distinct c0, c1 from (select * from t0 union all select space(1000000) as c0, space(1000000) as c1, space(1000000) as c2) tb) tb order by 1, 2 desc limit 10;

-- query 5
USE ${case_db};
select length(c0), max(length(c1)), max(length(c2)) from (select * from t0 union all select space(1000000) as c0, space(1000000) as c1, space(1000000) as c2) tb group by c0 order by 1, 2 desc limit 10;

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, 4096 - generate_series, generate_series FROM TABLE(generate_series(1,  4096));

-- query 7
USE ${case_db};
select max(length(c0)), max(length(c1)) from (select distinct c0, c1 from t0) tb;

-- query 8
USE ${case_db};
select max(length(c0)), max(length(c1)) from (select distinct c0, c1 from (select * from t0 union all select space(1000000) as c0, space(1000000) as c1, space(1000000) as c2) tb) tb order by 1, 2 desc limit 10;

-- query 9
USE ${case_db};
select length(c0), max(length(c1)), max(length(c2)) from (select * from t0 union all select space(1000000) as c0, space(1000000) as c1, space(1000000) as c2) tb group by c0, c1, c2 order by 1, 2 desc limit 10;
