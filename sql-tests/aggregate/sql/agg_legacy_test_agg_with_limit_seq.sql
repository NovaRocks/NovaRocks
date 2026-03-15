-- Migrated from dev/test/sql/test_agg/R/test_agg_with_limit_seq
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'scan_chunk_sleep_after_read';

-- name: test_agg_with_limit_seq @sequential
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table base_table (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into base_table SELECT generate_series % 4, generate_series % 9, generate_series % 9, generate_series %9 FROM TABLE(generate_series(1,  10000));

-- query 4
-- @skip_result_check=true
USE ${case_db};
create table agg_with_limit_seq (
    c0 int,
    c1 int,
    c2 string,
    c3 int
) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1 PROPERTIES('replication_num' = '1');

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into agg_with_limit_seq SELECT * FROM base_table;

-- query 6
-- @skip_result_check=true
USE ${case_db};
admin enable failpoint 'scan_chunk_sleep_after_read';

-- query 7
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode="force_streaming";

-- query 8
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c0 from agg_with_limit_seq group by c0 limit 10) t order by 3;

-- query 9
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c1 from agg_with_limit_seq group by c1 limit 10) t order by 3;

-- query 10
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'scan_chunk_sleep_after_read';

-- Legacy cleanup step.
-- query 11
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'scan_chunk_sleep_after_read';
