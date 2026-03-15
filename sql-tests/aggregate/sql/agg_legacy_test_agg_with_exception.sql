-- Migrated from dev/test/sql/test_agg/R/test_agg_with_exception
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'agg_hash_set_bad_alloc';
admin disable failpoint 'aggregate_build_hash_map_bad_alloc';

-- name: test_agg_with_exception @sequential
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t0` (
  `c0` int(11) NOT NULL COMMENT "",
  `c1` varchar(20) NOT NULL COMMENT "",
  `c2` varchar(200) NOT NULL COMMENT "",
  `c3` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 4
PROPERTIES (
"replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE ${case_db};
set enable_group_by_compressed_key=false;

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  40960));

-- query 5
-- @skip_result_check=true
USE ${case_db};
admin enable failpoint 'agg_hash_set_bad_alloc';

-- query 6
-- @expect_error=Mem usage has exceed the limit of BE: BE:10004
USE ${case_db};
select c0 from t0 group by c0;

-- query 7
-- @expect_error=Mem usage has exceed the limit of BE: BE:10004
USE ${case_db};
select c0 from t0 group by c0 limit 10;

-- query 8
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'agg_hash_set_bad_alloc';

-- query 9
-- @skip_result_check=true
USE ${case_db};
admin enable failpoint 'aggregate_build_hash_map_bad_alloc';

-- query 10
-- @expect_error=Mem usage has exceed the limit of BE: BE:10004
USE ${case_db};
select count(*) from t0 group by c0;

-- query 11
-- @expect_error=Mem usage has exceed the limit of BE: BE:10004
USE ${case_db};
select count(*) from t0 group by c0 limit 10;

-- query 12
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'aggregate_build_hash_map_bad_alloc';

-- Legacy cleanup step.
-- query 13
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'agg_hash_set_bad_alloc';

-- Legacy cleanup step.
-- query 14
-- @skip_result_check=true
USE ${case_db};
admin disable failpoint 'aggregate_build_hash_map_bad_alloc';
