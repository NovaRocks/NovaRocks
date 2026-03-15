-- Migrated from dev/test/sql/test_agg/R/test_grouping_set
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_grouping_set @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t0` (
  `v1` bigint(20) NULL COMMENT "",
  `v2` bigint(20) NULL COMMENT "",
  `v3` bigint(20) NULL COMMENT "",
  `v4` varchar NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`v1`, `v2`, `v3`)
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 values(1, 2, 3, 'a'), (1, 3, 4, 'b'), (2, 3, 4, 'a'), (null, 1, null, 'c'), (4, null, 1 , null),
(5, 1 , 3, 'c'), (2, 2, null, 'a'), (4, null, 4, 'c'), (null, null, 2, null);

-- query 4
USE ${case_db};
select v1, sum(v2), min(v2 + v3) from t0 group by grouping sets((v1, v2));

-- query 5
USE ${case_db};
select v1, sum(v2), min(v2 + v3) from t0 group by grouping sets((v1, v2), (v3));

-- query 6
USE ${case_db};
select xx, v2, max(v2 + v3) / sum(if(xx < 1, v2, v1 + 1)) from (select if(v1=1, 2, 3) as xx, * from t0) ff group by grouping sets ((xx, v2), (v2));

-- query 7
USE ${case_db};
select v2, sum(if(xx < 1, v2, v1 + 1)), max(v2 + v3) / sum(if(xx < 1, v2, v1 + 1)) from (select if(v1=1, 2, 3) as xx, * from t0) ff group by grouping sets ((xx, v2), (v2, v4));

-- query 8
USE ${case_db};
select v2, min(if(xx < 1, v2, v1 + 1)), sum(if(xx < 1, v2, v1 + 1)), max(v2 + v3) / sum(if(xx < 1, v2, v1 + 1)) from (select if(v1=1, 2, 3) as xx, * from t0) ff group by grouping sets ((xx, v2), (v2, v4));

-- query 9
USE ${case_db};
select v2, sum(if(xx < 1, v2, v1 + 1)), max(v2 + v3) / sum(if(xx < 1, v2, v1 + 1)) from (select if(v1=1, 2, 3) as xx, * from t0) ff group by rollup(xx, v2, v4);
