-- Migrated from dev/test/sql/test_agg/R/test_groupby_array
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_groupby_array @slow @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
SET @var = array_map(x -> CAST(x AS STRING), array_generate(1, 2000000, 1));

-- query 3
USE ${case_db};
with input as (SELECT @var AS a UNION ALL SELECT ["A", "B", "C"] AS a)
SELECT count(*) from (select a from input group by 1) AS t;

-- query 4
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `id` int(11) NULL COMMENT "",
  `array_varchar` array<varchar(100)>
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t1 select generate_series, array_map(x -> cast(x as string), array_generate(1, generate_series % 100, 1)) from table(generate_series(1, 10000));

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into t1 values (0, array_map(x -> CAST(x AS STRING), array_generate(1, 100000, 1))),
(10001, array_map(x -> CAST(x AS STRING), array_generate(1, 100000, 1)));

-- query 7
USE ${case_db};
select count() from (select distinct array_varchar from t1) t;
