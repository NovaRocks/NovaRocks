-- Test Objective:
-- 1. Validate aggregate rewrite across grouped queries and HAVING predicates.
-- 2. Cover aggregate rollup eligibility and final query correctness.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_rewrite_with_agg

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
CREATE TABLE `t1` (
  `id` int(11) NULL,
  `pt` date NOT NULL,
  `gmv` int(11) NULL
) ENGINE=OLAP
DUPLICATE KEY(`id`)
PARTITION BY date_trunc('day', pt)
DISTRIBUTED BY HASH(`pt`)
PROPERTIES (
"replication_num" = "1"
);

-- query 4
insert into t1 values(2,'2023-03-07',1), (2,'2023-03-08',3), (2,'2023-03-11',10);

-- query 5
CREATE MATERIALIZED VIEW `test_mv1`
PARTITION BY (`pt`)
DISTRIBUTED BY RANDOM
REFRESH DEFERRED ASYNC START("2024-03-08 03:00:00") EVERY(INTERVAL 1 DAY)
PROPERTIES (
"partition_refresh_number" = "1"
)
AS SELECT `pt`, `id`, sum(gmv) AS `sum_gmv`, count(gmv) AS `count_gmv`
FROM `t1`
GROUP BY `pt`, `id`;

-- query 6
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 7
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select pt,avg(gmv) as a from t1 group by pt;

-- query 8
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select pt,avg(gmv) as a from t1 group by pt having a>0;

-- query 9
select pt,avg(gmv) as a from t1 group by pt order by 1;

-- query 10
select pt,avg(gmv) as a from t1 group by pt having a>5.0 order by 1;

-- query 11
drop database db_${uuid0} force;
