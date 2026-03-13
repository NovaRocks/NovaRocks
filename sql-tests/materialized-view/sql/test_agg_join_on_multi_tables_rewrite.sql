-- Test Objective:
-- 1. Validate optimizer rewrite chooses the intended materialized view plan.
-- 2. Cover query correctness together with rewrite plan inspection.
-- Source: dev/test/sql/test_materialized_view/T/test_view_based_mv_rewrite

-- query 1
set enable_view_based_mv_rewrite = true;

-- query 2
CREATE TABLE `t1` (
`v4` bigint NULL COMMENT "",
`v5` bigint NULL COMMENT "",
`v6` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`v4`, `v5`, v6)
DISTRIBUTED BY HASH(`v4`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- query 3
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (1, 10, 100);

-- query 4
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (2, 20, 200);

-- query 5
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (3, 30, 300);

-- query 6
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (4, 40, 400);

-- query 7
INSERT INTO `t1` (`v4`, `v5`, `v6`) VALUES (5, 50, 500);

-- query 8
CREATE TABLE `t2` (
`c4` bigint NULL COMMENT "",
`c5` bigint NULL COMMENT "",
`c6` bigint NULL
) ENGINE=OLAP
DUPLICATE KEY(`c4`, `c5`, c6)
DISTRIBUTED BY HASH(`c4`) BUCKETS 3
PROPERTIES (
    "replication_num" = "1"
);

-- query 9
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (1, 1009, 10009);

-- query 10
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (2, NULL, 10010);

-- query 11
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (3, 1011, NULL);

-- query 12
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (4, 1009, 10009);

-- query 13
INSERT INTO `t2` (`c4`, `c5`, `c6`) VALUES (5, NULL, 10010);

-- query 14
create view agg_view_1
as
select v4, sum(v5) as total from t1 group by v4;

-- query 15
create materialized view mv_1
DISTRIBUTED by hash(`v4`)
refresh manual
as
select v4, total, c5, c6 from t2 join agg_view_1 on c4 = v4;

-- query 16
refresh materialized view mv_1 with sync mode;

-- query 17
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN select v4, total, c5, c6 from t2 join agg_view_1 on c4 = v4;
