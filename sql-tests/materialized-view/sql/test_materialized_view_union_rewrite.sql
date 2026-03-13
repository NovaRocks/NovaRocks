-- Test Objective:
-- 1. Validate UNION-style rewrite and compensation plan generation.
-- 2. Cover union rewrite eligibility across multiple branches.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_union_rewrite

-- query 1
CREATE TABLE `t1` (
  `k1` date NULL COMMENT "",
  `k2` datetime NULL COMMENT "",
  `k3` char(20) NULL COMMENT "",
  `k4` varchar(20) NULL COMMENT "",
  `k5` boolean NULL COMMENT "",
  `v1` tinyint(4) NULL COMMENT "",
  `v2` smallint(6) NULL COMMENT "",
  `v3` int(11) NULL COMMENT "",
  `v4` bigint(20) NULL COMMENT "",
  `v5` largeint(40) NULL COMMENT "",
  `v6` float NULL COMMENT "",
  `v7` double NULL COMMENT "",
  `v8` decimal(27, 9) NULL COMMENT ""
) ENGINE=OLAP
UNIQUE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)
COMMENT "OLAP"
PARTITION BY RANGE(`k1`)
(
  PARTITION p1 VALUES [("0000-01-01"), ("2020-01-01")),
  PARTITION p2 VALUES [("2020-01-01"), ("2023-01-01")),
  PARTITION p3 VALUES [("2023-01-01"), ("2025-01-01"))
)
DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`)
PROPERTIES (
  "replication_num" = "1"
);

-- query 2
insert into t1 values ('2020-01-01', '2020-01-01 01:00:00', '1', '1', 1, 1, 1, 1, 1, 1, 1, 1, 1.0);

-- query 3
CREATE MATERIALIZED VIEW test_mv0
PARTITION BY (`k1`)
DISTRIBUTED BY random
REFRESH DEFERRED MANUAL
PROPERTIES (
  "replication_num" = "1"
)
AS SELECT * FROM t1;

-- query 4
REFRESH MATERIALIZED VIEW test_mv0 WITH SYNC MODE;

-- query 5
-- complete rewrite
-- @result_contains=test_mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select count(*) from t1;

-- query 6
-- @result_contains=test_mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from t1;

-- query 7
select count(*) from t1;

-- query 8
select * from t1 order by k1;

-- query 9
-- union rewrite
insert into t1 values ('2023-01-02', '2020-01-01 01:00:00', '1', '1', 1, 1, 1, 1, 1, 1, 1, 1, 1.0);

-- query 10
-- @result_contains=test_mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select count(*) from t1;

-- query 11
-- @result_contains=test_mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from t1;

-- query 12
select count(*) from t1;

-- query 13
select * from t1 order by k1;

-- query 14
drop materialized view test_mv0;

-- query 15
drop table t1;
