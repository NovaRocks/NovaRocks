-- Test Objective:
-- 1. Validate optimizer rewrite chooses the intended materialized view plan.
-- 2. Cover query correctness together with rewrite plan inspection.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_rewrite2

-- query 1
CREATE TABLE test_empty_partition_tbl(
  `dt` datetime DEFAULT NULL,
  `col1` bigint(20) DEFAULT NULL,
  `col2` bigint(20) DEFAULT NULL,
  `col3` bigint(20) DEFAULT NULL,
  `error_code` varchar(1048576) DEFAULT NULL
)
DUPLICATE KEY (dt, col1)
PARTITION BY date_trunc('day', dt)
PROPERTIES (
"replication_num" = "1"
);

-- query 2
CREATE MATERIALIZED VIEW  test_empty_partition_mv1
DISTRIBUTED BY RANDOM
partition by date_trunc('day', dt)
PROPERTIES (
"replication_num" = "1"
)
AS select
      col1,
        dt,
        sum(col2) AS sum_col2,
        sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3
    FROM
        test_empty_partition_tbl AS f
    GROUP BY
        col1,
        dt;

-- query 3
insert into test_empty_partition_tbl values('2023-08-16', 1, 1, 1, 'a');

-- query 4
refresh materialized view test_empty_partition_mv1 with sync mode;

-- query 5
-- @result_contains=test_empty_partition_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE (dt >= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s')) AND (dt <= STR_TO_DATE('2023-08-16 00:00:00', '%Y-%m-%d %H:%i:%s')) GROUP BY col1;

-- query 6
-- @result_contains=test_empty_partition_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE dt >= '2023-08-15 00:00:00' GROUP BY col1;

-- query 7
-- @result_contains=test_empty_partition_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE dt = '2023-08-16 00:00:00' GROUP BY col1;

-- query 8
select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE (dt >= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s')) AND (dt <= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s')) GROUP BY col1;

-- query 9
select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE (dt >= STR_TO_DATE('2023-08-15 00:00:00', '%Y-%m-%d %H:%i:%s')) AND (dt <= STR_TO_DATE('2023-08-16 00:00:00', '%Y-%m-%d %H:%i:%s')) GROUP BY col1;

-- query 10
select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE dt >= '2023-08-15 00:00:00' GROUP BY col1;

-- query 11
select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE dt <= '2023-08-15 00:00:00' GROUP BY col1;

-- query 12
select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE dt = '2023-08-15 00:00:00' GROUP BY col1;

-- query 13
select col1, sum(col2) AS sum_col2, sum(if(error_code = 'TIMEOUT', col3, 0)) AS sum_col3 FROM test_empty_partition_tbl AS f WHERE dt = '2023-08-16 00:00:00' GROUP BY col1;

-- query 14
drop materialized view test_empty_partition_mv1;

-- query 15
drop table test_empty_partition_tbl;

-- query 16
-- name: test_mv_rewrite_with_having
CREATE TABLE IF NOT EXISTS `lineorder` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_linenumber` int(11) NOT NULL COMMENT "",
    `lo_custkey` int(11) NOT NULL COMMENT "",
    `lo_partkey` int(11) NOT NULL COMMENT "",
    `lo_suppkey` int(11) NOT NULL COMMENT "",
    `lo_orderdate` datetime NOT NULL COMMENT "",
    `lo_orderpriority` varchar(16) NOT NULL COMMENT "",
    `lo_shippriority` int(11) NOT NULL COMMENT "",
    `lo_quantity` int(11) NOT NULL COMMENT "",
    `lo_extendedprice` int(11) NOT NULL COMMENT "",
    `lo_ordtotalprice` int(11) NOT NULL COMMENT "",
    `lo_discount` int(11) NOT NULL COMMENT "",
    `lo_revenue` int(11) NOT NULL COMMENT "",
    `lo_supplycost` int(11) NOT NULL COMMENT "",
    `lo_tax` int(11) NOT NULL COMMENT "",
    `lo_commitdate` int(11) NOT NULL COMMENT "",
    `lo_shipmode` varchar(11) NOT NULL COMMENT ""
    ) ENGINE=OLAP
    DUPLICATE KEY(`lo_orderkey`)
    COMMENT "OLAP"
    PARTITION BY RANGE(`lo_orderdate`)
(
    PARTITION p1 VALUES [("1992-01-01 00:00:00"), ("1993-01-01 00:00:00")),
    PARTITION p2 VALUES [("1993-01-01 00:00:00"), ("1994-01-01 00:00:00")),
    PARTITION p3 VALUES [("1994-01-01 00:00:00"), ("1995-01-01 00:00:00")),
    PARTITION p4 VALUES [("1995-01-01 00:00:00"), ("1996-01-01 00:00:00")),
    PARTITION p5 VALUES [("1996-01-01 00:00:00"), ("1997-01-01 00:00:00")),
    PARTITION p6 VALUES [("1997-01-01 00:00:00"), ("1998-01-01 00:00:00")),
    PARTITION p7 VALUES [("1998-01-01 00:00:00"), ("1999-01-01 00:00:00"))
)
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- query 17
INSERT INTO lineorder (lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey
                      , lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice
                      , lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax
                      , lo_commitdate, lo_shipmode)
VALUES
    (1, 1, 101, 201, 301, '1993-01-01 00:00:00', 'HIGH', 1, 10, 100, 90, 0, 90, 50, 5, 19930115, 'AIR'),
    (2, 2, 102, 202, 302, '1994-01-01 00:00:00', 'MEDIUM', 2, 20, 200, 180, 5, 175, 75, 7, 19940116, 'SHIP'),
    (3, 3, 103, 203, 302, '1994-01-01 00:00:00', 'MEDIUM', 2, 20, 200, 180, 5, 175, 75, 7, 19940116, 'SHIP');

-- query 18
CREATE MATERIALIZED VIEW if not exists default_test_mv0
DISTRIBUTED BY RANDOM
REFRESH DEFERRED MANUAL
PROPERTIES (
'replication_num'='1',
'force_external_table_query_rewrite'='true'
) AS select lo_orderdate, count(1) from lineorder where lo_orderkey > 1 and lo_orderkey < 10000 group by lo_orderdate  having count(*) > 1;

-- query 19
refresh materialized view default_test_mv0 with sync mode;

-- query 20
-- @result_contains=default_test_mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select lo_orderdate, count(1) from lineorder where lo_orderkey > 1 and lo_orderkey < 10000 group by lo_orderdate  having count(*) > 1;

-- query 21
select lo_orderdate, count(1) from lineorder where lo_orderkey > 1 and lo_orderkey < 10000 group by lo_orderdate  having count(*) > 1 order by 1;

-- query 22
drop materialized view default_test_mv0;

-- query 23
drop table lineorder;
