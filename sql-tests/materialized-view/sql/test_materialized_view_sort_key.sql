-- Test Objective:
-- 1. Validate MV sort-key definitions and query correctness after refresh.
-- 2. Cover sort-key related DDL and read-path behavior.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_sort_key

-- query 1
CREATE TABLE `test_sort_key_tbl1` (
  `order_number` bigint(20) NULL COMMENT "",
  `date` datetime NULL COMMENT "",
  `sku_number` varchar(65533) NULL COMMENT "",
  `cost` decimal64(18, 0) NULL COMMENT "",
  `price` double NULL COMMENT "",
  `pt` varchar(255) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`order_number`, `date`, `sku_number`)
DISTRIBUTED BY HASH(`order_number`)
PROPERTIES (
"replication_num" = "1"
);

-- query 2
CREATE TABLE `TEST_SORT_KEY_TBL2` (
  `ORDER_NUMBER` bigint(20) NULL COMMENT "",
  `DATE` datetime NULL COMMENT "",
  `SKU_NUMBER` varchar(65533) NULL COMMENT "",
  `COST` decimal64(18, 0) NULL COMMENT "",
  `PRICE` double NULL COMMENT "",
  `PT` varchar(255) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`order_number`, `date`, `sku_number`)
DISTRIBUTED BY HASH(`order_number`)
PROPERTIES (
"replication_num" = "1"
);

-- query 3
insert into test_sort_key_tbl1 values (1, '2023-10-01', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-02', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-01', 'a1', 2.0, 1010.00, '20231001');

-- query 4
insert into TEST_SORT_KEY_TBL2 values (1, '2023-10-01', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-02', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-01', 'a1', 2.0, 1010.00, '20231001');

-- query 5
CREATE MATERIALIZED VIEW test_sort_key_tbl_mv1
DISTRIBUTED BY RANDOM
REFRESH MANUAL
PROPERTIES (
"replication_num" = "1"
)
AS select sum(price), sum(price + 1), sum(cost), `date` from test_sort_key_tbl1 group by `date`;

-- query 6
CREATE MATERIALIZED VIEW test_sort_key_tbl_mv2
DISTRIBUTED BY RANDOM
ORDER BY (`DATE`, SUM_COST)
REFRESH MANUAL
PROPERTIES (
"replication_num" = "1"
)
AS SELECT SUM(PRICE), MAX(PRICE), SUM(COST) AS SUM_COST, `DATE` FROM TEST_SORT_KEY_TBL2 GROUP BY `DATE`;

-- query 7
CREATE MATERIALIZED VIEW TEST_SORT_KEY_TBL_MV3
DISTRIBUTED BY RANDOM
REFRESH MANUAL
PROPERTIES (
"replication_num" = "1"
)
AS select sum(price), sum(price + 1), sum(cost), `date` from test_sort_key_tbl1 group by `date`;

-- query 8
CREATE MATERIALIZED VIEW TEST_SORT_KEY_TBL_MV4
DISTRIBUTED BY RANDOM
ORDER BY (`DATE`, SUM_COST)
REFRESH MANUAL
PROPERTIES (
"replication_num" = "1"
)
AS SELECT SUM(PRICE), MAX(PRICE), SUM(COST) AS SUM_COST, `DATE` FROM TEST_SORT_KEY_TBL2 GROUP BY `DATE`;

-- query 9
refresh materialized view test_sort_key_tbl_mv1 with sync mode;

-- query 10
refresh materialized view test_sort_key_tbl_mv2 with sync mode;

-- query 11
refresh materialized view TEST_SORT_KEY_TBL_MV3 with sync mode;

-- query 12
refresh materialized view TEST_SORT_KEY_TBL_MV4 with sync mode;

-- query 13
select * from test_sort_key_tbl_mv1 order by 1, 2;

-- query 14
select * from test_sort_key_tbl_mv2 order by 1, 2;

-- query 15
select * from TEST_SORT_KEY_TBL_MV3 order by 1, 2;

-- query 16
select * from TEST_SORT_KEY_TBL_MV4 order by 1, 2;

-- query 17
insert into test_sort_key_tbl1 values (1, '2023-10-01', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-02', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-01', 'a1', 2.0, 1010.00, '20231001');

-- query 18
insert into TEST_SORT_KEY_TBL2 values (1, '2023-10-01', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-02', 'a1', 1.0, 100.00, '20231001'), (1, '2023-10-01', 'a1', 2.0, 1010.00, '20231001');

-- query 19
refresh materialized view test_sort_key_tbl_mv1 with sync mode;

-- query 20
refresh materialized view test_sort_key_tbl_mv2 with sync mode;

-- query 21
refresh materialized view TEST_SORT_KEY_TBL_MV3 with sync mode;

-- query 22
refresh materialized view TEST_SORT_KEY_TBL_MV4 with sync mode;

-- query 23
select * from test_sort_key_tbl_mv1 order by 1, 2;

-- query 24
select * from test_sort_key_tbl_mv2 order by 1, 2;

-- query 25
select * from TEST_SORT_KEY_TBL_MV3 order by 1, 2;

-- query 26
select * from TEST_SORT_KEY_TBL_MV4 order by 1, 2;

-- query 27
drop materialized view test_sort_key_tbl_mv1;

-- query 28
drop materialized view test_sort_key_tbl_mv2;

-- query 29
drop materialized view TEST_SORT_KEY_TBL_MV3;

-- query 30
drop materialized view TEST_SORT_KEY_TBL_MV4;
