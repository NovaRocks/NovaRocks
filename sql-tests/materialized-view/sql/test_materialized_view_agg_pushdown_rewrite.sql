-- Test Objective:
-- 1. Validate aggregate pushdown rewrite can hit MV plans on grouped queries.
-- 2. Cover rewrite stability before and after base-table changes.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_agg_pushdown_rewrite

-- query 1
CREATE TABLE IF NOT EXISTS `lineorder` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_linenumber` int(11) NOT NULL COMMENT "",
    `lo_custkey` int(11) NOT NULL COMMENT "",
    `lo_partkey` int(11) NOT NULL COMMENT "",
    `lo_suppkey` int(11) NOT NULL COMMENT "",
    `lo_orderdate` date NOT NULL COMMENT "",
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
    PARTITION p1 VALUES [("1992-01-01"), ("1993-01-01")),
    PARTITION p2 VALUES [("1993-01-01"), ("1994-01-01")),
    PARTITION p3 VALUES [("1994-01-01"), ("1995-01-01")),
    PARTITION p4 VALUES [("1995-01-01"), ("1996-01-01")),
    PARTITION p5 VALUES [("1996-01-01"), ("1997-01-01")),
    PARTITION p6 VALUES [("1997-01-01"), ("1998-01-01")),
    PARTITION p7 VALUES [("1998-01-01"), ("1999-01-01"))
) DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48;

-- query 2
CREATE TABLE IF NOT EXISTS `customer` (
    `c_custkey` int(11) NOT NULL COMMENT "",
    `c_name` varchar(26) NOT NULL COMMENT "",
    `c_address` varchar(41) NOT NULL COMMENT "",
    `c_city` varchar(11) NOT NULL COMMENT "",
    `c_nation` varchar(16) NOT NULL COMMENT "",
    `c_region` varchar(13) NOT NULL COMMENT "",
    `c_phone` varchar(16) NOT NULL COMMENT "",
    `c_mktsegment` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c_custkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 12;

-- query 3
CREATE TABLE IF NOT EXISTS `dates` (
    `d_datekey` int(11) NOT NULL COMMENT "",
    `d_date` varchar(20) NOT NULL COMMENT "",
    `d_dayofweek` varchar(10) NOT NULL COMMENT "",
    `d_month` varchar(11) NOT NULL COMMENT "",
    `d_year` int(11) NOT NULL COMMENT "",
    `d_yearmonthnum` int(11) NOT NULL COMMENT "",
    `d_yearmonth` varchar(9) NOT NULL COMMENT "",
    `d_daynuminweek` int(11) NOT NULL COMMENT "",
    `d_daynuminmonth` int(11) NOT NULL COMMENT "",
    `d_daynuminyear` int(11) NOT NULL COMMENT "",
    `d_monthnuminyear` int(11) NOT NULL COMMENT "",
    `d_weeknuminyear` int(11) NOT NULL COMMENT "",
    `d_sellingseason` varchar(14) NOT NULL COMMENT "",
    `d_lastdayinweekfl` int(11) NOT NULL COMMENT "",
    `d_lastdayinmonthfl` int(11) NOT NULL COMMENT "",
    `d_holidayfl` int(11) NOT NULL COMMENT "",
    `d_weekdayfl` int(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`d_datekey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`d_datekey`) BUCKETS 1;

-- query 4
CREATE TABLE IF NOT EXISTS `supplier` (
    `s_suppkey` int(11) NOT NULL COMMENT "",
    `s_name` varchar(26) NOT NULL COMMENT "",
    `s_address` varchar(26) NOT NULL COMMENT "",
    `s_city` varchar(11) NOT NULL COMMENT "",
    `s_nation` varchar(16) NOT NULL COMMENT "",
    `s_region` varchar(13) NOT NULL COMMENT "",
    `s_phone` varchar(16) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`s_suppkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12;

-- query 5
CREATE TABLE IF NOT EXISTS `part` (
    `p_partkey` int(11) NOT NULL COMMENT "",
    `p_name` varchar(23) NOT NULL COMMENT "",
    `p_mfgr` varchar(7) NOT NULL COMMENT "",
    `p_category` varchar(8) NOT NULL COMMENT "",
    `p_brand` varchar(10) NOT NULL COMMENT "",
    `p_color` varchar(12) NOT NULL COMMENT "",
    `p_type` varchar(26) NOT NULL COMMENT "",
    `p_size` int(11) NOT NULL COMMENT "",
    `p_container` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`p_partkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 12;

-- query 6
INSERT INTO lineorder (lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey
                      , lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice
                      , lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax
                      , lo_commitdate, lo_shipmode)
VALUES
    (1, 1, 1, 1, 1, '1993-01-01', 'HIGH', 1, 10, 100, 90, 0, 90, 50, 5, 19930115, 'AIR'),
    (1, 1, 1, 1, 1, '1993-01-01', 'MEDIUM', 1, 10, 100, 90, 0, 90, 50, 5, 19930115, 'AIR'),
    (2, 2, 2, 2, 2, '1994-01-01', 'MEDIUM', 2, 20, 200, 180, 5, 175, 75, 7, 19940116, 'SHIP'),
    (3, 3, 3, 3, 2, '1995-01-01', 'MEDIUM', 2, 20, 200, 180, 5, 175, 75, 7, 19940116, 'SHIP')
;

-- query 7
INSERT INTO customer (c_custkey, c_name, c_address, c_city, c_nation
    , c_region, c_phone, c_mktsegment)
VALUES
(1, 'Customer1', '123 Main St', 'City1', 'Nation1', 'Region1', '123-456-7890', 'Segment1'),
(2, 'Customer2', '456 Oak St', 'City2', 'Nation2', 'Region2', '987-654-3210', 'Segment2'),
(3, 'Customer3', '789 Pine St', 'City3', 'Nation3', 'Region3', '555-123-4567', 'Segment3');

-- query 8
INSERT INTO dates (d_datekey, d_date, d_dayofweek, d_month, d_year
  , d_yearmonthnum, d_yearmonth, d_daynuminweek, d_daynuminmonth, d_daynuminyear
  , d_monthnuminyear, d_weeknuminyear, d_sellingseason, d_lastdayinweekfl, d_lastdayinmonthfl
  , d_holidayfl, d_weekdayfl)
VALUES (1, '1993-01-01', 'Wednesday', 'December', 2023, 202312, '2023-12', 3, 7, 341, 12, 49, 'Holiday Season', 0, 0, 1, 1),
(2, '1993-01-02', 'Wednesday', 'December', 2023, 202312, '2023-12', 3, 7, 341, 12, 49, 'Holiday Season', 0, 0, 1, 1),
(3, '1994-01-01', 'Wednesday', 'December', 2023, 202312, '2023-12', 3, 7, 341, 12, 49, 'Holiday Season', 0, 0, 1, 1);

-- query 9
INSERT INTO supplier (s_suppkey, s_name, s_address, s_city, s_nation, s_region, s_phone)
VALUES (1, 'Supplier1', '123 Main St', 'City1', 'Nation1', 'Region1', '123-456-7890');

-- query 10
INSERT INTO part (p_partkey, p_name, p_mfgr, p_category, p_brand, p_color, p_type, p_size, p_container)
VALUES (1, 'Part1', 'Manufr1', 'Catey1', 'Brand1', 'Red', 'Type1', 10, 'Container1');

-- query 11
set enable_materialized_view_agg_pushdown_rewrite=true;

-- query 12
-- - test simple case
CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), hll_union(hll_hash(LO_REVENUE)), percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE)
from lineorder l group by LO_ORDERDATE;

-- query 13
refresh materialized view mv0 with sync mode;

-- query 14
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 15
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 16
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 17
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 18
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1;

-- query 19
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 20
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 21
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 22
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 23
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;

-- query 24
drop materialized view mv0;

-- query 25
CREATE MATERIALIZED VIEW mv1 REFRESH MANUAL AS
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), bitmap_union(to_bitmap(LO_REVENUE + 1)), hll_union(hll_hash(LO_REVENUE + 1)), percentile_union(percentile_hash(LO_REVENUE + 1)), any_value(LO_REVENUE + 1), bitmap_agg(LO_REVENUE + 1), array_agg_distinct(LO_REVENUE + 1)
from lineorder l group by LO_ORDERDATE;

-- query 26
refresh materialized view mv1 with sync mode;

-- query 27
-- @result_not_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 28
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 29
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), sum(LO_REVENUE + 1), max(LO_REVENUE + 1) , sum(LO_REVENUE + 1), max(LO_REVENUE + 1), count(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 30
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), bitmap_union_count(to_bitmap(LO_REVENUE + 1)), approx_count_distinct(LO_REVENUE + 1), PERCENTILE_APPROX(LO_REVENUE + 1, 0.5), PERCENTILE_APPROX(LO_REVENUE + 1, 0.7), sum(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 31
-- @result_contains=mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), bitmap_union_count(to_bitmap(LO_REVENUE + 1)), approx_count_distinct(LO_REVENUE + 1), PERCENTILE_APPROX(LO_REVENUE + 1, 0.5), PERCENTILE_APPROX(LO_REVENUE + 1, 0.7), sum(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE + 1) > 1;

-- query 32
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;;

-- query 33
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;;

-- query 34
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), sum(LO_REVENUE + 1), max(LO_REVENUE + 1) , sum(LO_REVENUE + 1), max(LO_REVENUE + 1), count(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 35
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), bitmap_union_count(to_bitmap(LO_REVENUE + 1)), approx_count_distinct(LO_REVENUE + 1), PERCENTILE_APPROX(LO_REVENUE + 1, 0.5), PERCENTILE_APPROX(LO_REVENUE + 1, 0.7), sum(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 36
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1), min(LO_REVENUE + 1), bitmap_union_count(to_bitmap(LO_REVENUE + 1)), approx_count_distinct(LO_REVENUE + 1), PERCENTILE_APPROX(LO_REVENUE + 1, 0.5), PERCENTILE_APPROX(LO_REVENUE + 1, 0.7), sum(distinct LO_REVENUE + 1), count(distinct LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE + 1) > 1 order by LO_ORDERDATE;

-- query 37
drop materialized view mv1;

-- query 38
CREATE MATERIALIZED VIEW mv2 REFRESH MANUAL AS
select LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), hll_union(hll_hash(LO_REVENUE)), percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE)
from lineorder l GROUP BY LO_ORDERDATE, lo_custkey, lo_partkey, lo_suppkey;

-- query 39
refresh materialized view mv2 with sync mode;

-- query 40
-- @result_contains=mv2
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE) FROM lineorder AS l INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY INNER JOIN dates AS d ON l.lo_orderdate = d.d_date group by LO_ORDERDATE;

-- query 41
-- @result_contains=mv2
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE)  FROM lineorder AS l INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY INNER JOIN dates AS d ON l.lo_orderdate = d.d_date group by LO_ORDERDATE;

-- query 42
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE) FROM lineorder AS l INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY INNER JOIN dates AS d ON l.lo_orderdate = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 43
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE)  FROM lineorder AS l INNER JOIN customer AS c ON c.C_CUSTKEY = l.LO_CUSTKEY INNER JOIN supplier AS s ON s.S_SUPPKEY = l.LO_SUPPKEY INNER JOIN part AS p ON p.P_PARTKEY = l.LO_PARTKEY INNER JOIN dates AS d ON l.lo_orderdate = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 44
drop materialized view mv2;

-- query 45
-- - test partition mv
CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL
PARTITION BY (LO_ORDERDATE)
AS
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), hll_union(hll_hash(LO_REVENUE)), percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE)
from lineorder l group by LO_ORDERDATE;

-- query 46
refresh materialized view mv0 with sync mode;

-- query 47
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 48
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 49
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 50
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 51
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1;

-- query 52
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 53
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 54
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 55
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 56
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;

-- query 57
-- test table with partition predciates
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 58
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 59
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 60
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 61
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;

-- query 62
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 63
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 64
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 65
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 66
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;

-- query 67
-- test with no agg functions
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 68
select LO_ORDERDATE from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 69
-- test with more group by keys
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE, d_year order by LO_ORDERDATE;

-- query 70
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 71
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 72
select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE, d_year order by LO_ORDERDATE;

-- query 73
select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 74
select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 75
-- test union rewrite
INSERT INTO lineorder (lo_orderkey, lo_linenumber, lo_custkey, lo_partkey, lo_suppkey
                      , lo_orderdate, lo_orderpriority, lo_shippriority, lo_quantity, lo_extendedprice
                      , lo_ordtotalprice, lo_discount, lo_revenue, lo_supplycost, lo_tax
                      , lo_commitdate, lo_shipmode)
VALUES
    (3, 3, 3, 3, 2, '1995-01-01', 'MEDIUM', 2, 20, 200, 180, 5, 175, 75, 7, 19940116, 'SHIP')
;

-- query 76
-- @result_not_contains=mv0
-- @result_not_contains=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 77
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 78
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 79
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 80
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;

-- query 81
select LO_ORDERDATE, sum(LO_REVENUE + 1), max(LO_REVENUE + 1) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 82
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 83
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 84
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 85
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;

-- query 86
-- test with no agg functions
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 87
select LO_ORDERDATE from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 88
-- test with more group by keys
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE, d_year order by LO_ORDERDATE;

-- query 89
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 90
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 91
select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by LO_ORDERDATE, d_year order by LO_ORDERDATE;

-- query 92
select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 93
select d_year, LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date where l.LO_ORDERDATE >= '1993-01-01' group by d_year, LO_ORDERDATE order by LO_ORDERDATE;

-- query 94
drop materialized view mv0;

-- query 95
-- - test simple case
CREATE MATERIALIZED VIEW mv0 REFRESH MANUAL AS
select LO_ORDERDATE, sum(LO_REVENUE), count(LO_REVENUE), sum(lo_suppkey), count(lo_suppkey), max(LO_REVENUE), min(LO_REVENUE), bitmap_union(to_bitmap(LO_REVENUE)), hll_union(hll_hash(LO_REVENUE)), percentile_union(percentile_hash(LO_REVENUE)), any_value(LO_REVENUE), bitmap_agg(LO_REVENUE), array_agg_distinct(LO_REVENUE)
from lineorder l group by LO_ORDERDATE;

-- query 96
refresh materialized view mv0 with sync mode;

-- query 97
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), avg(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 98
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), avg(LO_REVENUE), avg(lo_suppkey), sum(LO_REVENUE), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 99
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), avg(LO_REVENUE), avg(lo_suppkey), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE;

-- query 100
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), avg(LO_REVENUE), avg(lo_suppkey), min(LO_REVENUE), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1;

-- query 101
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), avg(LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 102
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), sum(LO_REVENUE), avg(LO_REVENUE), avg(lo_suppkey), max(LO_REVENUE) , sum(LO_REVENUE), max(LO_REVENUE), count(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 103
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE), avg(LO_REVENUE), avg(lo_suppkey), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE order by LO_ORDERDATE;

-- query 104
select LO_ORDERDATE, sum(LO_REVENUE), max(LO_REVENUE), min(LO_REVENUE),  avg(LO_REVENUE), avg(lo_suppkey), bitmap_union_count(to_bitmap(LO_REVENUE)), approx_count_distinct(LO_REVENUE), PERCENTILE_APPROX(LO_REVENUE, 0.5), PERCENTILE_APPROX(LO_REVENUE, 0.7), sum(distinct LO_REVENUE), count(distinct LO_REVENUE) from lineorder l join dates d on l.LO_ORDERDATE = d.d_date group by LO_ORDERDATE HAVING sum(LO_REVENUE) > 1 order by LO_ORDERDATE;

-- query 105
-- test push down with decimal types
CREATE TABLE `test_pt8` (
  `id` bigint(20) NULL COMMENT "id",
  `pt` date NOT NULL COMMENT "",
  `gmv` bigint(20) NULL COMMENT "gmv",
  `gmv2` bigint(20) NULL COMMENT "gmv2"
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
PARTITION BY date_trunc('day', pt)
DISTRIBUTED BY HASH(`pt`)
PROPERTIES (
    "replication_num" = "1"
);

-- query 106
insert into test_pt8 values(1,'20241126',1,2);

-- query 107
CREATE TABLE `test_pt9` (
  `id` bigint(20) NULL COMMENT "id",
  `pt` date NOT NULL COMMENT "",
  `name` varchar(20) NULL COMMENT "gmv"
) ENGINE=OLAP
DUPLICATE KEY(`id`)
COMMENT "OLAP"
PARTITION BY date_trunc('day', pt)
DISTRIBUTED BY HASH(`pt`)
PROPERTIES (
    "replication_num" = "1"
);

-- query 108
insert into test_pt9 values(1,'20241126','a');

-- query 109
CREATE MATERIALIZED VIEW `test_mv1`
PARTITION BY (`pt`)
DISTRIBUTED BY HASH(id) BUCKETS 1
REFRESH DEFERRED ASYNC START("2024-11-22 17:34:45") EVERY(INTERVAL 1 MINUTE)
PROPERTIES (
    "query_rewrite_consistency" = "LOOSE",
    "replication_num" = "1"
)
AS
SELECT  `id`,`pt`,SUM((`gmv` + `gmv2`) * 0.01) AS `sum_channel_direct_indirect_gmv`
FROM `test_pt8`
GROUP BY  `id`,`pt`;

-- query 110
REFRESH MATERIALIZED VIEW `test_mv1` WITH SYNC MODE;

-- query 111
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT SUM((gmv+gmv2)*0.01) FROM test_pt8 WHERE pt = '20241126' AND id IN ( SELECT id FROM test_pt9 WHERE id = '1');

-- query 112
SELECT SUM((gmv+gmv2)*0.01) FROM test_pt8 WHERE pt = '20241126' AND id IN ( SELECT id FROM test_pt9 WHERE id = '1');
