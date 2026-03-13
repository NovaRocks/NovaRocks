-- Test Objective:
-- 1. Validate partition compensation rewrite on local OLAP base tables.
-- 2. Cover exact-match, range, IN-list, and UNION compensation plans.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_partition_compensate_olap

-- query 1
-- create mv
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
set enable_materialized_view_transparent_union_rewrite = true;

-- query 4
-- test mv with range partition
-- base table with date partition column
CREATE TABLE t1 (
  num int,
  dt date
)
DUPLICATE KEY(`num`)
PARTITION BY RANGE(`dt`)
(
  PARTITION p20200615 VALUES [("2020-06-15 00:00:00"), ("2020-06-16 00:00:00")),
  PARTITION p20200618 VALUES [("2020-06-18 00:00:00"), ("2020-06-19 00:00:00")),
  PARTITION p20200621 VALUES [("2020-06-21 00:00:00"), ("2020-06-22 00:00:00")),
  PARTITION p20200624 VALUES [("2020-06-24 00:00:00"), ("2020-06-25 00:00:00")),
  PARTITION p20200702 VALUES [("2020-07-02 00:00:00"), ("2020-07-03 00:00:00")),
  PARTITION p20200705 VALUES [("2020-07-05 00:00:00"), ("2020-07-06 00:00:00")),
  PARTITION p20200708 VALUES [("2020-07-08 00:00:00"), ("2020-07-09 00:00:00")),
  PARTITION p20200716 VALUES [("2020-07-16 00:00:00"), ("2020-07-17 00:00:00")),
  PARTITION p20200719 VALUES [("2020-07-19 00:00:00"), ("2020-07-20 00:00:00")),
  PARTITION p20200722 VALUES [("2020-07-22 00:00:00"), ("2020-07-23 00:00:00")),
  PARTITION p20200725 VALUES [("2020-07-25 00:00:00"), ("2020-07-26 00:00:00")),
  PARTITION p20200711 VALUES [("2020-07-11 00:00:00"), ("2020-07-12 00:00:00"))
)
DISTRIBUTED BY HASH(`num`);

-- query 5
INSERT INTO t1 VALUES
  (1,"2020-06-15"),(2,"2020-06-18"),(3,"2020-06-21"),(4,"2020-06-24"),
  (1,"2020-07-02"),(2,"2020-07-05"),(3,"2020-07-08"),(4,"2020-07-11"),
  (1,"2020-07-16"),(2,"2020-07-19"),(3,"2020-07-22"),(4,"2020-07-25"),
  (2,"2020-06-15"),(3,"2020-06-18"),(4,"2020-06-21"),(5,"2020-06-24"),
  (2,"2020-07-02"),(3,"2020-07-05"),(4,"2020-07-08"),(5,"2020-07-11");

-- query 6
-- base table with datetime partition column
CREATE TABLE t2 (
  num int,
  dt datetime
)
DUPLICATE KEY(`num`)
PARTITION BY RANGE(`dt`)
(
  PARTITION p20200615 VALUES [("2020-06-15 00:00:00"), ("2020-06-16 00:00:00")),
  PARTITION p20200618 VALUES [("2020-06-18 00:00:00"), ("2020-06-19 00:00:00")),
  PARTITION p20200621 VALUES [("2020-06-21 00:00:00"), ("2020-06-22 00:00:00")),
  PARTITION p20200624 VALUES [("2020-06-24 00:00:00"), ("2020-06-25 00:00:00")),
  PARTITION p20200702 VALUES [("2020-07-02 00:00:00"), ("2020-07-03 00:00:00")),
  PARTITION p20200705 VALUES [("2020-07-05 00:00:00"), ("2020-07-06 00:00:00")),
  PARTITION p20200708 VALUES [("2020-07-08 00:00:00"), ("2020-07-09 00:00:00")),
  PARTITION p20200716 VALUES [("2020-07-16 00:00:00"), ("2020-07-17 00:00:00")),
  PARTITION p20200719 VALUES [("2020-07-19 00:00:00"), ("2020-07-20 00:00:00")),
  PARTITION p20200722 VALUES [("2020-07-22 00:00:00"), ("2020-07-23 00:00:00")),
  PARTITION p20200725 VALUES [("2020-07-25 00:00:00"), ("2020-07-26 00:00:00")),
  PARTITION p20200711 VALUES [("2020-07-11 00:00:00"), ("2020-07-12 00:00:00"))
)
DISTRIBUTED BY HASH(`num`);

-- query 7
INSERT INTO t2 VALUES
  (1,"2020-06-15"),(2,"2020-06-18"),(3,"2020-06-21"),(4,"2020-06-24"),
  (1,"2020-07-02"),(2,"2020-07-05"),(3,"2020-07-08"),(4,"2020-07-11"),
  (1,"2020-07-16"),(2,"2020-07-19"),(3,"2020-07-22"),(4,"2020-07-25"),
  (2,"2020-06-15"),(3,"2020-06-18"),(4,"2020-06-21"),(5,"2020-06-24"),
  (2,"2020-07-02"),(3,"2020-07-05"),(4,"2020-07-08"),(5,"2020-07-11");

-- query 8
-- base table with datetime partition column
CREATE TABLE t3 (
  num int,
  dt datetime
)
DUPLICATE KEY(`num`)
PARTITION BY  date_trunc('day', dt)
DISTRIBUTED BY HASH(`num`);

-- query 9
INSERT INTO t3 VALUES
  (1,"2020-06-15"),(2,"2020-06-18"),(3,"2020-06-21"),(4,"2020-06-24"),
  (1,"2020-07-02"),(2,"2020-07-05"),(3,"2020-07-08"),(4,"2020-07-11"),
  (1,"2020-07-16"),(2,"2020-07-19"),(3,"2020-07-22"),(4,"2020-07-25"),
  (2,"2020-06-15"),(3,"2020-06-18"),(4,"2020-06-21"),(5,"2020-06-24"),
  (2,"2020-07-02"),(3,"2020-07-05"),(4,"2020-07-08"),(5,"2020-07-11");

-- query 10
-- Test partition compensate without partition expression
CREATE MATERIALIZED VIEW test_mv1
PARTITION BY dt
REFRESH DEFERRED MANUAL AS
  SELECT dt,sum(num) FROM t1 GROUP BY dt;

-- query 11
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 12
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt;

-- query 13
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt;

-- query 14
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt;

-- query 15
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt;

-- query 16
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 17
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 18
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 19
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 20
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 21
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 22
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 23
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 GROUP BY dt;

-- query 24
SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 25
SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 26
SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 27
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 28
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 29
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 30
SELECT dt,sum(num) FROM t1 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 31
SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 32
SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 33
SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 34
SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 35
SELECT dt,sum(num) FROM t1 GROUP BY dt order by dt;

-- query 36
-- union rewrite
INSERT INTO t1 VALUES (3, "2020-06-15");

-- query 37
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt;

-- query 38
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 39
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 40
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-21' GROUP BY dt;

-- query 41
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt;

-- query 42
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt;

-- query 43
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 44
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 45
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 46
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 47
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt;

-- query 48
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt;

-- query 49
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-21', '2020-07-25') GROUP BY dt;

-- query 50
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 51
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 GROUP BY dt;

-- query 52
SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 53
SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 54
SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 55
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 56
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 57
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 58
SELECT dt,sum(num) FROM t1 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 59
SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 60
SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 61
SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 62
SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 63
SELECT dt,sum(num) FROM t1 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt order by dt;

-- query 64
SELECT dt,sum(num) FROM t1 GROUP BY dt order by dt;

-- query 65
drop materialized view default_catalog.db_${uuid0}.test_mv1;

-- query 66
-- Test partition compensate with partition expression
CREATE MATERIALIZED VIEW test_mv1
PARTITION BY date_trunc('day', dt)
REFRESH DEFERRED MANUAL AS
  SELECT dt,sum(num) FROM t1 GROUP BY dt;

-- query 67
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 68
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt;

-- query 69
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt;

-- query 70
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt;

-- query 71
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt;

-- query 72
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 73
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 74
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 75
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 76
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 77
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 78
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 79
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 GROUP BY dt;

-- query 80
SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 81
SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 82
SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 83
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 84
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 85
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 86
SELECT dt,sum(num) FROM t1 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 87
SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 88
SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 89
SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 90
SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 91
SELECT dt,sum(num) FROM t1 GROUP BY dt order by dt;

-- query 92
-- union rewrite
INSERT INTO t1 VALUES (3, "2020-06-15");

-- query 93
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt;

-- query 94
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 95
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 96
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('day', dt) ='2020-06-21' GROUP BY dt;

-- query 97
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt;

-- query 98
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt;

-- query 99
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 100
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 101
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 102
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 103
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt;

-- query 104
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt;

-- query 105
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where dt in ('2020-06-15', '2020-06-21', '2020-07-25') GROUP BY dt;

-- query 106
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 107
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t1 GROUP BY dt;

-- query 108
SELECT dt,sum(num) FROM t1 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 109
SELECT dt,sum(num) FROM t1 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 110
SELECT dt,sum(num) FROM t1 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 111
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 112
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 113
SELECT dt,sum(num) FROM t1 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 114
SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 115
SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 116
SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 117
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 118
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 119
SELECT dt,sum(num) FROM t2 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt order by dt;

-- query 120
SELECT dt,sum(num) FROM t2 GROUP BY dt order by dt;

-- query 121
drop materialized view default_catalog.db_${uuid0}.test_mv1;

-- query 122
-- Test partition compensate with partition expression
CREATE MATERIALIZED VIEW test_mv1
PARTITION BY date_trunc('day', dt)
REFRESH DEFERRED MANUAL AS
  SELECT dt,sum(num) FROM t2 GROUP BY dt;

-- query 123
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 124
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt;

-- query 125
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt;

-- query 126
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt;

-- query 127
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt;

-- query 128
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 129
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 130
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 131
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 132
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 133
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 134
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 135
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 GROUP BY dt;

-- query 136
SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 137
SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 138
SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 139
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 140
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 141
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 142
SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 143
SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 144
SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 145
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 146
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 147
SELECT dt,sum(num) FROM t2 GROUP BY dt order by dt;

-- query 148
-- union rewrite
INSERT INTO t2 VALUES (3, "2020-06-15");

-- query 149
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt;

-- query 150
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 151
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 152
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-21' GROUP BY dt;

-- query 153
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt;

-- query 154
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt;

-- query 155
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 156
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 157
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 158
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 159
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt;

-- query 160
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt;

-- query 161
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-21', '2020-07-25') GROUP BY dt;

-- query 162
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 163
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 GROUP BY dt;

-- query 164
SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 165
SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 166
SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 167
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 168
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 169
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 170
SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 171
SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 172
SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 173
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 174
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 175
SELECT dt,sum(num) FROM t2 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt order by dt;

-- query 176
SELECT dt,sum(num) FROM t2 GROUP BY dt order by dt;

-- query 177
drop materialized view default_catalog.db_${uuid0}.test_mv1;

-- query 178
-- Test partition compensate with partition expression
-- TODO: Remove dt(extra column) later.
CREATE MATERIALIZED VIEW test_mv1
PARTITION BY dt
REFRESH DEFERRED MANUAL AS
  SELECT dt, date_trunc('day', dt) as format_dt,sum(num) FROM t2 GROUP BY dt;

-- query 179
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 180
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt;

-- query 181
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt;

-- query 182
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt;

-- query 183
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt;

-- query 184
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 185
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 186
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 187
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 188
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 189
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 190
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 191
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 GROUP BY dt;

-- query 192
SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 193
SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 194
SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 195
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 196
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 197
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 198
SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 199
SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 200
SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 201
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 202
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 203
SELECT dt,sum(num) FROM t2 GROUP BY dt order by dt;

-- query 204
-- union rewrite
INSERT INTO t2 VALUES (3, "2020-06-15");

-- query 205
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt;

-- query 206
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 207
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 208
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-21' GROUP BY dt;

-- query 209
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt;

-- query 210
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt;

-- query 211
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 212
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 213
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 214
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 215
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt;

-- query 216
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt;

-- query 217
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-21', '2020-07-25') GROUP BY dt;

-- query 218
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 219
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t2 GROUP BY dt;

-- query 220
SELECT dt,sum(num) FROM t2 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 221
SELECT dt,sum(num) FROM t2 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 222
SELECT dt,sum(num) FROM t2 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 223
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 224
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 225
SELECT dt,sum(num) FROM t2 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 226
SELECT dt,sum(num) FROM t2 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 227
SELECT dt,sum(num) FROM t2 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 228
SELECT dt,sum(num) FROM t2 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 229
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 230
SELECT dt,sum(num) FROM t2 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 231
SELECT dt,sum(num) FROM t2 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt order by dt;

-- query 232
SELECT dt,sum(num) FROM t2 GROUP BY dt order by dt;

-- query 233
drop materialized view default_catalog.db_${uuid0}.test_mv1;

-- query 234
-- Test partition compensate with partition expression
CREATE MATERIALIZED VIEW test_mv1
PARTITION BY dt
REFRESH DEFERRED MANUAL AS
  SELECT dt,sum(num) FROM t3 GROUP BY dt;

-- query 235
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 236
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt;

-- query 237
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt;

-- query 238
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt;

-- query 239
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt;

-- query 240
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 241
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 242
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 243
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 244
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 245
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 246
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 247
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 GROUP BY dt;

-- query 248
SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 249
SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 250
SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 251
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 252
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 253
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 254
SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 255
SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 256
SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 257
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 258
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 259
SELECT dt,sum(num) FROM t3 GROUP BY dt order by dt;

-- query 260
-- union rewrite
INSERT INTO t3 VALUES (3, "2020-06-15");

-- query 261
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt;

-- query 262
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 263
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 264
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-21' GROUP BY dt;

-- query 265
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt;

-- query 266
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt;

-- query 267
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 268
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 269
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 270
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 271
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt;

-- query 272
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt;

-- query 273
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-21', '2020-07-25') GROUP BY dt;

-- query 274
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 275
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 GROUP BY dt;

-- query 276
SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 277
SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 278
SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 279
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 280
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 281
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 282
SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 283
SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 284
SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 285
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 286
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 287
SELECT dt,sum(num) FROM t3 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt order by dt;

-- query 288
SELECT dt,sum(num) FROM t3 GROUP BY dt order by dt;

-- query 289
drop materialized view default_catalog.db_${uuid0}.test_mv1;

-- query 290
-- Test partition compensate with partition expression
-- TODO: Remove dt(extra column) later.
CREATE MATERIALIZED VIEW test_mv1
PARTITION BY date_trunc('day', dt)
REFRESH DEFERRED MANUAL AS
  SELECT dt, sum(num) FROM t3 GROUP BY dt;

-- query 291
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 292
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt;

-- query 293
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt;

-- query 294
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt;

-- query 295
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt;

-- query 296
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 297
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 298
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 299
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 300
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 301
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 302
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 303
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 GROUP BY dt;

-- query 304
SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 305
SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 306
SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 307
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 308
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 309
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 310
SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 311
SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 312
SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 313
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 314
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 315
SELECT dt,sum(num) FROM t3 GROUP BY dt order by dt;

-- query 316
-- union rewrite
INSERT INTO t3 VALUES (3, "2020-06-15");

-- query 317
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt;

-- query 318
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt;

-- query 319
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt;

-- query 320
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-21' GROUP BY dt;

-- query 321
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt;

-- query 322
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt;

-- query 323
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt;

-- query 324
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt;

-- query 325
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt;

-- query 326
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt;

-- query 327
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt;

-- query 328
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt;

-- query 329
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-21', '2020-07-25') GROUP BY dt;

-- query 330
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt;

-- query 331
-- @result_contains_any=test_mv1
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(num) FROM t3 GROUP BY dt;

-- query 332
SELECT dt,sum(num) FROM t3 where dt='2020-06-15' GROUP BY dt order by dt;

-- query 333
SELECT dt,sum(num) FROM t3 where dt !='2020-06-15' GROUP BY dt order by dt;

-- query 334
SELECT dt,sum(num) FROM t3 where dt>='2020-06-15' GROUP BY dt order by dt;

-- query 335
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' GROUP BY dt order by dt;

-- query 336
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt < '2020-07-22' GROUP BY dt order by dt;

-- query 337
SELECT dt,sum(num) FROM t3 where dt>'2020-06-15' and dt <= '2020-07-22' GROUP BY dt order by dt;

-- query 338
SELECT dt,sum(num) FROM t3 where (dt>'2020-06-15' and dt <= '2020-06-22') or dt>'2020-07-01' GROUP BY dt order by dt;

-- query 339
SELECT dt,sum(num) FROM t3 where dt in ('2020-06-15', '2020-06-22', '2020-07-01') GROUP BY dt order by dt;

-- query 340
SELECT dt,sum(num) FROM t3 where date_trunc('day', dt) ='2020-06-15' GROUP BY dt order by dt;

-- query 341
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-06-01' GROUP BY dt order by dt;

-- query 342
SELECT dt,sum(num) FROM t3 where date_trunc('month', dt) ='2020-07-01' GROUP BY dt order by dt;

-- query 343
SELECT dt,sum(num) FROM t3 where dt between '2020-06-15' and '2020-07-01' GROUP BY dt order by dt;

-- query 344
SELECT dt,sum(num) FROM t3 GROUP BY dt order by dt;

-- query 345
drop materialized view default_catalog.db_${uuid0}.test_mv1;

-- query 346
drop table t1 force;

-- query 347
drop table t2 force;

-- query 348
drop table t3 force;

-- query 349
drop database db_${uuid0} force;
