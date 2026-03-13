-- Test Objective:
-- 1. Validate UNION ALL queries can rewrite through materialized views when eligible.
-- 2. Cover union-all rewrite correctness on local OLAP tables.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_union_all_rewrite

-- query 1
CREATE TABLE `mt1` (
 k1 INT,
 k2 string,
 v1 INT,
 v2 INT
) ENGINE=OLAP
PARTITION BY RANGE(`k1`)
(
  PARTITION `p1` VALUES LESS THAN ('3'),
  PARTITION `p2` VALUES LESS THAN ('6'),
  PARTITION `p3` VALUES LESS THAN ('9')
)
DISTRIBUTED BY HASH(`k1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 2
INSERT INTO mt1 values (1,'a',1,1), (2,'aa',1,2),  (3,'a',1,3), (4,'aa',1,4), (5,'aa',1,5), (6,'aa',1,6);

-- query 3
set enable_materialized_view_transparent_union_rewrite=false;

-- query 4
CREATE MATERIALIZED VIEW mv0
PARTITION BY (k1)
DISTRIBUTED BY HASH(k1)
REFRESH DEFERRED MANUAL
AS SELECT k1,k2, v1,v2 from mt1 where v2 != 2;

-- query 5
REFRESH MATERIALIZED VIEW mv0 PARTITION START ('1') END ('3') with sync mode;

-- query 6
-- default mode
set materialized_view_union_rewrite_mode = 0;

-- query 7
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from mt1 where k1 < 3 and v2 != 2;

-- query 8
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select * from mt1 where k1 = 1 and v2 >= 4;

-- query 9
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1;

-- query 10
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT distinct k1,k2, v1,v2 from mt1;

-- query 11
-- @result_not_contains=mv0
-- @result_not_contains=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 WHERE k1 <6 and v2 != 3;

-- query 12
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where k1<6 and k2 = 'a' and v2 != 4;

-- query 13
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where k1>0 and k2 = 'a' and v2 >= 4;

-- query 14
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 != 4;

-- query 15
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 = 2;

-- query 16
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 <= 2;

-- query 17
select * from mt1 where k1 < 3 and v2 != 2 order by 1;

-- query 18
select * from mt1 where k1 = 1 and v2 >= 4  order by 1;

-- query 19
select * from mt1 where k1<6 and k2  = 'a' and v2 != 4 order by 1;

-- query 20
select * from mt1 where k1>0 and k2  = 'a' and v2 >= 4  order by 1;

-- query 21
SELECT k1,k2, v1,v2 from mt1 where v2 != 4 order by 1;

-- query 22
SELECT k1,k2, v1,v2 from mt1 order by 1;

-- query 23
SELECT DISTINCT k1,k2, v1,v2 from mt1 order by 1;

-- query 24
SELECT k1,k2, v1,v2 from mt1 where v2 = 2 order by 1;

-- query 25
SELECT k1,k2, v1,v2 from mt1 where v2 <= 2 order by 1;

-- query 26
SELECT k1,k2, v1,v2 from mt1 WHERE k1 <6 and v2 >= 4  order by 1;

-- query 27
-- mode 1
set materialized_view_union_rewrite_mode = 1;

-- query 28
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1;

-- query 29
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT distinct k1,k2, v1,v2 from mt1;

-- query 30
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 WHERE k1 <6 and v2 >= 4;

-- query 31
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where k1<6 and k2 = 'a' and v2 != 4;

-- query 32
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where k1>0 and k2 = 'a' and v2 >= 4;

-- query 33
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 != 4;

-- query 34
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 = 2;

-- query 35
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 <= 2;

-- query 36
select * from mt1 where k1 < 3 and v2 != 2 order by 1;

-- query 37
select * from mt1 where k1 = 1 and v2 >= 4  order by 1;

-- query 38
select * from mt1 where k1<6 and k2  = 'a' and v2 != 4 order by 1;

-- query 39
select * from mt1 where k1>0 and k2  = 'a' and v2 >= 4  order by 1;

-- query 40
SELECT k1,k2, v1,v2 from mt1 where v2 != 4 order by 1;

-- query 41
SELECT k1,k2, v1,v2 from mt1 order by 1;

-- query 42
SELECT DISTINCT k1,k2, v1,v2 from mt1 order by 1;

-- query 43
SELECT k1,k2, v1,v2 from mt1 where v2 = 2 order by 1;

-- query 44
SELECT k1,k2, v1,v2 from mt1 where v2 <= 2 order by 1;

-- query 45
SELECT k1,k2, v1,v2 from mt1 WHERE k1 <6 and v2 >= 4  order by 1;

-- query 46
-- mode 2
set materialized_view_union_rewrite_mode = 2;

-- query 47
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where k1<6 and k2 = 'a' and v2 != 4;

-- query 48
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where k1>0 and k2 = 'a' and v2 >= 4;

-- query 49
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 WHERE k1 <6 and v2 != 3;

-- query 50
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 != 4;

-- query 51
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1;

-- query 52
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT distinct k1,k2, v1,v2 from mt1;

-- query 53
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 = 2;

-- query 54
-- @result_contains_any=mv0
-- @result_contains_any=UNION
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k1,k2, v1,v2 from mt1 where v2 <= 2;

-- query 55
select * from mt1 where k1 < 3 and v2 != 2 order by 1;

-- query 56
select * from mt1 where k1 = 1 and v2 >= 4  order by 1;

-- query 57
select * from mt1 where k1<6 and k2 = 'a' and v2 != 4 order by 1;

-- query 58
select * from mt1 where k1>0 and k2 = 'a' and v2 >= 4  order by 1;

-- query 59
SELECT k1,k2, v1,v2 from mt1 where v2 != 4 order by 1;

-- query 60
SELECT k1,k2, v1,v2 from mt1 order by 1;

-- query 61
SELECT DISTINCT k1,k2, v1,v2 from mt1 order by 1;

-- query 62
SELECT k1,k2, v1,v2 from mt1 where v2 = 2 order by 1;

-- query 63
SELECT k1,k2, v1,v2 from mt1 where v2 <= 2 order by 1;

-- query 64
SELECT k1,k2, v1,v2 from mt1 WHERE k1 <6 and v2 >= 4 order by 1;

-- query 65
drop materialized view mv0;

-- query 66
CREATE MATERIALIZED VIEW mv0
PARTITION BY (k1)
DISTRIBUTED BY HASH(k1)
REFRESH DEFERRED MANUAL
AS SELECT k1, k2, sum(v1), sum(v2)
from mt1 where k2 != 'a'
group by k1, k2 ;

-- query 67
REFRESH MATERIALIZED VIEW mv0 PARTITION START ('1') END ('3') with sync mode;

-- query 68
set materialized_view_union_rewrite_mode = 0;

-- query 69
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' group by k1;

-- query 70
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k1 < 5 group by k1;

-- query 71
-- @result_not_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k1 <= 3 group by k1;

-- query 72
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 >= 1 group by k1;

-- query 73
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 > 1 group by k1;

-- query 74
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 != 1 group by k1;

-- query 75
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' group by k1 order by 1;

-- query 76
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 >= 1 group by k1 order by 1;

-- query 77
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 > 1 group by k1 order by 1;

-- query 78
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 != 1 group by k1 order by 1;

-- query 79
select k1, sum(v1), sum(v2) from mt1 where k1 < 5 group by k1 order by 1;

-- query 80
select k1, sum(v1), sum(v2) from mt1 where k1 <= 3 group by k1 order by 1;

-- query 81
set materialized_view_union_rewrite_mode = 1;

-- query 82
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' group by k1;

-- query 83
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k1 < 5 group by k1;

-- query 84
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k1 <= 3 group by k1;

-- query 85
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 >= 1 group by k1;

-- query 86
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 > 1 group by k1;

-- query 87
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 != 1 group by k1;

-- query 88
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' group by k1 order by 1;

-- query 89
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 >= 1 group by k1 order by 1;

-- query 90
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 > 1 group by k1 order by 1;

-- query 91
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 != 1 group by k1 order by 1;

-- query 92
select k1, sum(v1), sum(v2) from mt1 where k1 < 5 group by k1 order by 1;

-- query 93
select k1, sum(v1), sum(v2) from mt1 where k1 <= 3 group by k1 order by 1;

-- query 94
set materialized_view_union_rewrite_mode = 2;

-- query 95
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' group by k1;

-- query 96
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 >= 1 group by k1;

-- query 97
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 > 1 group by k1;

-- query 98
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 != 1 group by k1;

-- query 99
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k1 < 5 group by k1;

-- query 100
-- @result_contains=mv0
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, sum(v1), sum(v2) from mt1 where k1 <= 3 group by k1;

-- query 101
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' group by k1 order by 1;

-- query 102
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 >= 1 group by k1 order by 1;

-- query 103
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 > 1 group by k1 order by 1;

-- query 104
select k1, sum(v1), sum(v2) from mt1 where k2 != 'a' and k1 != 1 group by k1 order by 1;

-- query 105
select k1, sum(v1), sum(v2) from mt1 where k1 < 5 group by k1 order by 1;

-- query 106
select k1, sum(v1), sum(v2) from mt1 where k1 <= 3 group by k1 order by 1;

-- query 107
drop materialized view mv0;

-- query 108
drop table mt1;
