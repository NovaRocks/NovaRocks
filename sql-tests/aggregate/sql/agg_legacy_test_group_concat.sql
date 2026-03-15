-- Migrated from dev/test/sql/test_agg_function/R/test_group_concat
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: testGroupConcat
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE IF NOT EXISTS `lineorder` (
    `lo_orderkey` int(11) NOT NULL COMMENT "",
    `lo_shipmode` varchar(11) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`lo_orderkey`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`lo_orderkey`) BUCKETS 48
PROPERTIES (
    "replication_num" = "1"
);

-- query 3
USE ${case_db};
SELECT GROUP_CONCAT(lo_shipmode) orgs FROM lineorder WHERE 1 = 2;

-- query 4
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `ss` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into ss values (1,"Tom","English",90);

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into ss values (1,"Tom","Math",80);

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"Tom","English",NULL);

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"Tom",NULL,NULL);

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"May",NULL,NULL);

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"Ti","English",98);

-- query 11
-- @skip_result_check=true
USE ${case_db};
insert into ss values (4,NULL,NULL,NULL);

-- query 12
-- @skip_result_check=true
USE ${case_db};
insert into ss values (NULL,NULL,NULL,NULL);

-- query 13
-- @skip_result_check=true
USE ${case_db};
insert into ss values (NULL,"Ti","物理Phy",99);

-- query 14
-- @skip_result_check=true
USE ${case_db};
insert into ss values (11,"张三此地无银三百两","英文English",98);

-- query 15
-- @skip_result_check=true
USE ${case_db};
insert into ss values (11,"张三掩耳盗铃","Math数学欧拉方程",78);

-- query 16
-- @skip_result_check=true
USE ${case_db};
insert into ss values (12,"李四大闹天空","英语外语美誉",NULL);

-- query 17
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"王武程咬金","语文北京上海",22);

-- query 18
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"欧阳诸葛方程","数学大不列颠",NULL);

-- query 19
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct id), group_concat(name order by 1) from ss group by id order by 1;

-- query 20
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by id;

-- query 21
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss group by id order by 1;

-- query 22
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1, 2) from ss group by id order by 1;

-- query 23
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), group_concat(distinct score order by 1) from ss group by id order by 1;

-- query 24
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by 1;

-- query 25
USE ${case_db};
select group_concat(distinct name,subject order by 1,score) from ss group by id order by 1;

-- query 26
USE ${case_db};
select group_concat(distinct name,subject order by score, 1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 27
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct name,subject order by score,4,2,1) from ss group by id order by 1;

-- query 28
USE ${case_db};
select group_concat(distinct name,subject order by score,4.00, 1,2) from ss group by id order by 1;

-- query 29
USE ${case_db};
select group_concat(distinct name,null order by score,1,4.00) from ss group by id order by 1;

-- query 30
USE ${case_db};
select group_concat(distinct name,subject order by 1,2, null) from ss group by id order by 1;

-- query 31
USE ${case_db};
select group_concat(distinct null order by score,4.00) from ss group by id order by 1;

-- query 32
USE ${case_db};
select group_concat(distinct name, score order by score,4.00, 1),group_concat(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1;

-- query 33
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct score order by 1,2) from ss group by id order by 1;

-- query 34
USE ${case_db};
select group_concat(distinct score order by 1,name) from ss group by id order by 1;

-- query 35
USE ${case_db};
select group_concat(distinct 1,2 order by 1,2) from ss group by id order by 1;

-- query 36
USE ${case_db};
select group_concat(distinct 1,2 order by score,2) from ss group by id order by 1;

-- query 37
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct 3.1323,subject order by 1, 2,-20) from ss group by id order by 1;

-- query 38
USE ${case_db};
select group_concat( name,subject order by 1,2), count(distinct id), max(score) from ss group by id order by 1;

-- query 39
USE ${case_db};
select group_concat( name,subject order by 1,score), count(distinct id), max(score)  from ss group by id order by 1;

-- query 40
USE ${case_db};
select group_concat( name,subject order by score,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 41
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat( name,subject order by score,4,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 42
USE ${case_db};
select group_concat( name,subject order by score,4.00,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 43
USE ${case_db};
select group_concat( name,null order by score,4.00) from ss group by id order by 1;

-- query 44
USE ${case_db};
select group_concat( name,subject order by 1,2, null) from ss group by id order by 1;

-- query 45
USE ${case_db};
select group_concat( null order by score,4.00) from ss group by id order by 1;

-- query 46
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat( score order by 1,2) from ss group by id order by 1;

-- query 47
USE ${case_db};
select group_concat( score order by 1,name) from ss group by id order by 1;

-- query 48
USE ${case_db};
select group_concat( 1,2 order by 1,2) from ss group by id order by 1;

-- query 49
USE ${case_db};
select group_concat( 1,2 order by score,2) from ss group by id order by 1;

-- query 50
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat( 3.1323,subject order by 1,2,-20) from ss group by id order by 1;

-- query 51
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct id), group_concat(name order by 1) from ss order by 1;

-- query 52
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 53
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss order by 1;

-- query 54
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1,2) from ss order by 1;

-- query 55
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), group_concat(distinct score order by 1) from ss order by 1;

-- query 56
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 57
USE ${case_db};
select group_concat(distinct name,subject order by 1,2 ,score,2) from ss order by 1;

-- query 58
USE ${case_db};
select group_concat(distinct name,subject order by length(name),1,2,score), count(distinct id), max(score)  from ss order by 1;

-- query 59
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct name,subject order by score+1,1,2,4) from ss order by 1;

-- query 60
USE ${case_db};
select group_concat(distinct name,subject order by 1,2,score,4.00) from ss order by 1;

-- query 61
USE ${case_db};
select group_concat(distinct name,null order by score,4.00) from ss order by 1;

-- query 62
USE ${case_db};
select group_concat(distinct name,subject order by 1,2, null) from ss order by 1;

-- query 63
USE ${case_db};
select group_concat(distinct null order by score,4.00) from ss order by 1;

-- query 64
USE ${case_db};
select group_concat(distinct name order by 1,score,4.00,1),group_concat(subject order by score,4.00,1),array_agg(subject order by score,4.00,1)  from ss order by 1;

-- query 65
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct score order by 1,2) from ss order by 1;

-- query 66
USE ${case_db};
select group_concat(distinct score order by 1,name) from ss order by 1;

-- query 67
USE ${case_db};
select group_concat(distinct 1,2 order by 1,2) from ss order by 1;

-- query 68
USE ${case_db};
select group_concat(distinct 1,2 order by score,2) from ss order by 1;

-- query 69
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct 3.1323,subject order by 1,-20) from ss order by 1;

-- query 70
USE ${case_db};
select group_concat( name,subject order by 1,2), count(distinct id), max(score) from ss order by 1;

-- query 71
USE ${case_db};
select group_concat( name,subject order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 72
USE ${case_db};
select group_concat( name,subject order by score,1,2), count(distinct id), max(score)  from ss order by 1;

-- query 73
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat( name,subject order by score,4), count(distinct id), max(score)  from ss order by 1;

-- query 74
USE ${case_db};
select group_concat( name,subject order by score,4.00,1,2), count(distinct id), max(score)  from ss order by 1;

-- query 75
USE ${case_db};
select group_concat( name,null order by score,4.00, 1) from ss order by 1;

-- query 76
USE ${case_db};
select group_concat( name,subject order by null,1,2) from ss order by 1;

-- query 77
USE ${case_db};
select group_concat( null order by score,4.00) from ss order by 1;

-- query 78
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat( score order by 1,2) from ss order by 1;

-- query 79
USE ${case_db};
select group_concat( score order by 1,name) from ss order by 1;

-- query 80
USE ${case_db};
select group_concat( 1,2 order by 1,2) from ss order by 1;

-- query 81
USE ${case_db};
select group_concat( 1,2 order by score,2) from ss order by 1;

-- query 82
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat( 3.1323,subject order by 1,-20) from ss order by 1;

-- query 83
-- @skip_result_check=true
USE ${case_db};
set new_planner_agg_stage = 2;

-- query 84
-- @skip_result_check=true
USE ${case_db};
set streaming_preaggregation_mode = force_streaming;

-- query 85
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct id), group_concat(name order by 1) from ss group by id order by 1;

-- query 86
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by id;

-- query 87
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss group by id order by 1;

-- query 88
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1, 2) from ss group by id order by 1;

-- query 89
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), group_concat(distinct score order by 1) from ss group by id order by 1;

-- query 90
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by 1;

-- query 91
USE ${case_db};
select group_concat(distinct name,subject order by 1,score) from ss group by id order by 1;

-- query 92
USE ${case_db};
select group_concat(distinct name,subject order by score, 1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 93
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct name,subject order by score,4,2,1) from ss group by id order by 1;

-- query 94
USE ${case_db};
select group_concat(distinct name,subject order by score,4.00, 1,2) from ss group by id order by 1;

-- query 95
USE ${case_db};
select group_concat(distinct name,null order by score,1,4.00) from ss group by id order by 1;

-- query 96
USE ${case_db};
select group_concat(distinct name,subject order by 1,2, null) from ss group by id order by 1;

-- query 97
USE ${case_db};
select group_concat(distinct null order by score,4.00) from ss group by id order by 1;

-- query 98
USE ${case_db};
select group_concat(distinct name, score order by score,4.00, 1),group_concat(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1;

-- query 99
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct score order by 1,2) from ss group by id order by 1;

-- query 100
USE ${case_db};
select group_concat(distinct score order by 1,name) from ss group by id order by 1;

-- query 101
USE ${case_db};
select group_concat(distinct 1,2 order by 1,2) from ss group by id order by 1;

-- query 102
USE ${case_db};
select group_concat(distinct 1,2 order by score,2) from ss group by id order by 1;

-- query 103
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct 3.1323,subject order by 1, 2,-20) from ss group by id order by 1;

-- query 104
USE ${case_db};
select group_concat( name,subject order by 1,2), count(distinct id), max(score) from ss group by id order by 1;

-- query 105
USE ${case_db};
select group_concat( name,subject order by 1,score), count(distinct id), max(score)  from ss group by id order by 1;

-- query 106
USE ${case_db};
select group_concat( name,subject order by score,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 107
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat( name,subject order by score,4,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 108
USE ${case_db};
select group_concat( name,subject order by score,4.00,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 109
USE ${case_db};
select group_concat( name,null order by score,4.00) from ss group by id order by 1;

-- query 110
USE ${case_db};
select group_concat( name,subject order by 1,2, null) from ss group by id order by 1;

-- query 111
USE ${case_db};
select group_concat( null order by score,4.00) from ss group by id order by 1;

-- query 112
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat( score order by 1,2) from ss group by id order by 1;

-- query 113
USE ${case_db};
select group_concat( score order by 1,name) from ss group by id order by 1;

-- query 114
USE ${case_db};
select group_concat( 1,2 order by 1,2) from ss group by id order by 1;

-- query 115
USE ${case_db};
select group_concat( 1,2 order by score,2) from ss group by id order by 1;

-- query 116
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat( 3.1323,subject order by 1,2,-20) from ss group by id order by 1;

-- query 117
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct id), group_concat(name order by 1) from ss order by 1;

-- query 118
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 119
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss order by 1;

-- query 120
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1,2) from ss order by 1;

-- query 121
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), group_concat(distinct score order by 1) from ss order by 1;

-- query 122
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 123
USE ${case_db};
select group_concat(distinct name,subject order by 1,2,score,2) from ss order by 1;

-- query 124
USE ${case_db};
select group_concat(distinct name,subject order by length(name),1,2,score), count(distinct id), max(score)  from ss order by 1;

-- query 125
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct name,subject order by score+1,1,2,4) from ss order by 1;

-- query 126
USE ${case_db};
select group_concat(distinct name,subject order by 1,2,score,4.00) from ss order by 1;

-- query 127
USE ${case_db};
select group_concat(distinct name,null order by score,4.00) from ss order by 1;

-- query 128
USE ${case_db};
select group_concat(distinct name,subject order by 1,2, null) from ss order by 1;

-- query 129
USE ${case_db};
select group_concat(distinct null order by score,4.00) from ss order by 1;

-- query 130
USE ${case_db};
select group_concat(distinct name order by 1,score,4.00, 1),group_concat(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 131
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct score order by 1,2) from ss order by 1;

-- query 132
USE ${case_db};
select group_concat(distinct score order by 1,name) from ss order by 1;

-- query 133
USE ${case_db};
select group_concat(distinct 1,2 order by 1,2) from ss order by 1;

-- query 134
USE ${case_db};
select group_concat(distinct 1,2 order by score,2) from ss order by 1;

-- query 135
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct 3.1323,subject order by 1,-20) from ss order by 1;

-- query 136
USE ${case_db};
select group_concat( name,subject order by 1,2), count(distinct id), max(score) from ss order by 1;

-- query 137
USE ${case_db};
select group_concat( name,subject order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 138
USE ${case_db};
select group_concat( name,subject order by score,1,2), count(distinct id), max(score)  from ss order by 1;

-- query 139
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat( name,subject order by score,4), count(distinct id), max(score)  from ss order by 1;

-- query 140
USE ${case_db};
select group_concat( name,subject order by score,4.00,1,2), count(distinct id), max(score)  from ss order by 1;

-- query 141
USE ${case_db};
select group_concat( name,null order by score,4.00) from ss order by 1;

-- query 142
USE ${case_db};
select group_concat( name,subject order by null,1,2) from ss order by 1;

-- query 143
USE ${case_db};
select group_concat( null order by score,4.00) from ss order by 1;

-- query 144
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat( score order by 1,2) from ss order by 1;

-- query 145
USE ${case_db};
select group_concat( score order by 1,name) from ss order by 1;

-- query 146
USE ${case_db};
select group_concat( 1,2 order by 1,2) from ss order by 1;

-- query 147
USE ${case_db};
select group_concat( 1,2 order by score,2) from ss order by 1;

-- query 148
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat( 3.1323,subject order by 1,-20) from ss order by 1;

-- query 149
-- @skip_result_check=true
USE ${case_db};
set new_planner_agg_stage = 0;

-- query 150
-- @skip_result_check=true
USE ${case_db};
set enable_exchange_pass_through = false;

-- query 151
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct id), group_concat(name order by 1) from ss group by id order by 1;

-- query 152
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by id;

-- query 153
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss group by id order by 1;

-- query 154
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1, 2) from ss group by id order by 1;

-- query 155
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), group_concat(distinct score order by 1) from ss group by id order by 1;

-- query 156
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by 1;

-- query 157
USE ${case_db};
select group_concat(distinct name,subject order by 1,score) from ss group by id order by 1;

-- query 158
USE ${case_db};
select group_concat(distinct name,subject order by score, 1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 159
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct name,subject order by score,4,2,1) from ss group by id order by 1;

-- query 160
USE ${case_db};
select group_concat(distinct name,subject order by score,4.00, 1,2) from ss group by id order by 1;

-- query 161
USE ${case_db};
select group_concat(distinct name,null order by score,1,4.00) from ss group by id order by 1;

-- query 162
USE ${case_db};
select group_concat(distinct name,subject order by 1,2, null) from ss group by id order by 1;

-- query 163
USE ${case_db};
select group_concat(distinct null order by score,4.00) from ss group by id order by 1;

-- query 164
USE ${case_db};
select group_concat(distinct name, score order by score,4.00, 1),group_concat(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1;

-- query 165
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct score order by 1,2) from ss group by id order by 1;

-- query 166
USE ${case_db};
select group_concat(distinct score order by 1,name) from ss group by id order by 1;

-- query 167
USE ${case_db};
select group_concat(distinct 1,2 order by 1,2) from ss group by id order by 1;

-- query 168
USE ${case_db};
select group_concat(distinct 1,2 order by score,2) from ss group by id order by 1;

-- query 169
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct 3.1323,subject order by 1, 2,-20) from ss group by id order by 1;

-- query 170
USE ${case_db};
select group_concat( name,subject order by 1,2), count(distinct id), max(score) from ss group by id order by 1;

-- query 171
USE ${case_db};
select group_concat( name,subject order by 1,score,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 172
USE ${case_db};
select group_concat( name,subject order by score,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 173
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat( name,subject order by score,4,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 174
USE ${case_db};
select group_concat( name,subject order by score,4.00,1,2), count(distinct id), max(score)  from ss group by id order by 1;

-- query 175
USE ${case_db};
select group_concat( name,null order by score,4.00) from ss group by id order by 1;

-- query 176
USE ${case_db};
select group_concat( name,subject order by 1,2, null) from ss group by id order by 1;

-- query 177
USE ${case_db};
select group_concat( null order by score,4.00) from ss group by id order by 1;

-- query 178
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat( score order by 1,2) from ss group by id order by 1;

-- query 179
USE ${case_db};
select group_concat( score order by 1,name) from ss group by id order by 1;

-- query 180
USE ${case_db};
select group_concat( 1,2 order by 1,2) from ss group by id order by 1;

-- query 181
USE ${case_db};
select group_concat( 1,2 order by score,2) from ss group by id order by 1;

-- query 182
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat( 3.1323,subject order by 1,2,-20) from ss group by id order by 1;

-- query 183
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct id), group_concat(name order by 1) from ss order by 1;

-- query 184
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 185
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss order by 1;

-- query 186
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1,2) from ss order by 1;

-- query 187
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), group_concat(distinct score order by 1) from ss order by 1;

-- query 188
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 189
USE ${case_db};
select group_concat(distinct name,subject order by 1,2,score,2) from ss order by 1;

-- query 190
USE ${case_db};
select group_concat(distinct name,subject order by length(name),1,2,score), count(distinct id), max(score)  from ss order by 1;

-- query 191
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct name,subject order by score+1,1,2,4) from ss order by 1;

-- query 192
USE ${case_db};
select group_concat(distinct name,subject order by 1,2,score,4.00) from ss order by 1;

-- query 193
USE ${case_db};
select group_concat(distinct name,null order by score,4.00) from ss order by 1;

-- query 194
USE ${case_db};
select group_concat(distinct name,subject order by 1,2, null) from ss order by 1;

-- query 195
USE ${case_db};
select group_concat(distinct null order by score,4.00) from ss order by 1;

-- query 196
USE ${case_db};
select group_concat(distinct name order by 1,score,4.00, 1),group_concat(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 197
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct score order by 1,2) from ss order by 1;

-- query 198
USE ${case_db};
select group_concat(distinct score order by 1,name) from ss order by 1;

-- query 199
USE ${case_db};
select group_concat(distinct 1,2 order by 1,2) from ss order by 1;

-- query 200
USE ${case_db};
select group_concat(distinct 1,2 order by score,2) from ss order by 1;

-- query 201
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat(distinct 3.1323,subject order by 1,-20) from ss order by 1;

-- query 202
USE ${case_db};
select group_concat( name,subject order by 1,2), count(distinct id), max(score) from ss order by 1;

-- query 203
USE ${case_db};
select group_concat( name,subject order by 1,score, 2), count(distinct id), max(score)  from ss order by 1;

-- query 204
USE ${case_db};
select group_concat( name,subject order by score,1,2), count(distinct id), max(score)  from ss order by 1;

-- query 205
-- @expect_error=ORDER BY position 4 is not in group_concat output list.
USE ${case_db};
select group_concat( name,subject order by score,4), count(distinct id), max(score)  from ss order by 1;

-- query 206
USE ${case_db};
select group_concat( name,subject order by score,4.00,1,2), count(distinct id), max(score)  from ss order by 1;

-- query 207
USE ${case_db};
select group_concat( name,null order by score,4.00) from ss order by 1;

-- query 208
USE ${case_db};
select group_concat( name,subject order by null,1,2) from ss order by 1;

-- query 209
USE ${case_db};
select group_concat( null order by score,4.00) from ss order by 1;

-- query 210
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
USE ${case_db};
select group_concat( score order by 1,2) from ss order by 1;

-- query 211
USE ${case_db};
select group_concat( score order by 1,name) from ss order by 1;

-- query 212
USE ${case_db};
select group_concat( 1,2 order by 1,2) from ss order by 1;

-- query 213
USE ${case_db};
select group_concat( 1,2 order by score,2) from ss order by 1;

-- query 214
-- @expect_error=ORDER BY position -20 is not in group_concat output list.
USE ${case_db};
select group_concat( 3.1323,subject order by 1,-20) from ss order by 1;

-- query 215
-- @skip_result_check=true
USE ${case_db};
set enable_query_cache = true;

-- query 216
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct score), group_concat(name order by 1) from ss order by 1;

-- query 217
USE ${case_db};
select count(distinct score), group_concat(name order by 1) from ss order by 1;

-- query 218
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 219
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss order by 1;

-- query 220
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1,2) from ss order by 1;

-- query 221
USE ${case_db};
select id, group_concat(distinct name,subject order by 1,2), count(distinct score), group_concat(name order by 1) from ss group by id order by 1;

-- query 222
USE ${case_db};
select id, count(distinct score), group_concat(name order by 1) from ss group by id order by 1;

-- query 223
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by id;

-- query 224
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss group by id order by 1;

-- query 225
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1, 2) from ss group by id order by 1;

-- query 226
-- @skip_result_check=true
USE ${case_db};
set enable_query_cache = false;

-- query 227
USE ${case_db};
select group_concat(distinct name,subject order by 1,2), count(distinct score), group_concat(name order by 1) from ss order by 1;

-- query 228
USE ${case_db};
select count(distinct score), group_concat(name order by 1) from ss order by 1;

-- query 229
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss order by 1;

-- query 230
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss order by 1;

-- query 231
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1,2) from ss order by 1;

-- query 232
USE ${case_db};
select id, group_concat(distinct name,subject order by 1,2), count(distinct score), group_concat(name order by 1) from ss group by id order by 1;

-- query 233
USE ${case_db};
select id, count(distinct score), group_concat(name order by 1) from ss group by id order by 1;

-- query 234
USE ${case_db};
select group_concat(distinct name,subject order by 1,2) from ss group by id order by id;

-- query 235
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(name,score order by 1,2) from ss group by id order by 1;

-- query 236
USE ${case_db};
select group_concat(name,subject order by 1,2), group_concat(distinct name,score order by 1, 2) from ss group by id order by 1;

-- query 237
-- @expect_error=group_concat should have at least one input.
USE ${case_db};
select group_concat();

-- query 238
-- @expect_error=group_concat should have at least one input.
USE ${case_db};
select group_concat() from ss;

-- query 239
USE ${case_db};
select group_concat(',');

-- query 240
USE ${case_db};
select group_concat("中国",name order by 2, id) from ss;

-- query 241
USE ${case_db};
select group_concat("中国",name order by 2, id separator NULL) from ss;

-- query 242
USE ${case_db};
select group_concat("中国",name order by 2, "第一", id) from ss;

-- query 243
USE ${case_db};
select instr(gc, '\n') > 0 as has_lf_separator
from (
  select group_concat("中国",name order by 2, "第一", id separator '\n') as gc
  from ss
) t;

-- query 244
USE ${case_db};
select group_concat("中国",name order by 2, "第一", subject,id separator subject) from ss;

-- query 245
-- @expect_error=group_concat requires separator to be of getType() STRING: group_concat('中国', 1).
USE ${case_db};
select group_concat("中国" order by "第一" separator 1) from ss;

-- query 246
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE ${case_db};
select group_concat(  order by score) from ss order by 1;

-- query 247
-- @expect_error=Unexpected input 'order', the most similar input is {a legal identifier}.
USE ${case_db};
select group_concat(distinct  order by score) from ss order by 1;

-- query 248
-- @expect_error=No matching function with signature: group_concat(array<tinyint(4)>, varchar).
USE ${case_db};
select group_concat([1,2]) from ss;

-- query 249
USE ${case_db};
select group_concat(json_object("2:3")) from ss;

-- query 250
-- @expect_error=No matching function with signature: group_concat(map<tinyint(4),tinyint(4)>, varchar).
USE ${case_db};
select group_concat(map(2,3)) from ss;

-- query 251
USE ${case_db};
select group_concat(null);

-- query 252
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE ${case_db};
select group_concat(order by 1 separator '');

-- query 253
-- @expect_error=No viable statement for input 'group_concat(separator NULL'.
USE ${case_db};
select group_concat(separator NULL);

-- query 254
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = -121;

-- query 255
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- query 256
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 1;

-- query 257
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- query 258
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 5;

-- query 259
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- query 260
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 6;

-- query 261
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- query 262
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 7;

-- query 263
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- query 264
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 8;

-- query 265
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- query 266
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 9;

-- query 267
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- query 268
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 121;

-- query 269
USE ${case_db};
select group_concat(name,subject order by 1,2) from ss group by id order by 1;

-- name: testLegacyGroupConcat
-- query 270
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    id        tinyint(4)      NULL,
    value   varchar(65533)  NULL
) ENGINE=OLAP
DISTRIBUTED BY HASH(id)
PROPERTIES (
 "replication_num" = "1"
);

-- query 271
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
(1,'fruit'),
(1,'fruit'),
(1,'fruit'),
(2,'fruit'),
(2,'fruit'),
(2,'fruit');

-- query 272
-- @skip_result_check=true
USE ${case_db};
set group_concat_max_len = 1024;

-- query 273
-- @skip_result_check=true
USE ${case_db};
set sql_mode = 'GROUP_CONCAT_LEGACY';

-- query 274
USE ${case_db};
select id, group_concat( value ) from t1 group by id order by id;

-- query 275
USE ${case_db};
select id, group_concat( value, '-' ) from t1 group by id order by id;

-- query 276
USE ${case_db};
select group_concat( value ) from t1;

-- query 277
USE ${case_db};
select group_concat( value, '-' ) from t1;

-- query 278
-- @skip_result_check=true
USE ${case_db};
set sql_mode = 32;

-- query 279
USE ${case_db};
select /*+ set_var(sql_mode = 'GROUP_CONCAT_LEGACY') */ id, group_concat( value ) from t1 group by id order by id;

-- query 280
USE ${case_db};
select /*+ set_var(sql_mode = 68719476736) */ group_concat( value, '-' ) from t1;

-- query 281
USE ${case_db};
select /*+ set_var(sql_mode = 'GROUP_CONCAT_LEGACY') */ /*+ set_var(sql_mode = 'ONLY_FULL_GROUP_BY') */ id, group_concat( value, '-' ) from t1 group by id order by id;

-- query 282
USE ${case_db};
select /*+ set_var(sql_mode = 68719476736) */ /*+ set_var(sql_mode = 32) */ group_concat( value ) from t1;
