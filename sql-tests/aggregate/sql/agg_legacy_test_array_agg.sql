-- Migrated from dev/test/sql/test_agg_function/R/test_array_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: testArrayAgg
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `ss` (
  `id` int(11) NULL COMMENT "",
  `name` varchar(255) NULL COMMENT "",
  `subject` varchar(255) NULL COMMENT "",
  `score` int(11) NULL COMMENT "",
   arr array<int>,
   mmap map<int,varchar(20)>
) ENGINE=OLAP
DUPLICATE KEY(`id`)
DISTRIBUTED BY HASH(`id`) BUCKETS 4
PROPERTIES (
"replication_num" = "1",
"enable_persistent_index" = "true",
"replicated_storage" = "true",
"compression" = "LZ4"
);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into ss values (1,"Tom","English",90, [1,2],map{2:'name'});

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into ss values (1,"Tom","Math",80, [1,2], map{2:'name'});

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"Tom","English",NULL,[], map{});

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"Tom",NULL,NULL,null,null);

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"May",NULL,NULL,[2],map{3:'3',4:'4'});

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"Ti","English",98, [null],map{null:null});

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into ss values (4,NULL,NULL,NULL,null,null);

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into ss values (NULL,NULL,NULL,NULL,null,null);

-- query 11
-- @skip_result_check=true
USE ${case_db};
insert into ss values (NULL,"Ti","物理Phy",99,[3,4],map{9:''});

-- query 12
-- @skip_result_check=true
USE ${case_db};
insert into ss values (11,"张三此地无银三百两","英文English",98,[0,2], map{});

-- query 13
-- @skip_result_check=true
USE ${case_db};
insert into ss values (11,"张三掩耳盗铃","Math数学欧拉方程",78,[],map{7:'y'});

-- query 14
-- @skip_result_check=true
USE ${case_db};
insert into ss values (12,"李四大闹天空","英语外语美誉",NULL,[89], map{6:'6'});

-- query 15
-- @skip_result_check=true
USE ${case_db};
insert into ss values (2,"王武程咬金","语文北京上海",22,[23],map{8:''});

-- query 16
-- @skip_result_check=true
USE ${case_db};
insert into ss values (3,"欧阳诸葛方程","数学大不列颠",NULL,[],null);

-- query 17
USE ${case_db};
select max(id), array_agg(distinct name), count(distinct id), array_agg(name) from ss where id < 2  order by 1;

-- query 18
USE ${case_db};
select max(id), array_agg(distinct name) from ss where id < 2  order by 1;

-- query 19
USE ${case_db};
select max(id), array_agg(distinct name), array_agg(distinct (score > 0)) from ss where id < 2 order by 1;

-- query 20
USE ${case_db};
select max(id),array_agg(distinct name), count(distinct id), array_agg(name) from ss where id < 2  order by 1;

-- query 21
USE ${case_db};
select max(id),array_agg(distinct name) from ss where id < 2  order by 1;

-- query 22
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE ${case_db};
select max(id),array_agg(name), array_agg(name,score order by 1,2) from ss where id < 2  order by 1;

-- query 23
USE ${case_db};
select max(id),array_agg(name), array_agg(distinct name) from ss where id < 2 order by 1;

-- query 24
USE ${case_db};
select max(id),array_agg(distinct name), array_agg(distinct (score > 0)) from ss where id < 2 order by 1;

-- query 25
USE ${case_db};
select id, array_agg(distinct name), count(distinct score), array_agg(name) from ss where id < 2 group by id order by 1;

-- query 26
USE ${case_db};
select id, array_agg(distinct name) from ss where id < 2 group by id order by id;

-- query 27
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE ${case_db};
select id, array_agg(name), array_agg(name,score order by 1,2) from ss where id < 2 group by id order by 1;

-- query 28
USE ${case_db};
select id, array_agg(name), array_agg(distinct name) from ss where id < 2 group by id order by 1;

-- query 29
USE ${case_db};
select id, array_agg(distinct name), array_agg(distinct (score > 1)) from ss where id < 2 group by id order by 1;

-- query 30
USE ${case_db};
select score, array_agg(distinct name), count(distinct id), array_agg(name) from ss where id < 2 group by score order by 1;

-- query 31
USE ${case_db};
select score, array_agg(distinct name) from ss where id < 2 group by score order by score;

-- query 32
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE ${case_db};
select score, array_agg(name), array_agg(name,score order by 1,2) from ss where id < 2 group by score order by 1;

-- query 33
USE ${case_db};
select score, array_agg(name), array_agg(distinct name) from ss where id < 2 group by score order by 1;

-- query 34
USE ${case_db};
select score, array_agg(distinct name), array_agg(distinct (score > 1)) from ss where id < 2 group by score order by 1;

-- query 35
USE ${case_db};
select id, array_agg(distinct name), count(distinct score), array_agg(name) from ss where id < 2 group by rollup(id) order by 1;

-- query 36
USE ${case_db};
select id, array_agg(distinct name) from ss where id < 2 group by rollup(id) order by id;

-- query 37
USE ${case_db};
select id, array_agg(name), array_agg(name order by 1) from ss where id < 2 group by rollup(id) order by 1;

-- query 38
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by rollup(id) order by 1;

-- query 39
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss group by rollup(id) order by id;

-- query 40
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by rollup(id) order by 1;

-- query 41
USE ${case_db};
select array_agg(distinct name order by 1), array_agg(distinct subject order by 1) from ss group by rollup(score) order by 1;

-- query 42
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1;

-- query 43
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 44
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 45
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 46
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 47
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 48
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 49
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 50
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 51
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 52
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 53
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 54
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 55
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 56
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1,2;

-- query 57
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1,2;

-- query 58
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1,2;

-- query 59
USE ${case_db};
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 60
USE ${case_db};
select cardinality(array_agg(distinct arr order by score, cardinality(arr), arr)) from ss group by id order by 1;

-- query 61
USE ${case_db};
select array_agg(distinct name order by 1,score, 1), count(distinct id), max(score)  from ss group by id order by 1,2;

-- query 62
USE ${case_db};
select cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by id;

-- query 63
USE ${case_db};
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss group by id order by 1, 2;

-- query 64
USE ${case_db};
select array_agg(distinct subject order by 1, null) from ss group by id order by 1;

-- query 65
USE ${case_db};
select cardinality(array_agg(distinct arr order by score,name, subject)), cardinality(array_agg(distinct mmap order by score, name,subject)) from ss group by id order by 1,2;

-- query 66
USE ${case_db};
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1,2;

-- query 67
USE ${case_db};
select array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 68
USE ${case_db};
select array_agg(distinct score order by 1,name) from ss group by id order by 1;

-- query 69
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 70
USE ${case_db};
select array_agg(distinct 12 order by score) from ss group by id order by 1;

-- query 71
USE ${case_db};
select array_agg(distinct subject order by 1) from ss group by id order by 1;

-- query 72
USE ${case_db};
select group_concat( name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss group by id order by 1,2,3;

-- query 73
USE ${case_db};
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 74
USE ${case_db};
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 75
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE ${case_db};
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 76
USE ${case_db};
select cardinality(array_agg( name order by score,4.00)), cardinality(array_agg(distinct name order by score,4.00)) from ss group by id order by 1, 2;

-- query 77
USE ${case_db};
select array_agg( null order by score,4.00) from ss group by id order by 1;

-- query 78
USE ${case_db};
select array_agg( score order by 1) from ss group by id order by 1;

-- query 79
USE ${case_db};
select array_agg( score order by 1,name) from ss group by id order by 1;

-- query 80
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 81
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 82
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 83
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr)) from ss order by 1;

-- query 84
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 85
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 86
USE ${case_db};
select array_agg(distinct name order by 1) from ss order by 1;

-- query 87
USE ${case_db};
select cardinality(array_agg(distinct arr order by score,1)) from ss order by 1;

-- query 88
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), max(score)  from ss order by 1;

-- query 89
USE ${case_db};
select cardinality(array_agg(distinct mmap order by score, cardinality(mmap), name)),max(id) as a from ss order by a;

-- query 90
USE ${case_db};
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss order by 1;

-- query 91
USE ${case_db};
select array_agg(distinct subject order by 1, null) from ss order by 1;

-- query 92
USE ${case_db};
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss order by 1;

-- query 93
USE ${case_db};
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 94
USE ${case_db};
select array_agg(distinct score order by 1) from ss order by 1;

-- query 95
USE ${case_db};
select array_agg(distinct score order by 1,name) from ss order by 1;

-- query 96
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 97
USE ${case_db};
select array_agg(distinct 12 order by score) from ss order by 1;

-- query 98
USE ${case_db};
select array_agg(distinct subject order by 1) from ss order by 1;

-- query 99
USE ${case_db};
select group_concat(name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss order by 1;

-- query 100
USE ${case_db};
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 101
USE ${case_db};
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss order by 1;

-- query 102
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE ${case_db};
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss order by 1;

-- query 103
USE ${case_db};
select array_agg( name order by 1,score,4.00),array_agg(distinct name order by 1,score,4.00) from ss order by 1;

-- query 104
USE ${case_db};
select array_agg( null order by score,4.00) from ss order by 1;

-- query 105
USE ${case_db};
select array_agg( score order by 1) from ss order by 1;

-- query 106
USE ${case_db};
select array_agg( score order by 1,name) from ss order by 1;

-- query 107
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 108
-- @skip_result_check=true
USE ${case_db};
set new_planner_agg_stage = 2;

-- query 109
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1;

-- query 110
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 111
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1,2;

-- query 112
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1,2;

-- query 113
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1,2;

-- query 114
USE ${case_db};
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 115
USE ${case_db};
select cardinality(array_agg(distinct arr order by score, 1)) from ss group by id order by 1;

-- query 116
USE ${case_db};
select array_agg(distinct name order by 1,score, 1), count(distinct id), max(score)  from ss group by id order by 1,2;

-- query 117
USE ${case_db};
select cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by id;

-- query 118
USE ${case_db};
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss group by id order by 1, 2;

-- query 119
USE ${case_db};
select array_agg(distinct subject order by 1, null) from ss group by id order by 1;

-- query 120
USE ${case_db};
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by 1, 2;

-- query 121
USE ${case_db};
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1,2;

-- query 122
USE ${case_db};
select array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 123
USE ${case_db};
select array_agg(distinct score order by 1,name) from ss group by id order by 1;

-- query 124
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 125
USE ${case_db};
select array_agg(distinct 12 order by score) from ss group by id order by 1;

-- query 126
USE ${case_db};
select array_agg(distinct subject order by 1) from ss group by id order by 1;

-- query 127
USE ${case_db};
select group_concat( name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss group by id order by 1,2,3;

-- query 128
USE ${case_db};
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 129
USE ${case_db};
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 130
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE ${case_db};
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 131
USE ${case_db};
select cardinality(array_agg( name order by score,4.00)), cardinality(array_agg(distinct name order by score,4.00)) from ss group by id order by 1, 2;

-- query 132
USE ${case_db};
select array_agg( null order by score,4.00) from ss group by id order by 1;

-- query 133
USE ${case_db};
select array_agg( score order by 1) from ss group by id order by 1;

-- query 134
USE ${case_db};
select array_agg( score order by 1,name) from ss group by id order by 1;

-- query 135
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 136
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 137
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 138
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr)) from ss order by 1;

-- query 139
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 140
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 141
USE ${case_db};
select array_agg(distinct name order by 1) from ss order by 1;

-- query 142
USE ${case_db};
select cardinality(array_agg(distinct arr order by score, 1)) from ss order by 1;

-- query 143
USE ${case_db};
select array_agg(distinct name order by 1, score, 1), count(distinct id), max(score)  from ss order by 1;

-- query 144
-- @expect_error=No matching function with signature: cardinality(array<map<int(11),varchar(20)>>, varchar(255)).
USE ${case_db};
select cardinality(array_agg(distinct mmap order by score, cardinality(mmap)), name),max(id) as a from ss order by a;

-- query 145
USE ${case_db};
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss order by 1;

-- query 146
USE ${case_db};
select array_agg(distinct subject order by 1, null) from ss order by 1;

-- query 147
USE ${case_db};
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss order by 1;

-- query 148
USE ${case_db};
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 149
USE ${case_db};
select array_agg(distinct score order by 1) from ss order by 1;

-- query 150
USE ${case_db};
select array_agg(distinct score order by 1,name) from ss order by 1;

-- query 151
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 152
USE ${case_db};
select array_agg(distinct 12 order by score) from ss order by 1;

-- query 153
USE ${case_db};
select array_agg(distinct subject order by 1) from ss order by 1;

-- query 154
USE ${case_db};
select group_concat(name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss order by 1;

-- query 155
USE ${case_db};
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 156
USE ${case_db};
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss order by 1;

-- query 157
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE ${case_db};
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss order by 1;

-- query 158
USE ${case_db};
select array_agg( name order by 1,score,4.00),array_agg(distinct name order by 1,score,4.00) from ss order by 1;

-- query 159
USE ${case_db};
select array_agg( null order by score,4.00) from ss order by 1;

-- query 160
USE ${case_db};
select array_agg( score order by 1) from ss order by 1;

-- query 161
USE ${case_db};
select array_agg( score order by 1,name) from ss order by 1;

-- query 162
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 163
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 164
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 165
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 166
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 167
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 asc nulls last) from ss group by id order by id;

-- query 168
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 169
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 170
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 171
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 172
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 173
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 174
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 175
-- @skip_result_check=true
USE ${case_db};
set new_planner_agg_stage = 0;

-- query 176
-- @skip_result_check=true
USE ${case_db};
set enable_exchange_pass_through = false;

-- query 177
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1,2;

-- query 178
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 179
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1,2;

-- query 180
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1,2;

-- query 181
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1,2;

-- query 182
USE ${case_db};
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 183
USE ${case_db};
select cardinality(array_agg(distinct arr order by score,1)) from ss group by id order by 1;

-- query 184
USE ${case_db};
select array_agg(distinct name order by 1,score, 1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 185
USE ${case_db};
select cardinality(array_agg(distinct mmap order by score, name,subject)) from ss group by id order by id;

-- query 186
USE ${case_db};
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss group by id order by 1, 2;

-- query 187
USE ${case_db};
select array_agg(distinct subject order by 1, null) from ss group by id order by 1;

-- query 188
USE ${case_db};
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by 1, 2;

-- query 189
USE ${case_db};
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1, 2;

-- query 190
USE ${case_db};
select array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 191
USE ${case_db};
select array_agg(distinct score order by 1,name) from ss group by id order by 1;

-- query 192
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 193
USE ${case_db};
select array_agg(distinct 12 order by score) from ss group by id order by 1;

-- query 194
USE ${case_db};
select array_agg(distinct subject order by 1) from ss group by id order by 1;

-- query 195
USE ${case_db};
select group_concat( name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss group by id order by 1, 2, 3;

-- query 196
USE ${case_db};
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss group by id order by 1, 2, 3;

-- query 197
USE ${case_db};
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss group by id order by 1, 2, 3;

-- query 198
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE ${case_db};
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss group by id order by 1, 2,3;

-- query 199
USE ${case_db};
select cardinality(array_agg( name order by score,4.00)), cardinality(array_agg(distinct name order by score,4.00)) from ss group by id order by 1, 2;

-- query 200
USE ${case_db};
select array_agg( null order by score,4.00) from ss group by id order by 1;

-- query 201
USE ${case_db};
select array_agg( score order by 1) from ss group by id order by 1;

-- query 202
USE ${case_db};
select array_agg( score order by 1,name) from ss group by id order by 1;

-- query 203
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 204
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 205
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 206
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss order by 1;

-- query 207
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 208
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 209
USE ${case_db};
select array_agg(distinct name order by 1) from ss order by 1;

-- query 210
USE ${case_db};
select cardinality(array_agg(distinct arr order by score, 1)) from ss order by 1;

-- query 211
USE ${case_db};
select array_agg(distinct name order by 1, score, 1), count(distinct id), max(score)  from ss order by 1;

-- query 212
USE ${case_db};
select cardinality(array_agg(distinct mmap order by score, cardinality(mmap), name)),max(id) as a from ss order by a;

-- query 213
USE ${case_db};
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss order by 1;

-- query 214
USE ${case_db};
select array_agg(distinct subject order by 1, null) from ss order by 1;

-- query 215
USE ${case_db};
select cardinality(array_agg(distinct arr order by score, cardinality(arr),name)), cardinality(array_agg(distinct mmap order by score,cardinality(mmap),name)) from ss order by 1;

-- query 216
USE ${case_db};
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 217
USE ${case_db};
select array_agg(distinct score order by 1) from ss order by 1;

-- query 218
USE ${case_db};
select array_agg(distinct score order by 1,name) from ss order by 1;

-- query 219
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 220
USE ${case_db};
select array_agg(distinct 12 order by score) from ss order by 1;

-- query 221
USE ${case_db};
select array_agg(distinct subject order by 1) from ss order by 1;

-- query 222
USE ${case_db};
select group_concat(name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss order by 1;

-- query 223
USE ${case_db};
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 224
USE ${case_db};
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss order by 1;

-- query 225
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE ${case_db};
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss order by 1;

-- query 226
USE ${case_db};
select array_agg( name order by 1,score,4.00),array_agg(distinct name order by 1,score,4.00) from ss order by 1;

-- query 227
USE ${case_db};
select array_agg( null order by score,4.00) from ss order by 1;

-- query 228
USE ${case_db};
select array_agg( score order by 1) from ss order by 1;

-- query 229
USE ${case_db};
select array_agg( score order by 1,name) from ss order by 1;

-- query 230
USE ${case_db};
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 231
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 232
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 233
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 234
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 235
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 236
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 237
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 238
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 239
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 240
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 241
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 242
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 243
-- @skip_result_check=true
USE ${case_db};
set enable_query_cache = true;

-- query 244
USE ${case_db};
select array_agg(distinct subject order by 1), count(distinct score), array_agg(name order by 1) from ss order by 1;

-- query 245
USE ${case_db};
select count(distinct score), array_agg(name order by 1) from ss order by 1;

-- query 246
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1;

-- query 247
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 248
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1;

-- query 249
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1;

-- query 250
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 251
USE ${case_db};
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 252
USE ${case_db};
select cardinality(array_agg(distinct arr order by score, 1)) from ss group by id order by 1;

-- query 253
USE ${case_db};
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 254
USE ${case_db};
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 255
USE ${case_db};
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss order by 1;

-- query 256
USE ${case_db};
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 257
USE ${case_db};
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 258
USE ${case_db};
select array_agg(distinct name order by 1) from ss order by 1;

-- query 259
USE ${case_db};
select cardinality(array_agg(distinct arr order by score, 1)) from ss order by 1;

-- query 260
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 261
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 262
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 263
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 264
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 265
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 266
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 267
USE ${case_db};
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 268
USE ${case_db};
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 269
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 270
USE ${case_db};
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 271
USE ${case_db};
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 272
-- @expect_error=array_agg should have at least one input.
USE ${case_db};
select array_agg();

-- query 273
-- @expect_error=array_agg should have at least one input.
USE ${case_db};
select array_agg() from ss;

-- query 274
USE ${case_db};
select array_agg(',');

-- query 275
USE ${case_db};
select array_agg("中国" order by 1, id) from ss;

-- query 276
-- @expect_error=Unexpected input 'separator', the most similar input is {',', ')'}.
USE ${case_db};
select array_agg("中国" order by 2, id separator NULL) from ss;

-- query 277
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE ${case_db};
select array_agg("中国",name order by 2, "第一", id) from ss;

-- query 278
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE ${case_db};
select array_agg(  order by score) from ss order by 1;

-- query 279
-- @expect_error=Unexpected input 'order', the most similar input is {a legal identifier}.
USE ${case_db};
select array_agg(distinct  order by score) from ss order by 1;

-- query 280
USE ${case_db};
select array_agg([1,2]) from ss;

-- query 281
USE ${case_db};
select array_agg(json_object("2:3")) from ss;

-- query 282
-- @expect_error=array_agg(DISTINCT json_object('2:3')) can't rewrite distinct to group by on (json).
USE ${case_db};
select array_agg(distinct json_object("2:3")) from ss;

-- query 283
-- @expect_error=array_agg(DISTINCT [json_object('2:3')]) can't rewrite distinct to group by on (array<json>).
USE ${case_db};
select array_agg(distinct [json_object("2:3")]) from ss;

-- query 284
USE ${case_db};
select array_agg(map(2,3)) from ss;

-- query 285
-- @expect_error=Unknown error
USE ${case_db};
select array_agg(distinct map(2,3)) from ss;

-- query 286
USE ${case_db};
select array_agg(null);

-- query 287
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE ${case_db};
select array_agg(order by 1 separator '');

-- query 288
-- @expect_error=No viable statement for input 'array_agg(separator NULL'.
USE ${case_db};
select array_agg(separator NULL);

-- query 289
USE ${case_db};
select array_agg_distinct(name order by 1 asc), array_agg(distinct name order by 1 desc), array_agg(name order by 1 desc) from ss order by 1;

-- query 290
USE ${case_db};
select array_agg_distinct(name order by 1), cardinality(array_agg_distinct(arr order by score)) from ss group by id order by 1;

-- name: test_array_agg_all_type
-- query 291
-- @skip_result_check=true
USE ${case_db};
drop table if exists test_array_agg;

-- query 292
-- @skip_result_check=true
USE ${case_db};
create table test_array_agg (
    id INT,
    col_boolean BOOLEAN,
    col_tinyint TINYINT,
    col_smallint SMALLINT,
    col_int INT,
    col_bigint BIGINT,
    col_largeint LARGEINT,
    col_float FLOAT,
    col_double DOUBLE,
    col_varchar VARCHAR(100),
    col_char CHAR(10),
    col_datetime DATETIME,
    col_date DATE,
    col_array ARRAY<INT>,
    col_map MAP<STRING, INT>,
    col_struct STRUCT<f1 INT, f2 STRING>
) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 293
-- @skip_result_check=true
USE ${case_db};
insert into test_array_agg values
(1, true, 10, 100, 1000, 10000, 100000, 1.1, 2.2, 'hello', 'char_test', '2024-01-01 12:00:00', '2024-01-01', [1,2,3], map{"key1": 1, "key2": 2}, row(1, "test1")),
(2, false, 20, 200, 2000, 20000, 200000, 3.3, 4.4, 'world', 'char_test2', '2024-02-02 13:00:00', '2024-02-02', [4,5,6], map{"key3": 3, "key4": 4}, row(2, "test2")),
(3, null, 30, 300, 3000, 30000, 300000, 5.5, 6.6, null, null, null, null, null, null, null),
(4, true, 10, 100, 1000, 10000, 100000, 1.1, 2.2, 'hello', 'char_test', '2024-01-01 12:00:00', '2024-01-01', [1,2,3], map{"key1": 1, "key2": 2}, row(1, "test1")),
(5, false, 20, 200, 2000, 20000, 200000, 3.3, 4.4, 'world', 'char_test2', '2024-02-02 13:00:00', '2024-02-02', [4,5,6], map{"key3": 3, "key4": 4}, row(2, "test2")),
(1, true, 10, 100, 1000, 10000, 100000, 1.1, 2.2, 'hello', 'char_test', '2024-01-01 12:00:00', '2024-01-01', [1,2,3], map{"key1": 1, "key2": 2}, row(1, "test1")),
(2, false, 20, 200, 2000, 20000, 200000, 3.3, 4.4, 'world', 'char_test2', '2024-02-02 13:00:00', '2024-02-02', [4,5,6], map{"key3": 3, "key4": 4}, row(2, "test2"));

-- query 294
-- @skip_result_check=true
USE ${case_db};
set enable_per_bucket_optimize=false;

-- query 295
USE ${case_db};
SELECT ARRAY_AGG(col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg;

-- query 296
USE ${case_db};
SELECT ARRAY_AGG(col_tinyint ORDER BY id DESC) FROM test_array_agg;

-- query 297
USE ${case_db};
SELECT ARRAY_AGG(col_smallint ORDER BY id ASC) FROM test_array_agg;

-- query 298
USE ${case_db};
SELECT ARRAY_AGG(col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg;

-- query 299
USE ${case_db};
SELECT ARRAY_AGG(col_bigint ORDER BY id ASC) FROM test_array_agg;

-- query 300
USE ${case_db};
SELECT ARRAY_AGG(col_largeint ORDER BY id DESC) FROM test_array_agg;

-- query 301
USE ${case_db};
SELECT ARRAY_AGG(col_float ORDER BY id ASC) FROM test_array_agg;

-- query 302
USE ${case_db};
SELECT ARRAY_AGG(col_double ORDER BY id DESC) FROM test_array_agg;

-- query 303
USE ${case_db};
SELECT ARRAY_AGG(col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg;

-- query 304
USE ${case_db};
SELECT ARRAY_AGG(col_char ORDER BY id DESC) FROM test_array_agg;

-- query 305
USE ${case_db};
SELECT ARRAY_AGG(col_datetime ORDER BY id ASC) FROM test_array_agg;

-- query 306
USE ${case_db};
SELECT ARRAY_AGG(col_date ORDER BY id DESC) FROM test_array_agg;

-- query 307
USE ${case_db};
SELECT ARRAY_AGG(col_array ORDER BY id ASC) FROM test_array_agg;

-- query 308
USE ${case_db};
SELECT ARRAY_AGG(col_map ORDER BY id DESC) FROM test_array_agg;

-- query 309
USE ${case_db};
SELECT ARRAY_AGG(col_struct ORDER BY id ASC) FROM test_array_agg;

-- query 310
USE ${case_db};
SELECT id, ARRAY_AGG(col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 311
USE ${case_db};
SELECT id, ARRAY_AGG(col_tinyint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 312
USE ${case_db};
SELECT id, ARRAY_AGG(col_smallint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 313
USE ${case_db};
SELECT id, ARRAY_AGG(col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 314
USE ${case_db};
SELECT id, ARRAY_AGG(col_bigint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 315
USE ${case_db};
SELECT id, ARRAY_AGG(col_largeint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 316
USE ${case_db};
SELECT id, ARRAY_AGG(col_float ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 317
USE ${case_db};
SELECT id, ARRAY_AGG(col_double ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 318
USE ${case_db};
SELECT id, ARRAY_AGG(col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 319
USE ${case_db};
SELECT id, ARRAY_AGG(col_char ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 320
USE ${case_db};
SELECT id, ARRAY_AGG(col_datetime ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 321
USE ${case_db};
SELECT id, ARRAY_AGG(col_date ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 322
USE ${case_db};
SELECT id, ARRAY_AGG(col_array ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 323
USE ${case_db};
SELECT id, ARRAY_AGG(col_map ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 324
USE ${case_db};
SELECT id, ARRAY_AGG(col_struct ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 325
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg;

-- query 326
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_tinyint ORDER BY id DESC) FROM test_array_agg;

-- query 327
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_smallint ORDER BY id ASC) FROM test_array_agg;

-- query 328
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg;

-- query 329
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_bigint ORDER BY id ASC) FROM test_array_agg;

-- query 330
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_largeint ORDER BY id DESC) FROM test_array_agg;

-- query 331
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_float ORDER BY id ASC) FROM test_array_agg;

-- query 332
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_double ORDER BY id DESC) FROM test_array_agg;

-- query 333
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg;

-- query 334
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_char ORDER BY id DESC) FROM test_array_agg;

-- query 335
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_datetime ORDER BY id ASC) FROM test_array_agg;

-- query 336
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_date ORDER BY id DESC) FROM test_array_agg;

-- query 337
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_array ORDER BY id ASC) FROM test_array_agg;

-- query 338
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_map ORDER BY id DESC) FROM test_array_agg;

-- query 339
USE ${case_db};
SELECT ARRAY_AGG(DISTINCT col_struct ORDER BY id ASC) FROM test_array_agg;

-- query 340
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 341
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_tinyint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 342
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_smallint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 343
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 344
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_bigint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 345
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_largeint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 346
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_float ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 347
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_double ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 348
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 349
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_char ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 350
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_datetime ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 351
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_date ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 352
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_array ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 353
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_map ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 354
USE ${case_db};
SELECT id, ARRAY_AGG(DISTINCT col_struct ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;
