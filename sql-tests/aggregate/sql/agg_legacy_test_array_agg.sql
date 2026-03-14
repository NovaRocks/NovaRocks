-- Migrated from dev/test/sql/test_agg_function/R/test_array_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_test_array_agg FORCE;
CREATE DATABASE sql_tests_test_array_agg;
USE sql_tests_test_array_agg;

-- name: testArrayAgg
-- query 2
-- @skip_result_check=true
USE sql_tests_test_array_agg;
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
USE sql_tests_test_array_agg;
insert into ss values (1,"Tom","English",90, [1,2],map{2:'name'});

-- query 4
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (1,"Tom","Math",80, [1,2], map{2:'name'});

-- query 5
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (2,"Tom","English",NULL,[], map{});

-- query 6
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (2,"Tom",NULL,NULL,null,null);

-- query 7
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (3,"May",NULL,NULL,[2],map{3:'3',4:'4'});

-- query 8
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (3,"Ti","English",98, [null],map{null:null});

-- query 9
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (4,NULL,NULL,NULL,null,null);

-- query 10
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (NULL,NULL,NULL,NULL,null,null);

-- query 11
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (NULL,"Ti","物理Phy",99,[3,4],map{9:''});

-- query 12
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (11,"张三此地无银三百两","英文English",98,[0,2], map{});

-- query 13
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (11,"张三掩耳盗铃","Math数学欧拉方程",78,[],map{7:'y'});

-- query 14
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (12,"李四大闹天空","英语外语美誉",NULL,[89], map{6:'6'});

-- query 15
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (2,"王武程咬金","语文北京上海",22,[23],map{8:''});

-- query 16
-- @skip_result_check=true
USE sql_tests_test_array_agg;
insert into ss values (3,"欧阳诸葛方程","数学大不列颠",NULL,[],null);

-- query 17
USE sql_tests_test_array_agg;
select max(id), array_agg(distinct name), count(distinct id), array_agg(name) from ss where id < 2  order by 1;

-- query 18
USE sql_tests_test_array_agg;
select max(id), array_agg(distinct name) from ss where id < 2  order by 1;

-- query 19
USE sql_tests_test_array_agg;
select max(id), array_agg(distinct name), array_agg(distinct (score > 0)) from ss where id < 2 order by 1;

-- query 20
USE sql_tests_test_array_agg;
select max(id),array_agg(distinct name), count(distinct id), array_agg(name) from ss where id < 2  order by 1;

-- query 21
USE sql_tests_test_array_agg;
select max(id),array_agg(distinct name) from ss where id < 2  order by 1;

-- query 22
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE sql_tests_test_array_agg;
select max(id),array_agg(name), array_agg(name,score order by 1,2) from ss where id < 2  order by 1;

-- query 23
USE sql_tests_test_array_agg;
select max(id),array_agg(name), array_agg(distinct name) from ss where id < 2 order by 1;

-- query 24
USE sql_tests_test_array_agg;
select max(id),array_agg(distinct name), array_agg(distinct (score > 0)) from ss where id < 2 order by 1;

-- query 25
USE sql_tests_test_array_agg;
select id, array_agg(distinct name), count(distinct score), array_agg(name) from ss where id < 2 group by id order by 1;

-- query 26
USE sql_tests_test_array_agg;
select id, array_agg(distinct name) from ss where id < 2 group by id order by id;

-- query 27
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE sql_tests_test_array_agg;
select id, array_agg(name), array_agg(name,score order by 1,2) from ss where id < 2 group by id order by 1;

-- query 28
USE sql_tests_test_array_agg;
select id, array_agg(name), array_agg(distinct name) from ss where id < 2 group by id order by 1;

-- query 29
USE sql_tests_test_array_agg;
select id, array_agg(distinct name), array_agg(distinct (score > 1)) from ss where id < 2 group by id order by 1;

-- query 30
USE sql_tests_test_array_agg;
select score, array_agg(distinct name), count(distinct id), array_agg(name) from ss where id < 2 group by score order by 1;

-- query 31
USE sql_tests_test_array_agg;
select score, array_agg(distinct name) from ss where id < 2 group by score order by score;

-- query 32
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE sql_tests_test_array_agg;
select score, array_agg(name), array_agg(name,score order by 1,2) from ss where id < 2 group by score order by 1;

-- query 33
USE sql_tests_test_array_agg;
select score, array_agg(name), array_agg(distinct name) from ss where id < 2 group by score order by 1;

-- query 34
USE sql_tests_test_array_agg;
select score, array_agg(distinct name), array_agg(distinct (score > 1)) from ss where id < 2 group by score order by 1;

-- query 35
USE sql_tests_test_array_agg;
select id, array_agg(distinct name), count(distinct score), array_agg(name) from ss where id < 2 group by rollup(id) order by 1;

-- query 36
USE sql_tests_test_array_agg;
select id, array_agg(distinct name) from ss where id < 2 group by rollup(id) order by id;

-- query 37
USE sql_tests_test_array_agg;
select id, array_agg(name), array_agg(name order by 1) from ss where id < 2 group by rollup(id) order by 1;

-- query 38
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by rollup(id) order by 1;

-- query 39
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss group by rollup(id) order by id;

-- query 40
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by rollup(id) order by 1;

-- query 41
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), array_agg(distinct subject order by 1) from ss group by rollup(score) order by 1;

-- query 42
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1;

-- query 43
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 44
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 45
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 46
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 47
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 48
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 49
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 50
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 51
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 52
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 53
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 54
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 55
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 56
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1,2;

-- query 57
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1,2;

-- query 58
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1,2;

-- query 59
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 60
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score, cardinality(arr), arr)) from ss group by id order by 1;

-- query 61
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score, 1), count(distinct id), max(score)  from ss group by id order by 1,2;

-- query 62
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by id;

-- query 63
USE sql_tests_test_array_agg;
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss group by id order by 1, 2;

-- query 64
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, null) from ss group by id order by 1;

-- query 65
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score,name, subject)), cardinality(array_agg(distinct mmap order by score, name,subject)) from ss group by id order by 1,2;

-- query 66
USE sql_tests_test_array_agg;
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1,2;

-- query 67
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 68
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1,name) from ss group by id order by 1;

-- query 69
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 70
USE sql_tests_test_array_agg;
select array_agg(distinct 12 order by score) from ss group by id order by 1;

-- query 71
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1) from ss group by id order by 1;

-- query 72
USE sql_tests_test_array_agg;
select group_concat( name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss group by id order by 1,2,3;

-- query 73
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 74
USE sql_tests_test_array_agg;
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 75
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE sql_tests_test_array_agg;
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 76
USE sql_tests_test_array_agg;
select cardinality(array_agg( name order by score,4.00)), cardinality(array_agg(distinct name order by score,4.00)) from ss group by id order by 1, 2;

-- query 77
USE sql_tests_test_array_agg;
select array_agg( null order by score,4.00) from ss group by id order by 1;

-- query 78
USE sql_tests_test_array_agg;
select array_agg( score order by 1) from ss group by id order by 1;

-- query 79
USE sql_tests_test_array_agg;
select array_agg( score order by 1,name) from ss group by id order by 1;

-- query 80
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 81
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 82
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 83
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr)) from ss order by 1;

-- query 84
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 85
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 86
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss order by 1;

-- query 87
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score,1)) from ss order by 1;

-- query 88
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), max(score)  from ss order by 1;

-- query 89
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct mmap order by score, cardinality(mmap), name)),max(id) as a from ss order by a;

-- query 90
USE sql_tests_test_array_agg;
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss order by 1;

-- query 91
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, null) from ss order by 1;

-- query 92
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss order by 1;

-- query 93
USE sql_tests_test_array_agg;
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 94
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1) from ss order by 1;

-- query 95
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1,name) from ss order by 1;

-- query 96
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 97
USE sql_tests_test_array_agg;
select array_agg(distinct 12 order by score) from ss order by 1;

-- query 98
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1) from ss order by 1;

-- query 99
USE sql_tests_test_array_agg;
select group_concat(name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss order by 1;

-- query 100
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 101
USE sql_tests_test_array_agg;
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss order by 1;

-- query 102
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE sql_tests_test_array_agg;
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss order by 1;

-- query 103
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score,4.00),array_agg(distinct name order by 1,score,4.00) from ss order by 1;

-- query 104
USE sql_tests_test_array_agg;
select array_agg( null order by score,4.00) from ss order by 1;

-- query 105
USE sql_tests_test_array_agg;
select array_agg( score order by 1) from ss order by 1;

-- query 106
USE sql_tests_test_array_agg;
select array_agg( score order by 1,name) from ss order by 1;

-- query 107
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 108
-- @skip_result_check=true
USE sql_tests_test_array_agg;
set new_planner_agg_stage = 2;

-- query 109
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1;

-- query 110
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 111
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1,2;

-- query 112
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1,2;

-- query 113
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1,2;

-- query 114
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 115
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score, 1)) from ss group by id order by 1;

-- query 116
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score, 1), count(distinct id), max(score)  from ss group by id order by 1,2;

-- query 117
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by id;

-- query 118
USE sql_tests_test_array_agg;
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss group by id order by 1, 2;

-- query 119
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, null) from ss group by id order by 1;

-- query 120
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by 1, 2;

-- query 121
USE sql_tests_test_array_agg;
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1,2;

-- query 122
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 123
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1,name) from ss group by id order by 1;

-- query 124
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 125
USE sql_tests_test_array_agg;
select array_agg(distinct 12 order by score) from ss group by id order by 1;

-- query 126
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1) from ss group by id order by 1;

-- query 127
USE sql_tests_test_array_agg;
select group_concat( name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss group by id order by 1,2,3;

-- query 128
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 129
USE sql_tests_test_array_agg;
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 130
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE sql_tests_test_array_agg;
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 131
USE sql_tests_test_array_agg;
select cardinality(array_agg( name order by score,4.00)), cardinality(array_agg(distinct name order by score,4.00)) from ss group by id order by 1, 2;

-- query 132
USE sql_tests_test_array_agg;
select array_agg( null order by score,4.00) from ss group by id order by 1;

-- query 133
USE sql_tests_test_array_agg;
select array_agg( score order by 1) from ss group by id order by 1;

-- query 134
USE sql_tests_test_array_agg;
select array_agg( score order by 1,name) from ss group by id order by 1;

-- query 135
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 136
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 137
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 138
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr)) from ss order by 1;

-- query 139
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 140
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 141
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss order by 1;

-- query 142
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score, 1)) from ss order by 1;

-- query 143
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score, 1), count(distinct id), max(score)  from ss order by 1;

-- query 144
-- @expect_error=No matching function with signature: cardinality(array<map<int(11),varchar(20)>>, varchar(255)).
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct mmap order by score, cardinality(mmap)), name),max(id) as a from ss order by a;

-- query 145
USE sql_tests_test_array_agg;
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss order by 1;

-- query 146
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, null) from ss order by 1;

-- query 147
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss order by 1;

-- query 148
USE sql_tests_test_array_agg;
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 149
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1) from ss order by 1;

-- query 150
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1,name) from ss order by 1;

-- query 151
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 152
USE sql_tests_test_array_agg;
select array_agg(distinct 12 order by score) from ss order by 1;

-- query 153
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1) from ss order by 1;

-- query 154
USE sql_tests_test_array_agg;
select group_concat(name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss order by 1;

-- query 155
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 156
USE sql_tests_test_array_agg;
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss order by 1;

-- query 157
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE sql_tests_test_array_agg;
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss order by 1;

-- query 158
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score,4.00),array_agg(distinct name order by 1,score,4.00) from ss order by 1;

-- query 159
USE sql_tests_test_array_agg;
select array_agg( null order by score,4.00) from ss order by 1;

-- query 160
USE sql_tests_test_array_agg;
select array_agg( score order by 1) from ss order by 1;

-- query 161
USE sql_tests_test_array_agg;
select array_agg( score order by 1,name) from ss order by 1;

-- query 162
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 163
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 164
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 165
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 166
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 167
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 asc nulls last) from ss group by id order by id;

-- query 168
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 169
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 170
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 171
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 172
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 173
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 174
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 175
-- @skip_result_check=true
USE sql_tests_test_array_agg;
set new_planner_agg_stage = 0;

-- query 176
-- @skip_result_check=true
USE sql_tests_test_array_agg;
set enable_exchange_pass_through = false;

-- query 177
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1,2;

-- query 178
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 179
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1,2;

-- query 180
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1,2;

-- query 181
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1,2;

-- query 182
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 183
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score,1)) from ss group by id order by 1;

-- query 184
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score, 1), count(distinct id), max(score)  from ss group by id order by 1,2,3;

-- query 185
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct mmap order by score, name,subject)) from ss group by id order by id;

-- query 186
USE sql_tests_test_array_agg;
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss group by id order by 1, 2;

-- query 187
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, null) from ss group by id order by 1;

-- query 188
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score,name,subject)), cardinality(array_agg(distinct mmap order by score,name,subject)) from ss group by id order by 1, 2;

-- query 189
USE sql_tests_test_array_agg;
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss group by id order by 1, 2;

-- query 190
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 191
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1,name) from ss group by id order by 1;

-- query 192
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 193
USE sql_tests_test_array_agg;
select array_agg(distinct 12 order by score) from ss group by id order by 1;

-- query 194
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1) from ss group by id order by 1;

-- query 195
USE sql_tests_test_array_agg;
select group_concat( name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss group by id order by 1, 2, 3;

-- query 196
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss group by id order by 1, 2, 3;

-- query 197
USE sql_tests_test_array_agg;
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss group by id order by 1, 2, 3;

-- query 198
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE sql_tests_test_array_agg;
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss group by id order by 1, 2,3;

-- query 199
USE sql_tests_test_array_agg;
select cardinality(array_agg( name order by score,4.00)), cardinality(array_agg(distinct name order by score,4.00)) from ss group by id order by 1, 2;

-- query 200
USE sql_tests_test_array_agg;
select array_agg( null order by score,4.00) from ss group by id order by 1;

-- query 201
USE sql_tests_test_array_agg;
select array_agg( score order by 1) from ss group by id order by 1;

-- query 202
USE sql_tests_test_array_agg;
select array_agg( score order by 1,name) from ss group by id order by 1;

-- query 203
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss group by id order by 1;

-- query 204
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 205
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 206
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss order by 1;

-- query 207
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 208
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 209
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss order by 1;

-- query 210
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score, 1)) from ss order by 1;

-- query 211
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score, 1), count(distinct id), max(score)  from ss order by 1;

-- query 212
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct mmap order by score, cardinality(mmap), name)),max(id) as a from ss order by a;

-- query 213
USE sql_tests_test_array_agg;
select array_agg(distinct null order by score,1,4.00),array_agg(null order by score,1,4.00 ) from ss order by 1;

-- query 214
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, null) from ss order by 1;

-- query 215
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score, cardinality(arr),name)), cardinality(array_agg(distinct mmap order by score,cardinality(mmap),name)) from ss order by 1;

-- query 216
USE sql_tests_test_array_agg;
select array_agg(distinct score order by score,4.00),array_agg(subject order by score,4.00, 1),array_agg(subject order by score,4.00, 1)  from ss order by 1;

-- query 217
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1) from ss order by 1;

-- query 218
USE sql_tests_test_array_agg;
select array_agg(distinct score order by 1,name) from ss order by 1;

-- query 219
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 220
USE sql_tests_test_array_agg;
select array_agg(distinct 12 order by score) from ss order by 1;

-- query 221
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1) from ss order by 1;

-- query 222
USE sql_tests_test_array_agg;
select group_concat(name,subject order by 1,2), array_agg(distinct id order by 1), max(score) from ss order by 1;

-- query 223
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score), count(distinct id), max(score)  from ss order by 1;

-- query 224
USE sql_tests_test_array_agg;
select array_agg( subject order by score,1), count(distinct id), max(score)  from ss order by 1;

-- query 225
-- @expect_error=ORDER BY position 4 is not in array_agg output list.
USE sql_tests_test_array_agg;
select array_agg(name order by score,4,1), count(distinct id), max(score)  from ss order by 1;

-- query 226
USE sql_tests_test_array_agg;
select array_agg( name order by 1,score,4.00),array_agg(distinct name order by 1,score,4.00) from ss order by 1;

-- query 227
USE sql_tests_test_array_agg;
select array_agg( null order by score,4.00) from ss order by 1;

-- query 228
USE sql_tests_test_array_agg;
select array_agg( score order by 1) from ss order by 1;

-- query 229
USE sql_tests_test_array_agg;
select array_agg( score order by 1,name) from ss order by 1;

-- query 230
USE sql_tests_test_array_agg;
select array_agg(distinct 1 order by 1) from ss order by 1;

-- query 231
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 232
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 233
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 234
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 235
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 236
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 237
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 238
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 239
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 240
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 241
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 242
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 243
-- @skip_result_check=true
USE sql_tests_test_array_agg;
set enable_query_cache = true;

-- query 244
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1), count(distinct score), array_agg(name order by 1) from ss order by 1;

-- query 245
USE sql_tests_test_array_agg;
select count(distinct score), array_agg(name order by 1) from ss order by 1;

-- query 246
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss group by id order by 1;

-- query 247
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss group by id order by id;

-- query 248
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss group by id order by 1;

-- query 249
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss group by id order by 1;

-- query 250
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss group by id order by 1;

-- query 251
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss group by id order by 1;

-- query 252
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score, 1)) from ss group by id order by 1;

-- query 253
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1, score), count(distinct id), array_agg(name order by 1) from ss order by 1;

-- query 254
USE sql_tests_test_array_agg;
select array_agg(distinct subject order by 1, name) from ss order by 1;

-- query 255
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1), cardinality(array_agg(distinct arr order by score)) from ss order by 1;

-- query 256
USE sql_tests_test_array_agg;
select array_agg(name order by 1), array_agg(distinct name order by 1, score) from ss order by 1;

-- query 257
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1,score), array_agg(distinct score order by 1) from ss order by 1;

-- query 258
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1) from ss order by 1;

-- query 259
USE sql_tests_test_array_agg;
select cardinality(array_agg(distinct arr order by score, 1)) from ss order by 1;

-- query 260
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by 1;

-- query 261
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss group by id order by 1;

-- query 262
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss group by id order by id;

-- query 263
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 264
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss group by id order by id;

-- query 265
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss group by id order by id;

-- query 266
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 267
USE sql_tests_test_array_agg;
select array_agg(distinct name order by 1 asc), array_agg(distinct name order by 1 desc) from ss order by 1;

-- query 268
USE sql_tests_test_array_agg;
select array_agg(name order by 1 asc), array_agg(name order by 1 desc) from ss order by 1;

-- query 269
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 270
USE sql_tests_test_array_agg;
select array_agg(name order by 1 desc nulls first), array_agg(name order by 1 desc nulls last) from ss order by 1;

-- query 271
USE sql_tests_test_array_agg;
select array_agg(name order by 1 nulls first), array_agg(name order by 1 nulls last) from ss order by 1;

-- query 272
-- @expect_error=array_agg should have at least one input.
USE sql_tests_test_array_agg;
select array_agg();

-- query 273
-- @expect_error=array_agg should have at least one input.
USE sql_tests_test_array_agg;
select array_agg() from ss;

-- query 274
USE sql_tests_test_array_agg;
select array_agg(',');

-- query 275
USE sql_tests_test_array_agg;
select array_agg("中国" order by 1, id) from ss;

-- query 276
-- @expect_error=Unexpected input 'separator', the most similar input is {',', ')'}.
USE sql_tests_test_array_agg;
select array_agg("中国" order by 2, id separator NULL) from ss;

-- query 277
-- @expect_error=Unexpected input 'order', the most similar input is {',', ')'}.
USE sql_tests_test_array_agg;
select array_agg("中国",name order by 2, "第一", id) from ss;

-- query 278
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE sql_tests_test_array_agg;
select array_agg(  order by score) from ss order by 1;

-- query 279
-- @expect_error=Unexpected input 'order', the most similar input is {a legal identifier}.
USE sql_tests_test_array_agg;
select array_agg(distinct  order by score) from ss order by 1;

-- query 280
USE sql_tests_test_array_agg;
select array_agg([1,2]) from ss;

-- query 281
USE sql_tests_test_array_agg;
select array_agg(json_object("2:3")) from ss;

-- query 282
-- @expect_error=array_agg(DISTINCT json_object('2:3')) can't rewrite distinct to group by on (json).
USE sql_tests_test_array_agg;
select array_agg(distinct json_object("2:3")) from ss;

-- query 283
-- @expect_error=array_agg(DISTINCT [json_object('2:3')]) can't rewrite distinct to group by on (array<json>).
USE sql_tests_test_array_agg;
select array_agg(distinct [json_object("2:3")]) from ss;

-- query 284
USE sql_tests_test_array_agg;
select array_agg(map(2,3)) from ss;

-- query 285
-- @expect_error=Unknown error
USE sql_tests_test_array_agg;
select array_agg(distinct map(2,3)) from ss;

-- query 286
USE sql_tests_test_array_agg;
select array_agg(null);

-- query 287
-- @expect_error=Unexpected input '(', the most similar input is {<EOF>, ';'}.
USE sql_tests_test_array_agg;
select array_agg(order by 1 separator '');

-- query 288
-- @expect_error=No viable statement for input 'array_agg(separator NULL'.
USE sql_tests_test_array_agg;
select array_agg(separator NULL);

-- query 289
USE sql_tests_test_array_agg;
select array_agg_distinct(name order by 1 asc), array_agg(distinct name order by 1 desc), array_agg(name order by 1 desc) from ss order by 1;

-- query 290
USE sql_tests_test_array_agg;
select array_agg_distinct(name order by 1), cardinality(array_agg_distinct(arr order by score)) from ss group by id order by 1;

-- name: test_array_agg_all_type
-- query 291
-- @skip_result_check=true
USE sql_tests_test_array_agg;
drop table if exists test_array_agg;

-- query 292
-- @skip_result_check=true
USE sql_tests_test_array_agg;
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
USE sql_tests_test_array_agg;
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
USE sql_tests_test_array_agg;
set enable_per_bucket_optimize=false;

-- query 295
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg;

-- query 296
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_tinyint ORDER BY id DESC) FROM test_array_agg;

-- query 297
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_smallint ORDER BY id ASC) FROM test_array_agg;

-- query 298
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg;

-- query 299
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_bigint ORDER BY id ASC) FROM test_array_agg;

-- query 300
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_largeint ORDER BY id DESC) FROM test_array_agg;

-- query 301
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_float ORDER BY id ASC) FROM test_array_agg;

-- query 302
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_double ORDER BY id DESC) FROM test_array_agg;

-- query 303
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg;

-- query 304
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_char ORDER BY id DESC) FROM test_array_agg;

-- query 305
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_datetime ORDER BY id ASC) FROM test_array_agg;

-- query 306
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_date ORDER BY id DESC) FROM test_array_agg;

-- query 307
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_array ORDER BY id ASC) FROM test_array_agg;

-- query 308
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_map ORDER BY id DESC) FROM test_array_agg;

-- query 309
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(col_struct ORDER BY id ASC) FROM test_array_agg;

-- query 310
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 311
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_tinyint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 312
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_smallint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 313
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 314
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_bigint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 315
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_largeint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 316
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_float ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 317
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_double ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 318
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 319
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_char ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 320
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_datetime ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 321
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_date ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 322
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_array ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 323
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_map ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 324
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(col_struct ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 325
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg;

-- query 326
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_tinyint ORDER BY id DESC) FROM test_array_agg;

-- query 327
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_smallint ORDER BY id ASC) FROM test_array_agg;

-- query 328
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg;

-- query 329
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_bigint ORDER BY id ASC) FROM test_array_agg;

-- query 330
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_largeint ORDER BY id DESC) FROM test_array_agg;

-- query 331
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_float ORDER BY id ASC) FROM test_array_agg;

-- query 332
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_double ORDER BY id DESC) FROM test_array_agg;

-- query 333
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg;

-- query 334
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_char ORDER BY id DESC) FROM test_array_agg;

-- query 335
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_datetime ORDER BY id ASC) FROM test_array_agg;

-- query 336
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_date ORDER BY id DESC) FROM test_array_agg;

-- query 337
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_array ORDER BY id ASC) FROM test_array_agg;

-- query 338
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_map ORDER BY id DESC) FROM test_array_agg;

-- query 339
USE sql_tests_test_array_agg;
SELECT ARRAY_AGG(DISTINCT col_struct ORDER BY id ASC) FROM test_array_agg;

-- query 340
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_boolean ORDER BY id ASC NULLS LAST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 341
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_tinyint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 342
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_smallint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 343
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_int ORDER BY id DESC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 344
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_bigint ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 345
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_largeint ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 346
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_float ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 347
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_double ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 348
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_varchar ORDER BY id ASC NULLS FIRST) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 349
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_char ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 350
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_datetime ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 351
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_date ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 352
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_array ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 353
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_map ORDER BY id DESC) FROM test_array_agg GROUP BY id ORDER BY id;

-- query 354
USE sql_tests_test_array_agg;
SELECT id, ARRAY_AGG(DISTINCT col_struct ORDER BY id ASC) FROM test_array_agg GROUP BY id ORDER BY id;
