-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- Test Objective:
-- 1. Validate binary comparison operators (=, !=, <=>) on complex types:
--    ARRAY, MAP, STRUCT (including nested, with NULLs).
-- 2. Cover WHERE clause filtering and SELECT expression evaluation.
-- 3. Validate type mismatch errors for incompatible operands.

-- query 1
-- @skip_result_check=true
-- Keep Map values nullable; keys obey the frozen non-null contract.
-- fixture_order preserves source row identity under distributed sorting.
CREATE TABLE ${case_db}.sc2 (
  "v1" BIGINT,
  "array1" ARRAY<INT> ,
  "array2" ARRAY<MAP<INT, INT>> ,
  "array3" ARRAY<ROW(a INT, b INT)> ,
  "map1" MAP<INT, INT> ,
  "map2" MAP<INT, ARRAY<INT>> ,
  "map3" MAP<INT, ROW(c INT, b INT)> ,
  "st1" ROW(s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 ROW(e INT, f INT)),
  "fixture_order" INT NOT NULL
) WITH (format_version = 3);

insert into ${case_db}.sc2 values ROW(1, [11,NULL,31,41], [MAP(ARRAY[11], ARRAY[110]), MAP(ARRAY[101], ARRAY[101])], [row(11, 12), row(12, 13)], MAP(ARRAY[14, 999, 12], ARRAY[41, 11, 31]), MAP(ARRAY[101], ARRAY[[1, 10, 11, 23]]), MAP(ARRAY[101], ARRAY[row(NULL, 12)]), row(1, [1, 10, 11], MAP(ARRAY[101, 999], ARRAY[111, NULL]), row(111, 110)), 1);
insert into ${case_db}.sc2 values ROW(2, [12,22,32,42], [MAP(ARRAY[22], ARRAY[220]), MAP(ARRAY[999], ARRAY[201])], [row(21, 22), NULL], NULL, MAP(ARRAY[202], ARRAY[[2, 20, NULL, 23]]), MAP(ARRAY[202], ARRAY[row(21, 22)]), row(2, NULL, MAP(ARRAY[202], ARRAY[NULL]), row(222, 220)), 2);
insert into ${case_db}.sc2 values ROW(3, NULL, [MAP(ARRAY[33], ARRAY[NULL]), MAP(ARRAY[303], ARRAY[301])], [row(31, 32), row(NULL, 33)], MAP(ARRAY[34, 36, 32], ARRAY[43, 13, NULL]), MAP(ARRAY[303], ARRAY[NULL]), MAP(ARRAY[303], ARRAY[row(31, 32)]), NULL, 3);
insert into ${case_db}.sc2 values ROW(4, [14,24,NULL,44], [MAP(ARRAY[44], ARRAY[440]), MAP(ARRAY[404], ARRAY[401])], [row(41, 42), row(42, 43)], MAP(ARRAY[44, 46, 42], ARRAY[44, 14, 34]), MAP(ARRAY[999], ARRAY[[4, NULL, 41, 23]]), MAP(ARRAY[404], ARRAY[row(41, 42)]), row(NULL, [4, NULL, 41], MAP(ARRAY[999], ARRAY[444]), NULL), 4);
insert into ${case_db}.sc2 values ROW(5, [15,25,35,45], [MAP(ARRAY[999], ARRAY[550]), MAP(ARRAY[505], ARRAY[501])], [row(51, 52), row(NULL, 53)], MAP(ARRAY[54, 56, 999], ARRAY[45, 15, NULL]), MAP(ARRAY[999], ARRAY[NULL]), MAP(ARRAY[505], ARRAY[row(NULL, 52)]), row(5, [5, 50, 51], NULL, row(NULL, 550)), 5);

insert into ${case_db}.sc2 values ROW(1, [11,3,31,41], [MAP(ARRAY[11], ARRAY[110]), MAP(ARRAY[101], ARRAY[101])], [row(11, 12), row(12, 13)], MAP(ARRAY[14, 3, 12], ARRAY[41, 11, 31]), MAP(ARRAY[101], ARRAY[[1, 10, 11, 23]]), MAP(ARRAY[101], ARRAY[row(3, 12)]), row(1, [1, 10, 11], MAP(ARRAY[101, 3], ARRAY[111, 3]), row(111, 110)), 6);
insert into ${case_db}.sc2 values ROW(2, [12,22,32,42], [MAP(ARRAY[22], ARRAY[220]), MAP(ARRAY[3], ARRAY[201])], [row(21, 22), row(12, 13)], MAP(ARRAY[34, 36, 32], ARRAY[43, 13, 3]), MAP(ARRAY[202], ARRAY[[2, 20, 3, 23]]), MAP(ARRAY[202], ARRAY[row(21, 22)]), row(2, [2, 3, 4], MAP(ARRAY[202], ARRAY[3]), row(222, 220)), 7);
insert into ${case_db}.sc2 values ROW(3, [12,22,32,42], [MAP(ARRAY[33], ARRAY[3]), MAP(ARRAY[303], ARRAY[301])], [row(31, 32), row(3, 33)], MAP(ARRAY[34, 36, 32], ARRAY[43, 13, 3]), MAP(ARRAY[303], ARRAY[[4, 3, 41, 23]]), MAP(ARRAY[303], ARRAY[row(31, 32)]), row(2, [2, 3, 4], MAP(ARRAY[201], ARRAY[3]), row(222, 220)), 8);
insert into ${case_db}.sc2 values ROW(4, [14,24,3,44], [MAP(ARRAY[44], ARRAY[440]), MAP(ARRAY[404], ARRAY[401])], [row(41, 42), row(42, 43)], MAP(ARRAY[44, 46, 42], ARRAY[44, 14, 34]), MAP(ARRAY[3], ARRAY[[4, 3, 41, 23]]), MAP(ARRAY[404], ARRAY[row(41, 42)]), row(3, [4, 3, 41], MAP(ARRAY[3], ARRAY[444]), row(110, 330)), 9);
insert into ${case_db}.sc2 values ROW(5, [15,25,35,45], [MAP(ARRAY[3], ARRAY[550]), MAP(ARRAY[505], ARRAY[501])], [row(51, 52), row(3, 53)], MAP(ARRAY[54, 56, 3], ARRAY[45, 15, 3]), MAP(ARRAY[3], ARRAY[[2, 20, 3, 23]]), MAP(ARRAY[505], ARRAY[row(3, 52)]), row(5, [5, 50, 51], MAP(ARRAY[4], ARRAY[555]), row(3, 550)), 10);

-- query 2
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array1 is null order by v1, fixture_order;

-- query 3
-- Ordinary equality is UNKNOWN for internal NULLs; WHERE retains only TRUE.
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array1 = [11,null,31,41] order by v1, fixture_order;

-- query 4
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array1 = [15,25,35,45] order by v1, fixture_order;

-- query 5
-- Type mismatch: array<int> vs array<map<varchar,tinyint>>
-- @expect_error=does not support binary predicate operation
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array1 = [MAP(ARRAY['a', 'b'], ARRAY[21, 22]),null] order by v1, fixture_order;

-- query 6
-- Type mismatch: array<struct> vs array<map>
-- @expect_error=does not support binary predicate operation
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array3 = [MAP(ARRAY['a', 'b'], ARRAY[21, 22]),null] order by v1, fixture_order;

-- query 7
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array3 = [CAST(ROW(21,22) AS ROW(a INTEGER,b INTEGER)),null] order by v1, fixture_order;

-- query 8
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array3 = [row(21,22),null] order by v1, fixture_order;

-- query 9
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array2 = [MAP(ARRAY[999], ARRAY[550]),MAP(ARRAY[505], ARRAY[501])] order by v1, fixture_order;

-- query 10
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array2 = [MAP(ARRAY[44], ARRAY[440]),MAP(ARRAY[404], ARRAY[401])] order by v1, fixture_order;

-- query 11
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = null order by v1, fixture_order;

-- query 12
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[14, 999, 12], ARRAY[41, 11, 31]) order by v1, fixture_order;

-- query 13
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[44, 46, 42], ARRAY[44, 14, 34]) order by v1, fixture_order;

-- query 14
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[34, 36, 32], ARRAY[43, 13, null]) order by v1, fixture_order;

-- query 15
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map2 = MAP(ARRAY[505], ARRAY[[5,50,51,23]]) order by v1, fixture_order;

-- query 16
select cast(row(1, null) as ROW(a int, b array<int>));

-- query 17
select cast(MAP(ARRAY[1], ARRAY[null]) as map<int, array<int>>);

-- query 18
-- BOOLEAN to a MAP field is rejected while binding the conversion.
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
select cast(row(1, TRUE) as ROW(a int, b map<int, int>));

-- query 19
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map2 = MAP(ARRAY[999], ARRAY[null]) order by v1, fixture_order;

-- query 20
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map2 = MAP(ARRAY[999], ARRAY[[4,null,41,23]]) order by v1, fixture_order;

-- query 21
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map2 = MAP(ARRAY[101], ARRAY[[1,10,11,23]]) order by v1, fixture_order;

-- query 22
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[46, 42, 44], ARRAY[14, 34, 44]) order by v1, fixture_order;

-- query 23
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[54, 56], ARRAY[45, 15]) order by v1, fixture_order;

-- query 24
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[999, 54, 56], ARRAY[null, 45, 15]) order by v1, fixture_order;

-- query 25
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map3 = MAP(ARRAY[505], ARRAY[row(null, 52)]) order by v1, fixture_order;

-- query 26
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where st1 = row(2,null, MAP(ARRAY[202], ARRAY[null]), row(222,220)) order by v1, fixture_order;

-- query 27
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where st1 = CAST(ROW(5, ARRAY[5,50,51], NULL, ROW(NULL,550)) AS ROW(s1 INTEGER,s2 ARRAY(INTEGER),s3 MAP(INTEGER,INTEGER),s4 ROW(e INTEGER,f INTEGER))) order by v1, fixture_order;

-- query 28
-- = null is always NULL
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array1 = null order by v1, fixture_order;

-- query 29
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where st1 = null order by v1, fixture_order;

-- query 30
-- SELECT expression: = in projection
select array1 = [11,null,31,41] from ${case_db}.sc2 order by v1, fixture_order;

-- query 31
select array1 = [15,25,35,45] from ${case_db}.sc2 order by v1, fixture_order;

-- query 32
select array3 = [CAST(ROW(21,22) AS ROW(a INTEGER,b INTEGER)),null] from ${case_db}.sc2 order by v1, fixture_order;

-- query 33
select array3 = [row(21,22),null] from ${case_db}.sc2 order by v1, fixture_order;

-- query 34
select array2 = [MAP(ARRAY[999], ARRAY[550]),MAP(ARRAY[505], ARRAY[501])] from ${case_db}.sc2 order by v1, fixture_order;

-- query 35
select array2 = [MAP(ARRAY[44], ARRAY[440]),MAP(ARRAY[404], ARRAY[401])] from ${case_db}.sc2 order by v1, fixture_order;

-- query 36
select map1 = null from ${case_db}.sc2 order by v1, fixture_order;

-- query 37
select map1 = MAP(ARRAY[14, 999, 12], ARRAY[41, 11, 31]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 38
select map1 = MAP(ARRAY[44, 46, 42], ARRAY[44, 14, 34]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 39
select map1 = MAP(ARRAY[34, 36, 32], ARRAY[43, 13, null]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 40
select map2 = MAP(ARRAY[505], ARRAY[[5,50,51,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 41
select map2 = MAP(ARRAY[999], ARRAY[null]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 42
select map2 = MAP(ARRAY[999], ARRAY[[4,null,41,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 43
select map2 = MAP(ARRAY[101], ARRAY[[1,10,11,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 44
select map1 = MAP(ARRAY[46, 42, 44], ARRAY[14, 34, 44]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 45
select map1 = MAP(ARRAY[54, 56], ARRAY[45, 15]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 46
select map1 = MAP(ARRAY[999, 54, 56], ARRAY[null, 45, 15]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 47
select map3 = MAP(ARRAY[505], ARRAY[row(null, 52)]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 48
select st1 = row(2,null, MAP(ARRAY[202], ARRAY[null]), row(222,220)) from ${case_db}.sc2 order by v1, fixture_order;

-- query 49
select st1 = CAST(ROW(5, ARRAY[5,50,51], NULL, ROW(NULL,550)) AS ROW(s1 INTEGER,s2 ARRAY(INTEGER),s3 MAP(INTEGER,INTEGER),s4 ROW(e INTEGER,f INTEGER))) from ${case_db}.sc2 order by v1, fixture_order;

-- query 50
select array1 = null from ${case_db}.sc2 order by v1, fixture_order;

-- query 51
select st1 = null from ${case_db}.sc2 order by v1, fixture_order;

-- query 52
-- Non-null variant of WHERE clauses
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array1 = [11,3,31,41] order by v1, fixture_order;

-- query 53
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array1 = [15,25,35,45] order by v1, fixture_order;

-- query 54
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array3 = [CAST(ROW(21,22) AS ROW(a INTEGER,b INTEGER)), row(12, 13)] order by v1, fixture_order;

-- query 55
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array3 = [row(21,22)] order by v1, fixture_order;

-- query 56
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array2 = [MAP(ARRAY[3], ARRAY[550]),MAP(ARRAY[505], ARRAY[501])] order by v1, fixture_order;

-- query 57
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where array2 = [MAP(ARRAY[44], ARRAY[440]),MAP(ARRAY[404], ARRAY[401])] order by v1, fixture_order;

-- query 58
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[14, 3, 12], ARRAY[41, 11, 31]) order by v1, fixture_order;

-- query 59
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[44, 46, 42], ARRAY[44, 14, 34]) order by v1, fixture_order;

-- query 60
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[34, 36, 32], ARRAY[43, 13, 3]) order by v1, fixture_order;

-- query 61
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map2 = MAP(ARRAY[505], ARRAY[[5,50,51,23]]) order by v1, fixture_order;

-- query 62
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map2 = MAP(ARRAY[3], ARRAY[[4,3,41,23]]) order by v1, fixture_order;

-- query 63
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map2 = MAP(ARRAY[101], ARRAY[[1,10,11,44]]) order by v1, fixture_order;

-- query 64
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map1 = MAP(ARRAY[46, 42, 44], ARRAY[14, 34, 44]) order by v1, fixture_order;

-- query 65
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where map3 = MAP(ARRAY[505], ARRAY[row(3, 52)]) order by v1, fixture_order;

-- query 66
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where st1 = row(2,[2,3,4], MAP(ARRAY[202], ARRAY[3]), row(222,220)) order by v1, fixture_order;

-- query 67
-- Type mismatch: st1.s3 is MAP but literal has scalar int for s3
-- @expect_error=does not support binary predicate operation
select v1, array1, array2, array3, map1, map2, map3, st1 from ${case_db}.sc2 where st1 = CAST(ROW(5, ARRAY[5,50,51], 3, ROW(3,550)) AS ROW(s1 INTEGER,s2 ARRAY(INTEGER),s3 INTEGER,s4 ROW(e INTEGER,f INTEGER))) order by v1, fixture_order;

-- query 68
-- <=> (null-safe equality)
select array1  IS NOT DISTINCT FROM  [11,null,31,41] from ${case_db}.sc2 order by v1, fixture_order;

-- query 69
select array1  IS NOT DISTINCT FROM  [15,25,35,45] from ${case_db}.sc2 order by v1, fixture_order;

-- query 70
select array3  IS NOT DISTINCT FROM  [CAST(ROW(21,22) AS ROW(a INTEGER,b INTEGER)),null] from ${case_db}.sc2 order by v1, fixture_order;

-- query 71
select array3  IS NOT DISTINCT FROM  [row(21,22),null] from ${case_db}.sc2 order by v1, fixture_order;

-- query 72
select array2  IS NOT DISTINCT FROM  [MAP(ARRAY[999], ARRAY[550]),MAP(ARRAY[505], ARRAY[501])] from ${case_db}.sc2 order by v1, fixture_order;

-- query 73
select array2  IS NOT DISTINCT FROM  [MAP(ARRAY[44], ARRAY[440]),MAP(ARRAY[404], ARRAY[401])] from ${case_db}.sc2 order by v1, fixture_order;

-- query 74
select map1  IS NOT DISTINCT FROM  null from ${case_db}.sc2 order by v1, fixture_order;

-- query 75
select map1  IS NOT DISTINCT FROM  MAP(ARRAY[14, 999, 12], ARRAY[41, 11, 31]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 76
select map1  IS NOT DISTINCT FROM  MAP(ARRAY[44, 46, 42], ARRAY[44, 14, 34]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 77
select map1  IS NOT DISTINCT FROM  MAP(ARRAY[34, 36, 32], ARRAY[43, 13, null]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 78
select map2  IS NOT DISTINCT FROM  MAP(ARRAY[505], ARRAY[[5,50,51,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 79
select map2  IS NOT DISTINCT FROM  MAP(ARRAY[999], ARRAY[null]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 80
select map2  IS NOT DISTINCT FROM  MAP(ARRAY[999], ARRAY[[4,null,41,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 81
select map2  IS NOT DISTINCT FROM  MAP(ARRAY[101], ARRAY[[1,10,11,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 82
select map1  IS NOT DISTINCT FROM  MAP(ARRAY[46, 42, 44], ARRAY[14, 34, 44]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 83
select map1  IS NOT DISTINCT FROM  MAP(ARRAY[54, 56], ARRAY[45, 15]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 84
select map1  IS NOT DISTINCT FROM  MAP(ARRAY[999, 54, 56], ARRAY[null, 45, 15]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 85
select map3  IS NOT DISTINCT FROM  MAP(ARRAY[505], ARRAY[row(null, 52)]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 86
select st1  IS NOT DISTINCT FROM  row(2,null, MAP(ARRAY[202], ARRAY[null]), row(222,220)) from ${case_db}.sc2 order by v1, fixture_order;

-- query 87
select st1  IS NOT DISTINCT FROM  CAST(ROW(5, ARRAY[5,50,51], NULL, ROW(NULL,550)) AS ROW(s1 INTEGER,s2 ARRAY(INTEGER),s3 MAP(INTEGER,INTEGER),s4 ROW(e INTEGER,f INTEGER))) from ${case_db}.sc2 order by v1, fixture_order;

-- query 88
select array1  IS NOT DISTINCT FROM  null from ${case_db}.sc2 order by v1, fixture_order;

-- query 89
select st1  IS NOT DISTINCT FROM  null from ${case_db}.sc2 order by v1, fixture_order;

-- query 90
-- != operator
select array1 != [11,null,31,41] from ${case_db}.sc2 order by v1, fixture_order;

-- query 91
select array1 != [15,25,35,45] from ${case_db}.sc2 order by v1, fixture_order;

-- query 92
select array3 != [CAST(ROW(21,22) AS ROW(a INTEGER,b INTEGER)),null] from ${case_db}.sc2 order by v1, fixture_order;

-- query 93
select array3 != [row(21,22),null] from ${case_db}.sc2 order by v1, fixture_order;

-- query 94
select array2 != [MAP(ARRAY[999], ARRAY[550]),MAP(ARRAY[505], ARRAY[501])] from ${case_db}.sc2 order by v1, fixture_order;

-- query 95
select array2 != [MAP(ARRAY[44], ARRAY[440]),MAP(ARRAY[404], ARRAY[401])] from ${case_db}.sc2 order by v1, fixture_order;

-- query 96
select map1 != null from ${case_db}.sc2 order by v1, fixture_order;

-- query 97
select map1 != MAP(ARRAY[14, 999, 12], ARRAY[41, 11, 31]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 98
select map1 != MAP(ARRAY[44, 46, 42], ARRAY[44, 14, 34]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 99
select map1 != MAP(ARRAY[34, 36, 32], ARRAY[43, 13, null]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 100
select map2 != MAP(ARRAY[505], ARRAY[[5,50,51,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 101
select map2 != MAP(ARRAY[999], ARRAY[null]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 102
select map2 != MAP(ARRAY[999], ARRAY[[4,null,41,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 103
select map2 != MAP(ARRAY[101], ARRAY[[1,10,11,23]]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 104
select map1 != MAP(ARRAY[46, 42, 44], ARRAY[14, 34, 44]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 105
select map1 != MAP(ARRAY[54, 56], ARRAY[45, 15]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 106
select map1 != MAP(ARRAY[999, 54, 56], ARRAY[null, 45, 15]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 107
select map3 != MAP(ARRAY[505], ARRAY[row(null, 52)]) from ${case_db}.sc2 order by v1, fixture_order;

-- query 108
select st1 != row(2,null, MAP(ARRAY[202], ARRAY[null]), row(222,220)) from ${case_db}.sc2 order by v1, fixture_order;

-- query 109
select st1 != CAST(ROW(5, ARRAY[5,50,51], NULL, ROW(NULL,550)) AS ROW(s1 INTEGER,s2 ARRAY(INTEGER),s3 MAP(INTEGER,INTEGER),s4 ROW(e INTEGER,f INTEGER))) from ${case_db}.sc2 order by v1, fixture_order;

-- query 110
select array1 != null from ${case_db}.sc2 order by v1, fixture_order;

-- query 111
select st1 != null from ${case_db}.sc2 order by v1, fixture_order;

-- query 112
-- Frozen Map keys reject construction before comparison.
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
select MAP(ARRAY[1, null], ARRAY[1, 2]) = MAP(ARRAY[1, 2], ARRAY[1, null]);

-- query 113
-- Frozen Map keys reject construction before comparison.
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
select MAP(ARRAY[1, null], ARRAY[1, 2]) != MAP(ARRAY[1, 2], ARRAY[1, null]);

-- query 114
-- result not equal
select MAP(ARRAY[1, 999], ARRAY[1, 2]) != MAP(ARRAY[999, 3], ARRAY[2, null]);

-- query 115
select MAP(ARRAY[1, 999], ARRAY[1, 2]) = MAP(ARRAY[999, 3], ARRAY[2, null]);

-- query 116
select MAP(ARRAY[1, 999], ARRAY[1, 2]) = MAP(ARRAY[999, 1], ARRAY[3, null]);

-- query 117
select MAP(ARRAY[1, 999], ARRAY[1, 2]) != MAP(ARRAY[999, 1], ARRAY[3, null]);

-- query 118
-- A definite mismatch dominates UNKNOWN, even after a NULL field or value.
select row(cast(null as int), 1) = row(cast(null as int), 2) as struct_eq,
       row(cast(null as int), 1) != row(cast(null as int), 2) as struct_ne,
       MAP(ARRAY[1, 2], ARRAY[null, 3]) = MAP(ARRAY[1, 2], ARRAY[null, 4]) as map_eq,
       MAP(ARRAY[1, 2], ARRAY[null, 3]) != MAP(ARRAY[1, 2], ARRAY[null, 4]) as map_ne,
       row(1,cast(null as int)) = row(1,cast(null as int)) as struct_unknown,
       row(1,cast(null as int))  IS NOT DISTINCT FROM  row(1,cast(null as int)) as struct_safe,
       MAP(ARRAY[1, 2], ARRAY[null, 3])  IS NOT DISTINCT FROM  MAP(ARRAY[1, 2], ARRAY[null, 3]) as map_safe;

-- query 119
-- The stored column is an independent oracle for both NULL depths.
select v1, array1 = array1 as eq, array1 != array1 as ne, array1  IS NOT DISTINCT FROM  array1 as safe
from ${case_db}.sc2 order by v1, eq, fixture_order;
