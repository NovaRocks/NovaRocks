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
-- 1. Validate column pruning for complex types: nested MAP, STRUCT with sub-fields.
-- 2. Cover field access, subscript access, join with pruning, UNION ALL with pruning.
-- 3. Cover struct/row construction and casting.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.sc2 (
  "v1" BIGINT,
  "map1" MAP<int,MAP<int,MAP<int,int>>>,
  "st1" ROW(s1 int, sm2 MAP<int,int>, sm3 MAP<int,MAP<int,int>>),
  "st2" ROW(s1 int, sa2 ARRAY<int>, ss3 ROW(sss1 int, sss2 ROW(ssss1 int, ssss2 int)))
) WITH (format_version = 3);

CREATE TABLE ${case_db}.t1 (
  "v1" BIGINT,
  "v2" BIGINT,
  "v3" BIGINT
) WITH (format_version = 3);

insert into ${case_db}.t1 values ROW(1, 2, 3);
insert into ${case_db}.t1 values ROW(2, 2, 2);
insert into ${case_db}.t1 values ROW(3, 3, 3);
insert into ${case_db}.t1 values ROW(4, 4, 4);
insert into ${case_db}.t1 values ROW(5, 5, 5);

insert into ${case_db}.sc2 values ROW(1, MAP(ARRAY[1], ARRAY[MAP(ARRAY[10], ARRAY[MAP(ARRAY[100], ARRAY[101])])]), row(1, MAP(ARRAY[10], ARRAY[101]), MAP(ARRAY[10], ARRAY[MAP(ARRAY[100], ARRAY[101])])), row(1, [1,10,101], row(1, row(10, 11))));
insert into ${case_db}.sc2 values ROW(2, MAP(ARRAY[2], ARRAY[MAP(ARRAY[20], ARRAY[MAP(ARRAY[200], ARRAY[202])])]), row(2, MAP(ARRAY[20], ARRAY[202]), MAP(ARRAY[20], ARRAY[MAP(ARRAY[200], ARRAY[202])])), row(2, [2,20,202], row(2, row(20, 22))));
insert into ${case_db}.sc2 values ROW(3, MAP(ARRAY[3], ARRAY[MAP(ARRAY[30], ARRAY[MAP(ARRAY[300], ARRAY[303])])]), row(3, MAP(ARRAY[30], ARRAY[303]), MAP(ARRAY[30], ARRAY[MAP(ARRAY[300], ARRAY[303])])), row(3, [3,30,303], row(3, row(30, 33))));
insert into ${case_db}.sc2 values ROW(4, MAP(ARRAY[4], ARRAY[MAP(ARRAY[40], ARRAY[MAP(ARRAY[400], ARRAY[404])])]), row(4, MAP(ARRAY[40], ARRAY[404]), MAP(ARRAY[40], ARRAY[MAP(ARRAY[400], ARRAY[404])])), row(4, [4,40,404], row(4, row(40, 44))));
insert into ${case_db}.sc2 values ROW(5, MAP(ARRAY[5], ARRAY[MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[505])])]), row(5, MAP(ARRAY[50], ARRAY[505]), MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[505])])), row(5, [5,50,505], row(5, row(50, 55))));
insert into ${case_db}.sc2 values ROW(5, MAP(ARRAY[5], ARRAY[MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[506])])]), row(5, MAP(ARRAY[50], ARRAY[506]), MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[506])])), row(5, [5,50,506], row(5, row(50, 56))));
insert into ${case_db}.sc2 values ROW(5, MAP(ARRAY[5], ARRAY[MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[507])])]), row(5, MAP(ARRAY[50], ARRAY[507]), MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[507])])), row(5, [5,50,507], row(5, row(50, 57))));
insert into ${case_db}.sc2 values ROW(5, MAP(ARRAY[5], ARRAY[MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[508])])]), row(5, MAP(ARRAY[50], ARRAY[508]), MAP(ARRAY[50], ARRAY[MAP(ARRAY[500], ARRAY[508])])), row(5, [5,50,508], row(5, row(50, 58))));

-- query 2
select st1.s1, st2.sa2 from ${case_db}.sc2;

-- query 3
select element_at(st1.sm2, 10), cardinality(element_at(element_at(map1, 3), 30)), st2.sa2[1]  from ${case_db}.sc2;

-- query 4
select st2.ss3.sss1, st2.ss3.sss2.ssss2, st2.ss3 from ${case_db}.sc2;

-- query 5
select * from ${case_db}.t1 join ${case_db}.sc2 on ${case_db}.t1.v2 = ${case_db}.sc2.v1;

-- query 6
select st1.sm2, map_keys(map1) from ${case_db}.t1 join ${case_db}.sc2 on ${case_db}.t1.v2 = ${case_db}.sc2.v1;

-- query 7
select st1.sm2, map_keys(map1) from ${case_db}.t1 join ${case_db}.sc2 on ${case_db}.t1.v2 = ${case_db}.sc2.v1;

-- query 8
select * from ${case_db}.sc2 union all select * from ${case_db}.sc2;

-- query 9
select x1.st1.sm2, x1.st2.ss3 from ${case_db}.sc2 x1 union all select element_at(x2.st1.sm3, 10), x2.st2.ss3 from ${case_db}.sc2 x2;

-- query 10
select x.st2.ss3 from (select * from ${case_db}.sc2 union all select * from ${case_db}.sc2) x;

-- query 11
-- Inline struct construction
select row(1,2,3);

-- query 12
select * from ${case_db}.t1;

-- query 13
select row(1,v1,'a') from ${case_db}.t1;

-- query 14
select CAST(ROW(v1, v2) AS ROW(a BIGINT, b BIGINT)) from ${case_db}.t1;

-- query 15
select CAST(ROW(v1, v2, v3) AS ROW(col1 BIGINT, col2 BIGINT, col3 BIGINT)) from ${case_db}.t1;

-- query 16
select row(map1) from ${case_db}.sc2;

-- query 17
select row(map1), row(map1).col1 from ${case_db}.sc2;

-- query 18
select CAST(ROW(st1, st2) AS ROW(a ROW(s1 INTEGER, sm2 MAP(INTEGER, INTEGER), sm3 MAP(INTEGER, MAP(INTEGER, INTEGER))), b ROW(s1 INTEGER, sa2 ARRAY(INTEGER), ss3 ROW(sss1 INTEGER, sss2 ROW(ssss1 INTEGER, ssss2 INTEGER))))) from ${case_db}.sc2;

-- query 19
select CAST(ROW(st1, st2) AS ROW(a ROW(s1 INTEGER, sm2 MAP(INTEGER, INTEGER), sm3 MAP(INTEGER, MAP(INTEGER, INTEGER))), b ROW(s1 INTEGER, sa2 ARRAY(INTEGER), ss3 ROW(sss1 INTEGER, sss2 ROW(ssss1 INTEGER, ssss2 INTEGER)))))[1] from ${case_db}.sc2;

-- query 20
select CAST(ROW(st1, st2) AS ROW(a ROW(s1 INTEGER, sm2 MAP(INTEGER, INTEGER), sm3 MAP(INTEGER, MAP(INTEGER, INTEGER))), b ROW(s1 INTEGER, sa2 ARRAY(INTEGER), ss3 ROW(sss1 INTEGER, sss2 ROW(ssss1 INTEGER, ssss2 INTEGER)))))[1].sm2 from ${case_db}.sc2;

-- query 21
-- Cast struct to different field types
select cast(row(1,2,3) as ROW(a double, b VARCHAR, c BIGINT));

-- query 22
select CAST(ROW(1, 2, 3) AS ROW(a DOUBLE, b VARCHAR, c BIGINT));

-- query 23
with input as (select CAST(ROW(ARRAY[1, 2, 3], ARRAY[4, 5, 6]) AS ROW(col1 ARRAY(INTEGER), col2 ARRAY(INTEGER))) as s union all select CAST(ROW(ARRAY[5, 6, 7], ARRAY[6, 7]) AS ROW(col1 ARRAY(INTEGER), col2 ARRAY(INTEGER))) as s) select s, s.col1 from input;

-- query 24
with input as (select CAST(ROW(ARRAY[1, 2, 3], ARRAY[4, 5, 6]) AS ROW(col1 ARRAY(INTEGER), col2 ARRAY(INTEGER))) as s union all select null as s union all select CAST(ROW(ARRAY[5, 6, 7], ARRAY[6, 7]) AS ROW(col1 ARRAY(INTEGER), col2 ARRAY(INTEGER))) as s) select s.col1 from input;
