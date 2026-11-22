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
-- 1. Validate column pruning for complex types with array/map/struct field access in WHERE.
-- 2. Cover array element access, map element access, struct field access in projections and filters.
-- 3. Include dict-encoded string column with complex type pruning (scan with str filter).

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.sc3 (
  "v1" bigint(20),
  "a1" ARRAY<INT> ,
  "a2" ARRAY<ROW(a INT, b INT)> ,
  "m1" MAP<INT, INT> ,
  "m2" MAP<INT, ROW(c INT, b ARRAY<INT>)> ,
  "s1" ROW(s1 int, s2 ARRAY<ROW(a int, b int)>, s3 MAP<INT, INT>, s4 ROW(e INT, f INT))
) WITH (format_version = 3);

insert into ${case_db}.sc3 values ROW(1, [1,2,3],[row(1,11),row(2,21),row(3,31)], MAP(ARRAY[1, 2], ARRAY[11, 21]), MAP(ARRAY[1, 2], ARRAY[row(11, [111, 221, 331]), row(22, [221, 221, 331])]), row(1, [row(1,1), row(2,1)], MAP(ARRAY[1, 2], ARRAY[11, 21]), row(1,1)));
insert into ${case_db}.sc3 values ROW(2, [2,2,3],[row(1,12),row(2,22),row(3,32)], MAP(ARRAY[1, 2], ARRAY[12, 22]), MAP(ARRAY[1, 2], ARRAY[row(12, [112, 222, 332]), row(22, [222, 222, 332])]), row(1, [row(1,2), row(2,2)], MAP(ARRAY[1, 2], ARRAY[12, 22]), row(1,2)));
insert into ${case_db}.sc3 values ROW(3, [3,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));

-- query 2
select cardinality(a1) from ${case_db}.sc3;

-- query 3
select cardinality(a1) from ${case_db}.sc3 where a1[2] = 2;

-- query 4
select cardinality(a1), a1 from ${case_db}.sc3 where a1[2] = 2;

-- query 5
select cardinality(a2), a2[1].a from ${case_db}.sc3;

-- query 6
select cardinality(a2), a2[1].a from ${case_db}.sc3 where a2[1].b = 12;

-- query 7
select cardinality(a2), a2[1].a from ${case_db}.sc3 where a2[1] = row(1, 13);

-- query 8
select a2[1].a from ${case_db}.sc3 where a2[1].b = 13;

-- query 9
select cardinality(a2), a2[1].a from ${case_db}.sc3;

-- query 10
select cardinality(m1) from ${case_db}.sc3;

-- query 11
select cardinality(m1), map_keys(m1) from ${case_db}.sc3;

-- query 12
select map_values(m1), map_keys(m1) from ${case_db}.sc3;

-- query 13
select cardinality(m1) from ${case_db}.sc3 where m1[1] = 12;

-- query 14
select cardinality(m1), map_values(m1) from ${case_db}.sc3 where m1[2] = 23;

-- query 15
select cardinality(m1), m1[3] from ${case_db}.sc3 where m1[2] = 23;

-- query 16
select cardinality(m1) from ${case_db}.sc3 where m1[1] = 12;

-- query 17
select m2[1].c, cardinality(m2[2].b) from ${case_db}.sc3;

-- query 18
select m2[1].c, cardinality(m2[2].b) from ${case_db}.sc3 where m2[1] = row(11, [111, 221, 331]);

-- query 19
select m2[1].c, m2[2].b from ${case_db}.sc3 where m2[1].b[1] = 112;

-- query 20
select s1.s2[1].a from ${case_db}.sc3;

-- query 21
select s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.sc3;

-- query 22
select s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.sc3 where s1.s2 = [row(1,3), row(2,3)];

-- query 23
select s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.sc3 where s1.s2[2].a = 3;

-- query 24
select s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.sc3 where s1.s3[2] = 22;

-- query 25
select v1 from ${case_db}.sc3 where a1[1] not in (select tt.v1 from ${case_db}.sc3 as tt where false);

-- query 26
-- @skip_result_check=true
-- Setup second table with dict-encoded string + complex columns
CREATE TABLE ${case_db}.scs3 (
  "v1" bigint(20),
  "str" string,
  "a1" ARRAY<INT> ,
  "a2" ARRAY<ROW(a INT, b INT)> ,
  "m1" MAP<INT, INT> ,
  "m2" MAP<INT, ROW(c INT, b ARRAY<INT>)> ,
  "s1" ROW(s1 int, s2 ARRAY<ROW(a int, b int)>, s3 MAP<INT, INT>, s4 ROW(e INT, f INT))
) WITH (format_version = 3);

insert into ${case_db}.scs3 values ROW(1, 'aa', [1,2,3],[row(1,11),row(2,21),row(3,31)], MAP(ARRAY[1, 2], ARRAY[11, 21]), MAP(ARRAY[1, 2], ARRAY[row(11, [111, 221, 331]), row(22, [221, 221, 331])]), row(1, [row(1,1), row(2,1)], MAP(ARRAY[1, 2], ARRAY[11, 21]), row(1,1)));
insert into ${case_db}.scs3 values ROW(2, 'bb', [2,2,3],[row(1,12),row(2,22),row(3,32)], MAP(ARRAY[1, 2], ARRAY[12, 22]), MAP(ARRAY[1, 2], ARRAY[row(12, [112, 222, 332]), row(22, [222, 222, 332])]), row(1, [row(1,2), row(2,2)], MAP(ARRAY[1, 2], ARRAY[12, 22]), row(1,2)));
insert into ${case_db}.scs3 values ROW(3, 'cc', [3,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 values ROW(4, 'dd', [4,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 values ROW(5, 'ee', [5,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 values ROW(6, 'ff', [6,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 values ROW(7, 'gg', [7,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 values ROW(8, 'hh', [8,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 values ROW(9, 'ii', [9,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 values ROW(10, 'kk' ,[10,2,3],[row(1,13),row(2,23),row(3,33)], MAP(ARRAY[1, 2], ARRAY[13, 23]), MAP(ARRAY[1, 2], ARRAY[row(13, [113, 223, 333]), row(22, [223, 223, 333])]), row(1, [row(1,3), row(2,3)], MAP(ARRAY[1, 2], ARRAY[13, 23]), row(1,3)));
insert into ${case_db}.scs3 select * from ${case_db}.scs3;

-- query 27
select a1, v1 from ${case_db}.scs3 where str = 'aa' order by v1 limit 2;

-- query 28
select cardinality(a1), a2[1].a  from ${case_db}.scs3 where str = 'aa' order by v1 limit 2;

-- query 29
select cardinality(m1), s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.scs3 where str = 'aa' order by v1 limit 2;

-- query 30
select a1, a2[1] from ${case_db}.scs3 where str in ('aa', 'bb') order by v1 limit 2;

-- query 31
select cardinality(a1), a2[1].a  from ${case_db}.scs3 where str in ('aa', 'bb', 'cc') order by v1 limit 2;

-- query 32
select cardinality(m1), s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.scs3 where str in ('aa', 'bb', 'cc') order by v1 limit 2;

-- query 33
select a1, s1.s3 from ${case_db}.scs3 where str in ('aa', 'bb', 'cc', 'dd', 'ee', 'ff') order by v1 limit 2;

-- query 34
select cardinality(a1), a2[1].a  from ${case_db}.scs3 where str in ('aa', 'bb', 'cc', 'gg', 'kk') order by v1 limit 2;

-- query 35
select cardinality(m1), s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.scs3 where str in ('aa', 'bb', 'ii', 'cc') order by v1 limit 2;

-- query 36
select a1, m1 from ${case_db}.scs3 where str = 'aa' and a1[2] = 2 order by v1 limit 2;

-- query 37
select cardinality(a1), a2[1].a  from ${case_db}.scs3 where str = 'aa' and s1.s1 > 2 order by v1 limit 2;

-- query 38
select cardinality(m1), s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.scs3 where str = 'aa' and m1[2] > 0 order by v1 limit 2;

-- query 39
select a1, m1 from ${case_db}.scs3 where str = 'aa' and a1[2] = 2 order by v1 limit 2;

-- query 40
select cardinality(a1), a2[1].a  from ${case_db}.scs3 where str = 'aa' and s1.s1 > 2 order by v1 limit 2;

-- query 41
select cardinality(m1), s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.scs3 where str = 'aa' and m1[2] > 0 order by v1 limit 2;

-- query 42
select a1, m1 from ${case_db}.scs3 where str >= 'aa' and a1[2] = 2 order by v1 limit 2;

-- query 43
select cardinality(a1), a2[1].a  from ${case_db}.scs3 where str <= 'gg' and s1.s1 > 2 order by v1 limit 2;

-- query 44
select cardinality(m1), s1.s2[1].a, s1.s3[1], s1.s4.f from ${case_db}.scs3 where str != 'aa' and m1[2] > 0 order by v1 limit 2;
