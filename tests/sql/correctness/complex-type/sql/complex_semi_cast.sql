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
-- 1. Validate IN predicate with complex types via CAST expressions.
-- 2. Cover CastStringToArray, CastJsonToArray, CastJsonToStruct,
--    CastArrayExpr, CastMapExpr, and CastStructExpr with IN/NOT IN predicates.
-- 3. Functional-scale dataset with enough rows to cover hits, misses, and nulls.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.row_util_base (
  k1 bigint
) WITH (format_version = 3);
insert into ${case_db}.row_util_base select generate_series from TABLE(sequence(0, 5000 - 1)) AS generate_series(generate_series);
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 10000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 20000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 40000

CREATE TABLE ${case_db}.row_util (
  idx bigint
) WITH (format_version = 3);
insert into ${case_db}.row_util select row_number() over() as idx from ${case_db}.row_util_base;

CREATE TABLE ${case_db}.t1 (
    k1 bigint ,
    c_int bigint ,
    c_json VARCHAR ,
    c_array_int ARRAY<INT> ,
    c_map MAP<INT, INT> ,
    c_struct ROW(k1 INT, k2 INT)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t1
SELECT
    idx,
    idx,
    json_object('k1': idx, 'k2': idx + 1),
    [idx, idx + 1, idx + 2, idx + 3],
    MAP(ARRAY[0, 1, 2], ARRAY[idx, idx + 1, idx + 2]),
    ROW(idx, idx + 1)
FROM ${case_db}.row_util;

INSERT INTO ${case_db}.t1 (k1) SELECT idx from ${case_db}.row_util order by idx limit 1000;

-- query 2
-- CastStringToArray: IN with cast from string literal
select count(1) from ${case_db}.t1 where c_int > 0 AND (
    c_array_int in (cast('[10, 11,12,13]' as array<INT>))
    OR c_array_int in (cast('[20, 21,22,23]' as array<INT>))
);

-- query 3
-- CastJsonToArray: IN with cast from json_parse
select count(1) from ${case_db}.t1 where
    c_array_int in (cast(json_parse('[10, 11,12,13]') as array<INT>))
    OR c_array_int in (cast(json_parse('[20, 21,22,23]') as array<INT>));

-- query 4
-- CastJsonToArray element access: subscript on cast json array
select count(1) from ${case_db}.t1 where
    c_int in (cast(json_parse('[10, 11,12,13]') as array<INT>)[1])
    OR c_int in (cast(json_parse('[20, 21,22,23]') as array<INT>)[1]);

-- query 5
-- CastJsonToStruct: IN/NOT IN with cast from json
select count(1) from ${case_db}.t1 where
    c_struct in (cast(json_parse('[10, 11]') as ROW(k1 INT, k2 INT)))
    OR c_struct not in (cast(json_parse('[20, 21]') as ROW(k1 INT, k2 INT)));

-- query 6
-- CastArrayExpr: IN with array literal, NOT IN with array literal
select count(1) from ${case_db}.t1 where (
    c_array_int in ([10, 11, 12, 13])
    OR c_array_int not in ([11, 12, 12, 14])
);

-- query 7
-- CastArrayExpr: mixed string/int cast for IN
select count(1) from ${case_db}.t1 where (
    c_array_int in (cast(['1', 11, 12, 13] as array<int>))
    OR c_array_int not in ([11, 12, 12, 14])
);

-- query 8
-- CastArrayExpr element access
select count(1) from ${case_db}.t1 where
    c_int =cast(['1', 11, 12, 13] as array<int>)[1]
    OR c_int = [11, 12, 12, 14][1];

-- query 9
-- CastMapExpr: IN/NOT IN with map literals
select count(1) from ${case_db}.t1 where (
    c_map in (MAP(ARRAY[0, 1, 2], ARRAY[10, 10 + 1, 10 + 2]), MAP(ARRAY[0, 1, 2], ARRAY[1000, 1000 + 1, 1000 + 2]), MAP(ARRAY[0, 1, 2], ARRAY[10000, 10000 + 1, 10000 + 2]))
    OR c_map not in (MAP(ARRAY[0, 1, 2], ARRAY[10, 10 + 1, 10 + 2]), MAP(ARRAY[0, 1, 2], ARRAY[1000, 1000 + 1, 1000 + 2]), MAP(ARRAY[0, 1, 2], ARRAY[10000, 10000 + 1, 10000 + 2]))
);

-- query 10
-- CastMapExpr: cast from string key map
select count(1) from ${case_db}.t1 where
    c_int > 0 and (
    c_map in (cast (MAP(ARRAY[0, 1, 2], ARRAY['10', 11, 12]) as map<int, int>))
    OR c_map in (cast (MAP(ARRAY[0, 1, 2], ARRAY['20', 21, 22]) as map<int, int>))
);

-- query 11
-- CastMapExpr element access
select count(1) from ${case_db}.t1 where
    c_int = cast (MAP(ARRAY[0, 1, 2], ARRAY['10', 11, 12]) as map<int, int>)[0]
    OR c_int = cast (MAP(ARRAY[0, 1, 2], ARRAY['20', 21, 22]) as map<int, int>)[0];

-- query 12
-- CastStructExpr: struct IN/NOT IN with struct literal
select count(1) from ${case_db}.t1 where
    c_struct in (ROW(10, 11))
    OR c_struct not in (ROW(20, 21)) ;
