-- Test Objective:
-- 1. Validate IN predicate with complex types via CAST expressions.
-- 2. Cover CastStringToArray, CastJsonToArray, CastJsonToStruct,
--    CastArrayExpr, CastMapExpr, and CastStructExpr with IN/NOT IN predicates.
-- 3. Large dataset (1.28M rows) to test at scale.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.row_util_base (
  k1 bigint NULL
) DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
insert into ${case_db}.row_util_base select generate_series from TABLE(generate_series(0, 10000 - 1));
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 20000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 40000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 80000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 160000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 320000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 640000
insert into ${case_db}.row_util_base select * from ${case_db}.row_util_base; -- 1280000

CREATE TABLE ${case_db}.row_util (
  idx bigint NULL
) DISTRIBUTED BY HASH(`idx`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);
insert into ${case_db}.row_util select row_number() over() as idx from ${case_db}.row_util_base;

CREATE TABLE ${case_db}.t1 (
    k1 bigint NULL,
    c_int bigint NULL,
    c_json JSON NULL,
    c_array_int ARRAY<INT> NULL,
    c_map MAP<INT, INT> NULL,
    c_struct STRUCT<k1 INT, k2 INT> NULL
) DISTRIBUTED BY HASH(`k1`) BUCKETS 64
PROPERTIES (
    "replication_num" = "1"
);

INSERT INTO ${case_db}.t1
SELECT
    idx,
    idx,
    json_object('k1', idx, 'k2', idx + 1),
    [idx, idx + 1, idx + 2, idx + 3],
    map{0: idx, 1: idx + 1, 2: idx + 2},
    struct(idx, idx + 1)
FROM ${case_db}.row_util;

INSERT INTO ${case_db}.t1 (k1) SELECT idx from ${case_db}.row_util order by idx limit 10000;

-- query 2
-- CastStringToArray: IN with cast from string literal
select count(1) from ${case_db}.t1 where c_int > 0 AND (
    c_array_int in (cast("[10, 11,12,13]" as array<INT>))
    OR c_array_int in (cast("[20, 21,22,23]" as array<INT>))
);

-- query 3
-- CastJsonToArray: IN with cast from parse_json
select count(1) from ${case_db}.t1 where
    c_array_int in (cast(parse_json("[10, 11,12,13]") as array<INT>))
    OR c_array_int in (cast(parse_json("[20, 21,22,23]") as array<INT>));

-- query 4
-- CastJsonToArray element access: subscript on cast json array
select count(1) from ${case_db}.t1 where
    c_int in (cast(parse_json("[10, 11,12,13]") as array<INT>)[1])
    OR c_int in (cast(parse_json("[20, 21,22,23]") as array<INT>)[1]);

-- query 5
-- CastJsonToStruct: IN/NOT IN with cast from json
select count(1) from ${case_db}.t1 where
    c_struct in (cast(parse_json("[10, 11]") as struct<k1 INT, k2 INT>))
    OR c_struct not in (cast(parse_json("[20, 21]") as struct<k1 INT, k2 INT>));

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
    c_map in (map{0: 10, 1: 10 + 1, 2: 10 + 2}, map{0: 100000, 1: 100000 + 1, 2: 100000 + 2}, map{0: 1000000, 1: 1000000 + 1, 2: 1000000 + 2})
    OR c_map not in (map{0: 10, 1: 10 + 1, 2: 10 + 2}, map{0: 100000, 1: 100000 + 1, 2: 100000 + 2}, map{0: 1000000, 1: 1000000 + 1, 2: 1000000 + 2})
);

-- query 10
-- CastMapExpr: cast from string key map
select count(1) from ${case_db}.t1 where
    c_int > 0 and (
    c_map in (cast (map{0: '10', 1: 11, 2: 12} as map<int, int>))
    OR c_map in (cast (map{0: '20', 1: 21, 2: 22} as map<int, int>))
);

-- query 11
-- CastMapExpr element access
select count(1) from ${case_db}.t1 where
    c_int = cast (map{0: '10', 1: 11, 2: 12} as map<int, int>)[0]
    OR c_int = cast (map{0: '20', 1: 21, 2: 22} as map<int, int>)[0];

-- query 12
-- CastStructExpr: struct IN/NOT IN with struct literal
select count(1) from ${case_db}.t1 where
    c_struct in (struct(10, 11))
    OR c_struct not in (struct(20, 21)) ;
