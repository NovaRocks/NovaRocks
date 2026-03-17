-- Migrated from dev/test/sql/test_function/T/test_typeof
-- Test Objective:
-- 1. Validate typeof() returns correct type names for all scalar literal types (tinyint through json, including BINARY).
-- 2. Validate typeof() for complex types: array, map, struct, bitmap, hll, json.
-- 3. Validate typeof() with table columns preserves the stored column types.
-- 4. Validate typeof() with BITMAP and HLL aggregate columns from AGGREGATE KEY tables.

-- query 1
select typeof(cast(1 as tinyint));

-- query 2
select typeof(cast(1 as smallint));

-- query 3
select typeof(cast(1 as int));

-- query 4
select typeof(cast(1 as bigint));

-- query 5
select typeof(cast(1 as largeint));

-- query 6
select typeof(cast(1 as decimal(19, 2)));

-- query 7
select typeof(cast(1 as double));

-- query 8
select typeof(cast(1 as float));

-- query 9
select typeof(cast(1 as boolean));

-- query 10
select typeof(cast(1 as char));

-- query 11
select typeof(cast(1 as string));

-- query 12
select typeof(cast(1 as varchar));

-- query 13
select typeof(cast('s' as BINARY));

-- query 14
select typeof(cast('2023-03-07' as date));

-- query 15
select typeof(cast('2023-03-07 11:22:33' as datetime));

-- query 16
select typeof([1, 2, 3]);

-- query 17
select typeof(get_json_object('{"k1":1, "k2":"v2"}', '$.k1'));

-- query 18
select typeof(map{1:"apple", 2:"orange", 3:"pear"});

-- query 19
select typeof(struct(1, 2, 3, 4));

-- query 20
select typeof(bitmap_empty());

-- query 21
select typeof(hll_empty());

-- query 22
select typeof(parse_json('{"a": 1, "b": true}'));

-- query 23
select typeof(null);

-- query 24
-- @skip_result_check=true
USE ${case_db};
create table t1 properties("replication_num" = "1") as
select cast(1 as tinyint) as c1
,cast(1 as smallint) as c2
,cast(1 as int) as c3
,cast(1 as bigint) as c4
,cast(1 as largeint) as c5
,cast(1 as decimal(19, 2)) as c6
,cast(1 as double) as c7
,cast(1 as float) as c8
,cast(1 as boolean) as c9
,cast(1 as char) as c10
,cast(1 as string) as c11
,cast(1 as varchar) as c12
,cast('s' as BINARY) as c13
,cast('2023-03-07' as date) as c14
,cast('2023-03-07 11:22:33' as datetime) as c15
,[1, 2, 3] as c16
,get_json_object('{"k1":1, "k2":"v2"}', '$.k1') as c17
,map{1:"apple", 2:"orange", 3:"pear"} as c18
,struct(1, 2, 3, 4) as c19
,parse_json('{"a": 1, "b": true}') as c20;

-- query 25
USE ${case_db};
select typeof(c1)
  ,typeof(c2)
  ,typeof(c3)
  ,typeof(c4)
  ,typeof(c5)
  ,typeof(c6)
  ,typeof(c7)
  ,typeof(c8)
  ,typeof(c9)
  ,typeof(c10)
  ,typeof(c11)
  ,typeof(c12)
  ,typeof(c13)
  ,typeof(c14)
  ,typeof(c15)
  ,typeof(c16)
  ,typeof(c17)
  ,typeof(c18)
  ,typeof(c19)
  ,typeof(c20)
  from t1;

-- query 26
-- @skip_result_check=true
CREATE TABLE ${case_db}.pv_bitmap (
    dt INT(11) NULL COMMENT "",
    page VARCHAR(10) NULL COMMENT "",
    user_id bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(dt, page)
COMMENT "OLAP"
DISTRIBUTED BY HASH(dt) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- query 27
-- @skip_result_check=true
USE ${case_db};
insert into pv_bitmap values(1, 'test', to_bitmap(10));

-- query 28
USE ${case_db};
select typeof(user_id) from pv_bitmap;

-- query 29
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_uv(
    dt date,
    id int,
    uv_set hll hll_union
)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- query 30
-- @skip_result_check=true
USE ${case_db};
insert into test_uv values('2024-01-01', 1, hll_hash(10));

-- query 31
USE ${case_db};
select typeof(uv_set) from test_uv;
