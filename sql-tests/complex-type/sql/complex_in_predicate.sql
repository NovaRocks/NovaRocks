-- Test Objective:
-- 1. Validate IN/NOT IN predicate with complex types: JSON, ARRAY, MAP, STRUCT.
-- 2. Cover NULL handling, empty arrays/maps, nested complex types in IN lists.
-- 3. Cover error paths: JSON subquery (not supported), type mismatch for each type.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.sc2 (
  `v1` bigint(20) NULL COMMENT "",
  `js` json NULL,
  `array1` ARRAY<INT> NULL,
  `array2` ARRAY<MAP<INT, INT>> NULL,
  `array3` ARRAY<STRUCT<a INT, b INT>> NULL,
  `map1` MAP<INT, INT> NULL,
  `map2` MAP<INT, ARRAY<INT>> NULL,
  `map3` MAP<INT, STRUCT<c INT, b INT>> NULL,
  `st1` STRUCT<s1 int, s2 ARRAY<INT>, s3 MAP<INT, INT>, s4 Struct<e INT, f INT>>
) ENGINE=OLAP
DUPLICATE KEY(`v1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`v1`) BUCKETS 3
PROPERTIES (
"replication_num" = "1",
"compression" = "LZ4"
);

insert into ${case_db}.sc2 values (0, null, null, null, null, null, null, null, null);
insert into ${case_db}.sc2 values (2, json_object("a", null), [1,3,2], [map{null:null}], [row(1, 2)], map{2:null}, map{2:[2,3,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,3], map{2:2, 1:1}, null));
insert into ${case_db}.sc2 values (3, json_object("a", 1,'b',2), [1,2,3], [],               [row(1, 2)], map{2:20, 1:null, 3:30}, map{2:[3,2,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)));
insert into ${case_db}.sc2 values (4, json_object("a", 1,null,null), [1,2,3], [map{2:20, null:10}],       [row(1, 3)], map{}, map{2:[3,2,4], 1:[1,2,3]}, map{2:row(2,4), 1:row(1,null)}, row(1, null, map{1:1, 2:2}, row(1, 2)));
insert into ${case_db}.sc2 values (5, json_object("a", 1,'b',null), [1,2,3], [null],       [row(1, 2)], map{1:10, 2:20, null:40}, map{1:[1,2,3], 2:[2,3,4]}, map{}, row(1, [], map{1:1, 2:null}, row(null, 2)));
insert into ${case_db}.sc2 values (6, json_object("a", 1,'b',3), [1,3,2], [map{}],       [row(1, 2)], map{1:10, 2:20, 4:40}, map{1:[2,1,3], 2:[2,3,4]}, map{1:row(2,3), 2:row(2,3)}, row(1, [null], map{}, row(1, 2)));
insert into ${case_db}.sc2 values (7, json_object("a", 1, 'b',4), [2,1,3], [map{1:10, 3:30, 2:20}], [], map{}, map{1:[2,1,3], 2:[2,4,3]}, map{1:row(2,3), 2:row(2,4)}, row(1, [1,2,3], map{2:null}, row(1, 2)));
insert into ${case_db}.sc2 values (8, json_object("a", 1, 'c', null), [2,1,3], [map{1:10, 3:30, 2:20}], [null], map{2:20, 1:10, 3:30}, map{1:[1,2,3], null:[2,4,3]}, map{null:row(2,4)}, row(1, null, map{2:2, 3:3, 1:1}, row(1, 2)));
insert into ${case_db}.sc2 values (9, json_object("a", 1), [],      [map{1:10, 3:30, 2:20}], [row(1, 2),null], map{1:10, 2:20, 3:30}, map{1:[1,2,3], 2:[2,4,3]}, map{1:row(1,3), 2:row(2,4)}, row(1, [1,2,3], map{2:2, 3:3, 1:1}, null));
insert into ${case_db}.sc2 values (10, json_object("a"), [1,2,null], [map{2:20, 1:10, null:30}], [row(1, 2)], map{1:10, 4:40}, map{2:[2,null,4], 1:[1,2,null]}, map{null:null}, row(1, [2,1,null], map{2:2, 1:1}, row(null, 2)));
insert into ${case_db}.sc2 values (11, json_object("", 2), [2,1,null], [map{2:20, 1:10, null:30}], [row(1, null)], map{}, map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,null], null, row(null, 2)));
insert into ${case_db}.sc2 values (12, parse_json('{}'), [1,2],  [map{2:20, 1:10, null:30}], [row(1, 2)], map{2:20, 1:10, null:30}, map{2:[2,null,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [null], map{null:2, 1:1}, row(null, 2)));
insert into ${case_db}.sc2 values (13, null, [],     [map{2:20, 1:10}],       [row(1, 2)], map{2:20, 1:10, null:30}, map{2:[null,2,4], 1:[1,2,null]}, map{2:row(2,4), 1:row(1,2)}, row(1, [2,1,null], map{1:1, 2:2}, row(null, 2)));

-- query 2
-- JSON IN predicate
select js in (parse_json('{}'),json_object('a',1)) from ${case_db}.sc2 order by 1;

-- query 3
select js not in (parse_json('{}'),json_object('a',1)) from ${case_db}.sc2 order by 1;

-- query 4
select parse_json('{}') in (js,json_object('a',1)) from ${case_db}.sc2 order by 1;

-- query 5
select parse_json('{}') not in (js,json_object('a',1)) from ${case_db}.sc2 order by 1;

-- query 6
select js in (null,json_object('a',1)) from ${case_db}.sc2 order by 1;

-- query 7
select js not in (null) from ${case_db}.sc2 order by 1;

-- query 8
select null in (js,json_object('a',1)) from ${case_db}.sc2 order by 1;

-- query 9
select null not in (js, json_object('a',3)) from ${case_db}.sc2 order by 1;

-- query 10
select js in (1,3,json_object('a',2)) from ${case_db}.sc2 order by 1;

-- query 11
select js not in (2,3) from ${case_db}.sc2 order by 1;

-- query 12
select js in (1,3,v1,json_object('a',11)) from ${case_db}.sc2 order by 1;

-- query 13
select js not in (2,3,v1) from ${case_db}.sc2 order by 1;

-- query 14
select js in (json_object("a", 1),json_object("ab", 12, 'bc',4),json_object("ab", 14),json_object("ac", 15, '6b',46)) from ${case_db}.sc2 order by 1;

-- query 15
select js not in (json_object("a", 1),json_object("ab", 13, 'b',4) ) from ${case_db}.sc2 order by 1;

-- query 16
-- JSON subquery not supported
-- @expect_error=In predicate of JSON does not support subquery
select js not in (select js from ${case_db}.sc2 where v1>3) from ${case_db}.sc2;

-- query 17
-- JSON subquery not supported
-- @expect_error=In predicate of JSON does not support subquery
select js in (select js from ${case_db}.sc2 where v1>3) from ${case_db}.sc2;

-- query 18
-- ARRAY IN predicates
select array1 in ([],[1]) from ${case_db}.sc2 order by v1;

-- query 19
select array1 not in ([],[1]) from ${case_db}.sc2 order by v1;

-- query 20
select array1 in ([null],[]) from ${case_db}.sc2 order by v1;

-- query 21
select array1 not in ([null],[3]) from ${case_db}.sc2 order by v1;

-- query 22
select array1 in ([1,null,2],[]) from ${case_db}.sc2 order by v1;

-- query 23
select array1 not in ([1,null,2],[]) from ${case_db}.sc2 order by v1;

-- query 24
select [] in (array1), [] not in (array1), [null] in (array1), [null] not in (array1),[] in (array2), [] not in (array2), [null] in (array2), [null] not in (array2),[] in (array3), [] not in (array3), [null] in (array3), [null] not in (array3) from ${case_db}.sc2 order by 1;

-- query 25
select array2 in ([],[map{}]),  array2 in ([null],[map{1:3}]), array2 in ([map{1:10, 3:30, 2:20}], [map{}])  from ${case_db}.sc2 order by 1;

-- query 26
select array2 not in ([],[map{}]),  array2 not in ([null],[map{1:3}]), array2 not in ([map{1:10, 3:30, 2:20}], [map{3:3}])  from ${case_db}.sc2 order by 1;

-- query 27
select array3 in ([]), array3 not in ([]), array3 in ([null]), array3 not in ([null]), array3 in ([row(1, 2)], [null]), array3 not in ([row(1, 2)], [null]) from ${case_db}.sc2 order by 1,2,3,4,5,6;

-- query 28
select array1 in (null), array1 not in (null), array2 in (null), array2 not in (null), array3 in (null), array3 not in (null) from ${case_db}.sc2 order by 1;

-- query 29
select null in (array1), null not in (array1), null in (array2), null not in (array2), null in (array3), null not in (array3) from ${case_db}.sc2 order by 1;

-- query 30
-- Type mismatch: ARRAY<INT> vs type from array<map>
-- @expect_error=of in predict are not compatible
select array2 in (map{}) from ${case_db}.sc2 order by 1;

-- query 31
-- Type mismatch: ARRAY<STRUCT> vs INT
-- @expect_error=in predicate type
select array3 in (33,1) from ${case_db}.sc2 order by 1;

-- query 32
-- MAP IN predicates
select map1 in (map{}), map1 not in (map{}), map2 in (map{}), map2 not in (map{}), map3 in (map{}), map3 not in (map{}) from ${case_db}.sc2 order by 1;

-- query 33
select map1 in (null), map1 not in (null), map2 in (null), map2 not in (null), map3 in (null), map3 not in (null) from ${case_db}.sc2 order by 1;

-- query 34
select null in (map1), null not in (map1), null in (map2), null not in (map2), null in (map3), null not in (map3) from ${case_db}.sc2 order by 1;

-- query 35
select map{} in (map1), map() not in (map1), map() in (map2), map() not in (map2), map() in (map3), map() not in (map3) from ${case_db}.sc2 order by 1;

-- query 36
select map1 in (map{1:10, 2:20, 4:40}, map{}), map2 in (map{2:[2,3,4], 1:[1,2,3]}, map{2:[2,3,4], 1:[1,22,3]}), map3 not in (map{2:row(2,4), 1:row(1,2)},map{2:row(2,4), 1:row(1,22)}) from ${case_db}.sc2 order by 1;

-- query 37
select map2 not in (map{2:[2,3,4], 1:[1,2,3]},null), map3 in (map{2:row(2,4), 1:row(1,2)}, null) from ${case_db}.sc2 order by 1;

-- query 38
select map2 in (map{1:[1,2,null],2:[2,4,null]},map{2:[2,3,4], 1:[1,2,3]}) from ${case_db}.sc2 order by 1;

-- query 39
select map2 not in (map{1:[1,2,null],2:[2,4,null]},map{2:[2,3,4], 1:[1,2,3]}) from ${case_db}.sc2 order by 1;

-- query 40
-- MAP<INT,INT> vs MAP<VARCHAR,VARCHAR>: FE coerces, returns 0 (not equal)
select map1 in (map{'a':'b'}) from ${case_db}.sc2 order by 1;

-- query 41
-- MAP<INT,ARRAY<INT>> vs MAP<VARCHAR,ARRAY<TINYINT>>: FE coerces, returns 0
select map2 in (map{'2':[3]}) from ${case_db}.sc2 order by 1;

-- query 42
-- Type mismatch: MAP<INT,INT> vs double
-- @expect_error=in predicate type
select map1 in (2,3) from ${case_db}.sc2 order by 1;

-- query 43
-- STRUCT IN predicates
select st1 in (null), st1 not in (null) from ${case_db}.sc2 order by 1;

-- query 44
select null in (st1), null not in (st1) from ${case_db}.sc2 order by 1;

-- query 45
-- row(null,null) inferred as struct<col1 bool, col2 bool> (2 fields), incompatible with st1 (4 fields)
-- @expect_error=of in predict are not compatible
select st1 in (row(null, null)), st1 not in (row(null,null)) from ${case_db}.sc2 order by 1;

-- query 46
-- row(null,null) inferred as struct<col1 bool, col2 bool> (2 fields), incompatible with st1 (4 fields)
-- @expect_error=of in predict are not compatible
select row(null,null) in (st1), row(null,null) not in (st1) from ${case_db}.sc2 order by 1;

-- query 47
select st1 in (row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)), row(12, [2,null,3], map{11:1, 2:2}, row(31, 2))) from ${case_db}.sc2 order by 1;

-- query 48
select st1 not in (row(1, [2,1,3], map{1:1, 2:2}, row(3, 2)), row(12, [2,null,3], map{11:1, 2:2}, row(31, 2))) from ${case_db}.sc2 order by 1;

-- query 49
select st1 in (row(1, [2,1,null], map{1:1, 2:2}, row(1, 2)),row(1, [2,1,3], map{1:1, 2:2}, row(3, 2))) from ${case_db}.sc2 order by 1;

-- query 50
select st1 not in (row(1, [2,1,3], map{1:1, 2:2}, row(1, 2)),row(1, [2,1,3], map{1:1, 2:2}, row(3, 2))) from ${case_db}.sc2 order by 1;

-- query 51
-- Type mismatch: STRUCT vs MAP
-- @expect_error=of in predict are not compatible
select st1 in (map{}) from ${case_db}.sc2 order by 1;

-- query 52
-- Type mismatch: STRUCT vs MAP (not in)
-- @expect_error=of in predict are not compatible
select st1 not in (map{}) from ${case_db}.sc2 order by 1;
