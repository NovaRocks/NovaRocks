-- Migrated from dev/test/sql/test_cast/T/test_cast_json_to_struct
-- Test Objective:
-- 1. Validate CAST(JSON array as STRUCT) with positional field mapping.
-- 2. Validate CAST(JSON object as STRUCT) with named field mapping.
-- 3. Cover missing fields (null padding), type coercion (float→int), extra fields (truncation).
-- 4. Cover nested structs, array fields, and deeply nested combinations.

-- query 1
-- Array JSON → struct with positional mapping, exact match
select cast(PARSE_JSON('[1,2,3]') as struct<col1 int, col2 int, col3 int>);

-- query 2
-- Array JSON → struct, fewer fields than JSON elements (truncation)
select cast(PARSE_JSON('[1,2,3]') as struct<col1 int, col2 int>);

-- query 3
-- Array JSON → struct, more fields than JSON elements (null padding)
select cast(PARSE_JSON('[1,2,3]') as struct<col1 int, col2 int, col3 int, col4 int>);

-- query 4
-- Array JSON with string element that cannot convert to int → null in that slot
select cast(PARSE_JSON('[1,   2,    3, "a"]') as struct<col1 int, col2 int, col3 int, col4 int>);

-- query 5
-- Float values cast into double struct fields
select cast(PARSE_JSON('[1.1, 2.2, 3.3]') as struct<col1 double, col2 double, col3 double>);

-- query 6
-- Float values: last field cast from double to int (truncation)
select cast(PARSE_JSON('[1.1, 2.2, 3.3]') as struct<col1 double, col2 double, col3 int>);

-- query 7
-- Object JSON → struct with named field mapping (reordered)
select cast(PARSE_JSON('{"star": "rocks", "number": 1}') as struct<number int, star varchar>);

-- query 8
-- Object JSON → struct: field not found in JSON → null
select cast(PARSE_JSON('{"star": "rocks", "number": 1}') as struct<number int, not_found varchar>);

-- query 9
-- Object JSON with array field → array<int> in struct
select cast(PARSE_JSON('{"star": "rocks", "number": [1, 2, 3]}') as struct<number array<int>, not_found varchar>);

-- query 10
-- Array JSON → struct where second element is array<json>
select cast(PARSE_JSON('[1, [{"star": "rocks"}, {"star": "rocks"}]]') as struct<col1 int, col2 array<json>>);

-- query 11
-- Deeply nested struct: object JSON with nested array and nested struct
select cast(PARSE_JSON('{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}') as struct<star varchar(10), length int, numbers array<int>, nest struct<col1 int, col2 int, col3 int>>);

-- query 12
-- Array of structs: each element is a complex struct with nested array and nested struct
select cast(PARSE_JSON('[{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}, {"star" : "rockses", "length": 33, "numbers": [2, 5, 9], "nest": [3, 6, 9]}]') as array<struct<star varchar(10), length int, numbers array<int>, nest struct<col1 int, col2 int, col3 int>>>);
