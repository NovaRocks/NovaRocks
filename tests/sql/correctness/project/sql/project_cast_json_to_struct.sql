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

-- Migrated from dev/test/sql/test_cast/T/test_cast_json_to_struct
-- Test Objective:
-- 1. Validate CAST(JSON array as STRUCT) with positional field mapping.
-- 2. Validate CAST(JSON object as STRUCT) with named field mapping.
-- 3. Cover missing fields (null padding), type coercion (float→int), extra fields (truncation).
-- 4. Cover nested structs, array fields, and deeply nested combinations.

-- query 1
-- Array JSON → struct with positional mapping, exact match
select cast(json_parse('[1,2,3]') as ROW(col1 int, col2 int, col3 int));

-- query 2
-- Array JSON → struct, fewer fields than JSON elements (truncation)
select cast(json_parse('[1,2,3]') as ROW(col1 int, col2 int));

-- query 3
-- Array JSON → struct, more fields than JSON elements (null padding)
select cast(json_parse('[1,2,3]') as ROW(col1 int, col2 int, col3 int, col4 int));

-- query 4
-- Array JSON with string element that cannot convert to int → null in that slot
select cast(json_parse('[1,   2,    3, "a"]') as ROW(col1 int, col2 int, col3 int, col4 int));

-- query 5
-- Float values cast into double struct fields
select cast(json_parse('[1.1, 2.2, 3.3]') as ROW(col1 double, col2 double, col3 double));

-- query 6
-- Float values: last field cast from double to int (truncation)
select cast(json_parse('[1.1, 2.2, 3.3]') as ROW(col1 double, col2 double, col3 int));

-- query 7
-- Object JSON → struct with named field mapping (reordered)
select cast(json_parse('{"star": "rocks", "number": 1}') as ROW(number int, star varchar));

-- query 8
-- Object JSON → struct: field not found in JSON → null
select cast(json_parse('{"star": "rocks", "number": 1}') as ROW(number int, not_found varchar));

-- query 9
-- Object JSON with array field → array<int> in struct
select cast(json_parse('{"star": "rocks", "number": [1, 2, 3]}') as ROW(number array<int>, not_found varchar));

-- query 10
-- Array JSON → struct where second element is array<json>
select cast(json_parse('[1, [{"star": "rocks"}, {"star": "rocks"}]]') as ROW(col1 int, col2 array<json>));

-- query 11
-- Deeply nested struct: object JSON with nested array and nested struct
select cast(json_parse('{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}') as ROW(star varchar(10), length int, numbers array<int>, nest ROW(col1 int, col2 int, col3 int)));

-- query 12
-- Array of structs: each element is a complex struct with nested array and nested struct
select cast(json_parse('[{"star" : "rocks", "length": 5, "numbers": [1, 4, 7], "nest": [1, 2, 3]}, {"star" : "rockses", "length": 33, "numbers": [2, 5, 9], "nest": [3, 6, 9]}]') as array<ROW(star varchar(10), length int, numbers array<int>, nest ROW(col1 int, col2 int, col3 int))>);
