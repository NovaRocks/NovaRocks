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

-- @tags=subquery,json,mark_join,trino_semantics
-- Independent expected values: Trino handoff 2.1/2.2; raw typed JSON preserves scale.
-- scale: False
SELECT JSON '1.0' IN (SELECT JSON '1.00') AS included;
-- exponent_integer: False
SELECT JSON '1e2' IN (SELECT JSON '100') AS included;
-- exponent_decimal: False
SELECT JSON '1e2' IN (SELECT JSON '100.0') AS included;
-- zero_exponent: True
SELECT JSON '100e0' IN (SELECT JSON '100') AS included;
-- scientific: True
SELECT JSON '1.5e-7' IN (SELECT JSON '0.00000015') AS included;
-- negative_zero: True
SELECT JSON '-0.0' IN (SELECT JSON '0.0') AS included;
-- integer_decimal: False
SELECT JSON '1' IN (SELECT JSON '1.0') AS included;
-- objects: True
SELECT JSON '{"b":2,"a":1}' IN (SELECT JSON '{"a":1,"b":2}') AS included;
-- arrays: False
SELECT JSON '[1,2]' IN (SELECT JSON '[2,1]') AS included;
-- duplicate_object_key: True
SELECT JSON '{"a":1,"a":2}' IN (SELECT JSON '{"a":2}') AS included;
-- big_integer_identity: True
SELECT JSON '123456789012345678901234567890' IN (SELECT JSON '123456789012345678901234567890') AS included;
-- big_integer_distinct: False
SELECT JSON '123456789012345678901234567890' IN (SELECT JSON '123456789012345678901234567891') AS included;
-- large_exponent: True
SELECT JSON '1E400' IN (SELECT JSON '1e400') AS included;
-- raw_marker_is_ordinary_key: False
SELECT JSON '{"$serde_json::private::RawValue":"1"}' IN (SELECT JSON '1') AS included;
-- json_null: True
SELECT JSON 'null' IN (SELECT JSON 'null') AS included;
-- unmatched_sql_null: None
SELECT JSON '10' IN (SELECT JSON '11' UNION ALL SELECT parse_json(NULL)) AS included;
-- not_unmatched_sql_null: None
SELECT JSON '10' NOT IN (SELECT JSON '11' UNION ALL SELECT parse_json(NULL)) AS included;
-- empty_sql_null: False
SELECT parse_json(NULL) IN (SELECT JSON 'null' WHERE FALSE) AS included;
-- varchar_numeric: False
SELECT '1.0' IN ('1.00') AS included;
-- varchar_object: False
SELECT '{"b":1,"a":2}' IN ('{"a":2,"b":1}') AS included;
-- json_inlist_scale: False
SELECT JSON '1.0' IN (JSON '1.00') AS included;
-- json_inlist_object: True
SELECT JSON '{"b":1,"a":2}' IN (JSON '{"a":2,"b":1}') AS included;
-- isolated_surrogate: True
SELECT JSON '"\\ud800"' IN (SELECT JSON '"\\uD800"') AS included;
-- unicode_keys: True
SELECT JSON '{"\\uffff":1,"\\ud800\\udc00":2,"z":3,"é":4}' IN (SELECT JSON '{"é":4,"z":3,"𐀀":2,"￿":1}') AS included;
-- Invalid logically JSON values fail as Data, including equal raw text.
-- @expect_error=[query.execution.invalid_data]
SELECT JSON 'not JSON' IN (SELECT JSON 'not JSON') AS included;
-- @expect_error=[query.execution.invalid_data]
SELECT JSON 'not JSON' IN (JSON 'not JSON') AS included;
-- @expect_error=[sql.analyze.unsupported_query_shape]
SELECT JSON '1' IN (SELECT CAST(NULL AS VARIANT)) AS included;
-- Required JSON errors remain visible after a deciding match or SQL NULL.
-- @expect_error=[query.execution.invalid_data]
SELECT JSON '1' IN (JSON '1', JSON 'not JSON') AS included;
-- @expect_error=[query.execution.invalid_data]
SELECT JSON 'not JSON' IN (parse_json(NULL)) AS included;
-- @expect_error=[query.execution.invalid_data]
SELECT parse_json(NULL) IN (JSON 'not JSON') AS included;
