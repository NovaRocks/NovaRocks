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
-- Numeric and object equality follow the Trino 484 JSON value policy.
-- scale: True
SELECT JSON '1.0' IN (SELECT JSON '1.00') AS included;
-- exponent_integer: True
SELECT JSON '1e2' IN (SELECT JSON '100') AS included;
-- exponent_decimal: True
SELECT JSON '1e2' IN (SELECT JSON '100.0') AS included;
-- zero_exponent: True
SELECT JSON '100e0' IN (SELECT JSON '100') AS included;
-- scientific: True
SELECT JSON '1.5e-7' IN (SELECT JSON '0.00000015') AS included;
-- negative_zero: True
SELECT JSON '-0.0' IN (SELECT JSON '0.0') AS included;
-- integer_decimal: True
SELECT JSON '1' IN (SELECT JSON '1.0') AS included;
-- objects: True
SELECT JSON '{"b":2,"a":1}' IN (SELECT JSON '{"a":1,"b":2}') AS included;
-- arrays: False
SELECT JSON '[1,2]' IN (SELECT JSON '[2,1]') AS included;
-- duplicate_object_key: False
SELECT JSON '{"a":1,"a":2}' IN (SELECT JSON '{"a":2}') AS included;
-- big_integer_identity: True
SELECT JSON '123456789012345678901234567890' IN (SELECT JSON '123456789012345678901234567890') AS included;
-- big_integer_distinct: False
SELECT JSON '123456789012345678901234567890' IN (SELECT JSON '123456789012345678901234567891') AS included;
-- large_exponent: True
SELECT JSON '1e23' IN (SELECT JSON '100000000000000000000000') AS included;
-- raw_marker_is_ordinary_key: False
SELECT JSON '{"$serde_json::private::RawValue":"1"}' IN (SELECT JSON '1') AS included;
-- json_null: True
SELECT JSON 'null' IN (SELECT JSON 'null') AS included;
-- unmatched_sql_null: None
SELECT JSON '10' IN (SELECT JSON '11' UNION ALL SELECT json_parse(NULL)) AS included;
-- not_unmatched_sql_null: None
SELECT JSON '10' NOT IN (SELECT JSON '11' UNION ALL SELECT json_parse(NULL)) AS included;
-- empty_sql_null: False
SELECT json_parse(NULL) IN (SELECT JSON 'null' WHERE FALSE) AS included;
-- varchar_numeric: False
SELECT '1.0' IN ('1.00') AS included;
-- varchar_object: False
SELECT '{"b":1,"a":2}' IN ('{"a":2,"b":1}') AS included;
-- json_inlist_scale: True
SELECT JSON '1.0' IN (JSON '1.00') AS included;
-- json_inlist_object: True
SELECT JSON '{"b":1,"a":2}' IN (JSON '{"a":2,"b":1}') AS included;
-- native_real: False
SELECT JSON '0.1' IN (SELECT json_scalar(REAL '0.1')) AS included;
-- unicode_keys: True
SELECT JSON '{"\uffff":1,"\ud800\udc00":2,"z":3,"é":4}' IN (SELECT JSON '{"é":4,"z":3,"𐀀":2,"￿":1}') AS included;
-- Both the list and materialized mark keys preserve exact instants.
SELECT json_scalar(TIMESTAMP '2024-10-01 04:00:00.123 UTC')
       IN (json_scalar(TIMESTAMP '2024-10-01 00:00:00.123 America/New_York'))
   AND json_scalar(TIMESTAMP '2024-10-01 04:00:00.123 UTC')
       IN (SELECT json_scalar(TIMESTAMP '2024-10-01 00:00:00.123 America/New_York')) AS included;
SELECT json_scalar(TIMESTAMP '1969-12-31 23:59:59.123')
       IN (SELECT json_scalar(TIMESTAMP '1969-12-31 23:59:59.123000')) AS included;
SELECT json_scalar(TIME '00:30:00.000000000000 +01:00')
       IN (SELECT json_scalar(TIME '23:30:00.000000000001 +00:00')) AS included;
SELECT JSON '{"a":1,"a":2}'
       IN (SELECT JSON '{"a":2e0,"a":1.00}') AS included;
SELECT JSON '0.1' IN (json_scalar(DOUBLE '0.1')) AS included;
SELECT JSON '-0e0' IN (SELECT JSON '0') AS included;
SELECT json_scalar(REAL 'NaN') IN (SELECT json_scalar(DOUBLE 'NaN')) AS included;
SELECT CAST(X'31' AS VARBINARY) IN (CAST(X'31' AS VARBINARY))
   AND BIGINT '4294967296' IN (BIGINT '4294967296') AS included;
-- Invalid literals fail at analysis before membership.
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT JSON 'not JSON' IN (SELECT JSON 'not JSON') AS included;
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT JSON 'not JSON' IN (JSON 'not JSON') AS included;
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT JSON '"\ud800"' IN (SELECT JSON '"\uD800"') AS included;
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT JSON '1e9999' IN (JSON '1') AS included;
-- @expect_error=[sql.analyze.unsupported_query_shape]
SELECT JSON '1' IN (SELECT CAST(NULL AS VARIANT)) AS included;
