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

-- query 1
SELECT [JSON '{"2:3": null}'] AS json_values,
       [CAST('{"2:3": null}' AS VARCHAR)] AS string_values;

-- query 2
SELECT array_sortby([JSON '{"k": 1}', JSON '{"k": 2}'], [2,1]) AS json_values,
       array_sortby([CAST('{"k": 1}' AS VARCHAR), CAST('{"k": 2}' AS VARCHAR)], [2,1]) AS string_values;

-- query 3
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
WITH q AS (SELECT [JSON '{"key": "value1"}', JSON '{"key": "value2"}'] AS j)
SELECT array_sortby(j,[2,1]) AS json_values,
       array_sortby(CAST(j AS ARRAY<VARCHAR>),[2,1]) AS string_values FROM q;

-- query 4
SELECT CAST([] AS ARRAY<JSON>) AS json_values, CAST([] AS ARRAY<VARCHAR>) AS string_values;

-- query 5
-- Preserve the original first projection independently of the retired object-to-text conversion.
WITH q AS (SELECT [JSON '{"key": "value1"}', JSON '{"key": "value2"}'] AS j)
SELECT array_sortby(j,[2,1]) AS json_values FROM q;

-- query 6
SELECT CAST(['abc','{"k":1}','null',NULL] AS ARRAY<JSON>) AS json_values;

-- query 7
SELECT TRY_CAST(['abc','{"k":1}','null',NULL] AS ARRAY<JSON>) AS json_values;

-- query 8
SELECT CAST([JSON '100.0',JSON '1e2',JSON 'null',JSON '"a\n中"',NULL] AS ARRAY<VARCHAR>) AS string_values;

-- query 9
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT CAST([JSON '[1'] AS ARRAY<VARCHAR>);

-- query 10
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT TRY_CAST([JSON '[1'] AS ARRAY<VARCHAR>) AS whole_array;

-- query 11
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT CAST([JSON '1e309'] AS ARRAY<VARCHAR>);

-- query 12
-- @expect_trino_error=INVALID_LITERAL,USER_ERROR
SELECT TRY_CAST([JSON '1e309'] AS ARRAY<VARCHAR>) AS whole_array;

-- query 13
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST([JSON 'null',JSON '{"k":1}'] AS ARRAY<VARCHAR>);

-- query 14
SELECT TRY_CAST([JSON 'null',JSON '{"k":1}'] AS ARRAY<VARCHAR>) AS whole_array;
