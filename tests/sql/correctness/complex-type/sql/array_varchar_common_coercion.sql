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
-- ARRAY common VARCHAR coercion must use ordinary SQL CAST text, including
-- nested values; distinct VARCHAR spellings remain distinct.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- query 2
-- @skip_result_check=true
CREATE TABLE input_values (
    pk INT NOT NULL,
    floats ARRAY<DOUBLE>,
    floats32 ARRAY<FLOAT>,
    strings ARRAY<STRING>,
    datetimes ARRAY<DATETIME>,
    timestamp_strings ARRAY<STRING>
) TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
INSERT INTO input_values VALUES
    (1, [10.0], [10.0], ['10'], ['1970-01-01 00:00:00'], ['1970-01-01 00:00:00']),
    (2, [1.25], [1.25], ['1.25'], ['1970-01-01 00:00:00'], ['1970-01-01T00:00:00']),
    (3, [], [], ['10'], [], ['1970-01-01 00:00:00']),
    (4, NULL, NULL, ['10'], NULL, ['1970-01-01 00:00:00']);

-- query 4
SELECT pk, arrays_overlap(floats, strings) AS forward_overlap,
       arrays_overlap(strings, floats) AS reverse_overlap,
       arrays_overlap(floats32, strings) AS float32_overlap
FROM input_values ORDER BY pk;

-- query 5
SELECT pk, array_intersect(datetimes, timestamp_strings) AS forward_intersection,
       array_intersect(timestamp_strings, datetimes) AS reverse_intersection
FROM input_values ORDER BY pk;

-- query 6
-- VARCHAR identity: '10.0' must not be parsed as numeric 10.
SELECT pk, arrays_overlap(floats, ['10.0']) AS spelling_overlap
FROM input_values ORDER BY pk;

-- query 7
-- VARCHAR identity: the input T spelling is not normalized to a datetime.
SELECT pk, array_intersect(datetimes, ['1970-01-01T00:00:00']) AS spelling_intersection
FROM input_values ORDER BY pk;

-- query 8
SELECT CAST(CAST(10 AS DOUBLE) AS STRING) AS float_text,
       CAST(CAST('1970-01-01 00:00:00' AS DATETIME) AS STRING) AS datetime_text;

-- query 9
SELECT arrays_overlap(CAST([[10], [1.25, NULL]] AS ARRAY<ARRAY<DOUBLE>>), [['10']]) AS nested_overlap,
       arrays_overlap([['10']], CAST([[10], [1.25, NULL]] AS ARRAY<ARRAY<DOUBLE>>)) AS nested_reverse,
       arrays_overlap(CAST([[10]] AS ARRAY<ARRAY<DOUBLE>>), [['10.0']]) AS nested_spelling;

-- query 10
-- @skip_result_check=true
DROP TABLE input_values;
