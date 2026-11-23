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

-- Array functions match implicit argument types before execution.
-- Explicit conversion and set operations preserve exact VARCHAR spellings.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
USE ${case_db};

-- query 2
-- @skip_result_check=true
CREATE TABLE input_values (
    pk INTEGER NOT NULL,
    floats ARRAY(DOUBLE),
    floats32 ARRAY(REAL),
    strings ARRAY(VARCHAR),
    datetimes ARRAY(TIMESTAMP(6)),
    timestamp_strings ARRAY(VARCHAR)
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO input_values VALUES
    ROW(1, ARRAY[DOUBLE '10'], ARRAY[REAL '10'], ARRAY['1.0E1'],
        ARRAY[TIMESTAMP '1970-01-01 00:00:00.000000'], ARRAY['1970-01-01 00:00:00.000000']),
    ROW(2, ARRAY[DOUBLE '1.25'], ARRAY[REAL '1.25'], ARRAY['1.25E0'],
        ARRAY[TIMESTAMP '1970-01-01 00:00:00.000000'], ARRAY['1970-01-01T00:00:00']),
    ROW(3, ARRAY[], ARRAY[], ARRAY['1.0E1'], ARRAY[], ARRAY['1970-01-01 00:00:00.000000']),
    ROW(4, NULL, NULL, ARRAY['1.0E1'], NULL, ARRAY['1970-01-01 00:00:00.000000']);

-- query 4
SELECT pk, arrays_overlap(CAST(floats AS ARRAY(VARCHAR)), strings) AS forward_overlap,
       arrays_overlap(strings, CAST(floats AS ARRAY(VARCHAR))) AS reverse_overlap,
       arrays_overlap(CAST(floats32 AS ARRAY(VARCHAR)), strings) AS float32_overlap
FROM input_values ORDER BY pk;

-- query 5
SELECT pk, array_intersect(CAST(datetimes AS ARRAY(VARCHAR)), timestamp_strings) AS forward_intersection,
       array_intersect(timestamp_strings, CAST(datetimes AS ARRAY(VARCHAR))) AS reverse_intersection
FROM input_values ORDER BY pk;

-- query 6
SELECT pk, arrays_overlap(CAST(floats AS ARRAY(VARCHAR)), ARRAY['10.0']) AS spelling_overlap
FROM input_values ORDER BY pk;

-- query 7
SELECT pk, array_intersect(CAST(datetimes AS ARRAY(VARCHAR)), ARRAY['1970-01-01T00:00:00']) AS spelling_intersection
FROM input_values ORDER BY pk;

-- query 8
SELECT CAST(CAST(10 AS DOUBLE) AS VARCHAR) AS float_text,
       CAST(TIMESTAMP '1970-01-01 00:00:00.000000' AS VARCHAR) AS datetime_text;

-- query 9
SELECT arrays_overlap(CAST(ARRAY[ARRAY[DOUBLE '10'], ARRAY[DOUBLE '1.25',NULL]] AS ARRAY(ARRAY(VARCHAR))),
                      ARRAY[ARRAY['1.0E1']]) AS nested_overlap,
       arrays_overlap(ARRAY[ARRAY['1.0E1']],
                      CAST(ARRAY[ARRAY[DOUBLE '10'], ARRAY[DOUBLE '1.25',NULL]] AS ARRAY(ARRAY(VARCHAR)))) AS nested_reverse,
       arrays_overlap(CAST(ARRAY[ARRAY[DOUBLE '10']] AS ARRAY(ARRAY(VARCHAR))),
                      ARRAY[ARRAY['10.0']]) AS nested_spelling;

-- query 10
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT arrays_overlap(ARRAY[DOUBLE '10'], ARRAY['10']);

-- query 11
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
SELECT array_intersect(ARRAY[TIMESTAMP '1970-01-01 00:00:00.000000'], ARRAY['1970-01-01 00:00:00']);

-- query 12
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS input_values;
