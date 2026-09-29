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
SELECT concat('a', CAST('[]' AS ARRAY<STRING>)) AS empty_tail;
-- query 2
SELECT concat('a', ['b'], 'c') AS mixed;
-- query 3
SELECT concat(CAST(NULL AS STRING), ['b']) AS null_element;
-- query 4
SELECT concat('a', CAST(NULL AS ARRAY<STRING>)) AS null_array;
-- query 5
SELECT array_map(x -> concat(x, []), ['a','b']) AS nested;
-- query 6
SELECT concat('a','b') AS string_value;
-- query 7
SELECT CAST('[]' AS ARRAY<STRING>) AS l, array_map(x -> concat(x,l), ['a','b']) AS nested;
-- query 8
SELECT concat(CAST(9223372036854775807 AS BIGINT), CAST([1] AS ARRAY<BIGINT>)) AS exact_integer;
