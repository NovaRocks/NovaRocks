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
SELECT concat('a', CAST(ARRAY[] AS ARRAY(VARCHAR))) AS empty_tail;
-- query 2
SELECT concat(ARRAY['a'], ARRAY['b'], ARRAY['c']) AS mixed;
-- query 3
SELECT concat(ARRAY[CAST(NULL AS VARCHAR)], ARRAY['b']) AS null_element;
-- query 4
SELECT concat('a', CAST(NULL AS ARRAY(VARCHAR))) AS null_array;
-- query 5
SELECT ARRAY[
    concat('a', CAST(ARRAY[] AS ARRAY(VARCHAR))),
    concat('b', CAST(ARRAY[] AS ARRAY(VARCHAR)))
] AS nested;
-- query 6
SELECT concat('a','b') AS string_value;
-- query 7
WITH q(l) AS (VALUES CAST(ARRAY[] AS ARRAY(VARCHAR)))
SELECT l, ARRAY[concat('a',l), concat('b',l)] AS nested FROM q;
-- query 8
SELECT concat(CAST(9223372036854775807 AS BIGINT), CAST(ARRAY[1] AS ARRAY(BIGINT))) AS exact_integer;

-- query 9
SELECT concat(ARRAY[CAST(1 AS INTEGER)], ARRAY[CAST(2147483648 AS BIGINT)]) AS combined,
       concat(CAST(1 AS INTEGER), ARRAY[CAST(2147483648 AS BIGINT)]) AS prepended,
       concat(ARRAY[CAST(1 AS INTEGER)], CAST(2147483648 AS BIGINT)) AS appended;
-- query 10
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT concat(ARRAY['a'], ARRAY[1]);
-- query 11
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
SELECT concat(ARRAY['a'], MAP(ARRAY[1], ARRAY[2]));
