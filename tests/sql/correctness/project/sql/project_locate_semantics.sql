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

-- @order_sensitive=true
-- @tags=project,string
-- Search by code-point position, keeping position zero when no match exists.
-- query 1
WITH inputs(needle, source, start_pos) AS (
    VALUES ('b', 'abc', 2), ('', 'abc', 2), ('é', 'aébc', 2), ('z', 'abc', 2)
), suffix_matches AS (
    SELECT needle, source, start_pos, strpos(substr(source, start_pos), needle) AS suffix_position
    FROM inputs
)
SELECT strpos(source, needle) AS strpos_basic,
       POSITION(needle IN source) AS syntax_position,
       CASE WHEN suffix_position = 0 THEN 0 ELSE start_pos + suffix_position - 1 END AS position_from_start
FROM suffix_matches
ORDER BY needle;

-- query 2
SELECT strpos('aéé', 'é', 2) AS second_from_left,
       strpos('aéé', 'é', -1) AS first_from_right,
       POSITION('中' IN 'é中é') AS multibyte_position;

-- query 3
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT strpos('abc', 'b', 0);
