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

-- Migrated from dev/test/sql/test_cast/T/test_cast_string_to_int
-- Test Objective:
-- 1. Validate that CAST('+' or '-' as integer types) returns NULL.
-- 2. Covers tinyint, smallint, int, bigint, largeint.
-- 3. These strings cannot be parsed as integers and must return NULL, not an error.

-- query 1
select cast('-' as tinyint);

-- query 2
select cast('-' as smallint);

-- query 3
select cast('-' as int);

-- query 4
select cast('-' as bigint);

-- query 5
select CAST(cast('-' as largeint) AS VARCHAR) AS `CAST('-' AS LARGEINT)`;

-- query 6
select cast('+' as tinyint);

-- query 7
select cast('+' as smallint);

-- query 8
select cast('+' as int);

-- query 9
select cast('+' as bigint);

-- query 10
select CAST(cast('+' as largeint) AS VARCHAR) AS `CAST('+' AS LARGEINT)`;
