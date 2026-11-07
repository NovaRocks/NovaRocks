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

-- Migrated from dev/test/sql/test_cast/T/test_cast_to_datetime
-- Test Objective:
-- 1. Validate that CAST of out-of-range integers as DATETIME returns NULL.
-- 2. Covers overflow, zero, one, max uint64, and yearweek() with overflow.
-- 3. These values cannot represent valid DATETIME and must return NULL.

-- query 1
select cast(CAST('-18446744073709551494' AS LARGEINT) AS DATETIME);

-- query 2
select cast(0 AS DATETIME);

-- query 3
select cast(1 AS DATETIME);

-- query 4
select cast(CAST('18446744073709551615' AS LARGEINT) AS DATETIME);

-- query 5
select yearweek(CAST('-18446744073709551494' AS LARGEINT));
