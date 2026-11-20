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

-- Migrated from dev/test/sql/test_function/T/test_cbrt
-- Test Objective:
-- 1. Validate cbrt() returns the cube root for positive, negative, and zero values.
-- 2. Validate cbrt() with non-integer floating-point inputs.
-- 3. Validate cbrt(null) returns NULL.

-- query 1
select cbrt(27);

-- query 2
select cbrt(0.0);

-- query 3
select cbrt(-27);

-- query 4
select cbrt(3.1415);

-- query 5
SELECT cbrt((- (DECIMAL '3.1415')));

-- query 6
select cbrt(null);
