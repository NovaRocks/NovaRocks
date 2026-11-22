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

-- Migrated from dev/test/sql/test_function/T/test_synopse
-- Test Objective:
-- 1. Validate bar() function renders a horizontal bar chart string for a value within a range.
-- 2. Validate equiwidth_bucket() assigns values into equal-width histogram buckets.
-- 3. Both functions tested over a generate_series range from 0 to 10.

-- query 1
select r, bar(r, 0, 10, 20) as x from table(sequence(0, 10)) as s(r);

-- query 2
select r, equiwidth_bucket(r, 0, 10, 20) as x from table(sequence(0, 10)) as s(r);
