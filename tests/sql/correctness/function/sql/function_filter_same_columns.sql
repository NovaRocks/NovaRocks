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

-- Migrated from dev/test/sql/test_function/T/test_filter_same_columns
-- Test Objective:
-- 1. Validate that filtering with least(col), greatest(col), positive(col), timestamp(col) on
--    the same column works correctly when the column appears in both IS NOT NULL and comparison predicates.
-- 2. Validate correct NULL handling when the column has interleaved NULL values.
-- 3. Cover least(), greatest(), positive(), and timestamp() as filter expressions.

-- query 1
WITH input AS (SELECT if(id%2=0, id, null) AS col FROM TABLE(sequence(1, 10)) AS tmp(id) limit 10) SELECT col FROM input WHERE least(col) IS NOT NULL AND least(col) < 20;

-- query 2
WITH input AS (SELECT if(id%2=0, id, null) AS col FROM TABLE(sequence(1, 10)) AS tmp(id) limit 10) SELECT col FROM input WHERE greatest(col) IS NOT NULL AND greatest(col) < 20;

-- query 3
WITH input AS (SELECT if(id%2=0, id, null) AS col FROM TABLE(sequence(1, 10)) AS tmp(id) limit 10) SELECT col FROM input WHERE positive(col) IS NOT NULL AND positive(col) < 20;

-- query 4
with input as (select date'2020-01-01' as dt, if(id%2, add_months(dt, 1), null) as col from table(sequence(1, 10)) as tmp(id)) select col from input where timestamp(col) is not null and timestamp(col) > '2020-01-01';
