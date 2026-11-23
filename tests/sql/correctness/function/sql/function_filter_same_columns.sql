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
-- 1. Validate that filtering with least(col), greatest(col), positive(col), CAST(col AS TIMESTAMP(6)) on
--    the same column works correctly when the column appears in both IS NOT NULL and comparison predicates.
-- 2. Validate correct NULL handling when the column has interleaved NULL values.
-- 3. Cover least(), greatest(), positive(), and explicit TIMESTAMP casts as filter expressions.

-- query 1
WITH input AS (SELECT if(id%2=0, id, null) AS col FROM TABLE(sequence(1, 10)) AS tmp(id) limit 10) SELECT col FROM input WHERE least(col) IS NOT NULL AND least(col) < 20;

-- query 2
WITH input AS (SELECT if(id%2=0, id, null) AS col FROM TABLE(sequence(1, 10)) AS tmp(id) limit 10) SELECT col FROM input WHERE greatest(col) IS NOT NULL AND greatest(col) < 20;

-- query 3
WITH input AS (SELECT if(id%2=0, id, null) AS col FROM TABLE(sequence(1, 10)) AS tmp(id) limit 10) SELECT col FROM input WHERE positive(col) IS NOT NULL AND positive(col) < 20;

-- query 4
WITH input AS (
    SELECT IF(id % 2 <> 0, CAST(date_add('month', 1, DATE '2020-01-01') AS TIMESTAMP(6)), NULL) AS col
    FROM TABLE(sequence(1, 10)) AS tmp(id)
)
SELECT col
FROM input
WHERE CAST(col AS TIMESTAMP(6)) IS NOT NULL
  AND CAST(col AS TIMESTAMP(6)) > TIMESTAMP '2020-01-01 00:00:00.000000';
