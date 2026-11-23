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
SELECT approx_distinct(x) AS cardinality
FROM (VALUES NULL, NULL) t(x);

-- query 2
SELECT approx_distinct(x) AS distinct_boolean,
       approx_distinct(x) FILTER (WHERE x) AS distinct_true
FROM (VALUES true, true, false, CAST(NULL AS BOOLEAN)) t(x);

-- query 3
SELECT approx_distinct(DISTINCT x) AS cardinality
FROM (VALUES BIGINT '1', BIGINT '2', BIGINT '2', CAST(NULL AS BIGINT)) t(x);

-- query 4
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT approx_distinct(BIGINT '1', DOUBLE '0.023');

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT approx_distinct(true, DOUBLE '0.023');

-- query 6
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT approx_distinct(NULL, DOUBLE '0.023');

-- query 7
WITH domain AS (
    SELECT g, g * BIGINT '8192' + key AS x
    FROM TABLE(sequence(0, 31)) AS base(g)
    CROSS JOIN TABLE(sequence(1, 8192)) AS span(key)
), estimates AS (
    SELECT g, count(DISTINCT x) AS exact, approx_distinct(x) AS estimate
    FROM domain GROUP BY g
), errors AS (
    SELECT exact, CAST(estimate - exact AS DOUBLE) / CAST(exact AS DOUBLE) AS relative_error
    FROM estimates
)
SELECT count(*) AS groups,
       min(exact) AS smallest_exact,
       max(exact) AS largest_exact,
       sqrt(avg(relative_error * relative_error)) <= DOUBLE '0.046' AS within_rmse
FROM errors;
