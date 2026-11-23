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
-- @tags=aggregate,percentile,weighted
-- Test Objective:
-- 1. Validate percentile_approx and percentile_approx_weighted.
-- 2. Preserve legacy percentile error-path coverage for invalid percentile and compression arguments.
-- Test Flow:
-- 1. Create/reset a percentile source table with numeric, date, and datetime inputs.
-- 2. Insert deterministic ordered rows plus one NULL row.
-- 3. Snapshot percentile outputs and assert representative invalid-argument errors.
-- query 1
CREATE TABLE ${case_db}.t_agg_percentile_semantics (
    id INT,
    v INT,
    w INT,
    d DATE,
    dt TIMESTAMP(6),
    dbl DOUBLE
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_percentile_semantics VALUES
    ROW(1, 10, 1, '2018-01-01', '2018-01-01 00:00:01', 11.1),
    ROW(2, 20, 1, '2019-01-01', '2019-01-01 00:00:01', 11.2),
    ROW(3, 30, 2, '2020-01-01', '2020-01-01 00:00:01', 11.3),
    ROW(4, 40, 1, '2021-01-01', '2021-01-01 00:00:01', 11.4),
    ROW(5, 50, 3, '2022-01-01', '2022-01-01 00:00:01', 11.5),
    ROW(6, NULL, NULL, NULL, NULL, NULL);
SELECT
    CAST(percentile_approx(v, 0.5) AS INT) AS approx50,
    CAST(percentile_approx(v, 0.9, 2048) AS INT) AS approx90,
    percentile_approx(v, CAST([0.25, 0.5, 0.75] AS array<double>), 2048) AS approx_quartiles
FROM ${case_db}.t_agg_percentile_semantics;

-- query 2
SELECT
    CAST(percentile_approx_weighted(v, w, 0.5, 2048) AS INT) AS weighted50,
    CAST(percentile_approx_weighted(v, w, 0.9, 2048) AS INT) AS weighted90,
    percentile_approx_weighted(v, w, CAST([0.25, 0.5, 0.75] AS array<double>), 2048) AS weighted_quartiles
FROM ${case_db}.t_agg_percentile_semantics;

-- query 3
-- @expect_error=percentile parameter must be between 0 and 1
SELECT percentile_approx_weighted(v, w, 1.5)
FROM ${case_db}.t_agg_percentile_semantics;

-- query 4
-- @expect_error=compression parameter must be positive
SELECT percentile_approx_weighted(v, w, 0.5, 0)
FROM ${case_db}.t_agg_percentile_semantics;
