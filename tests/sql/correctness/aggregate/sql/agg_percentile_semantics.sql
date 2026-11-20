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
-- @tags=aggregate,percentile,weighted,lc
-- Test Objective:
-- 1. Validate percentile_cont, percentile_disc_lc, percentile_approx, percentile_approx_weighted, and percentile_union.
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
    dt DATETIME,
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
    percentile_cont(v, 0) AS p0,
    percentile_cont(v, 0.5) AS p50,
    percentile_cont(v, 1) AS p100,
    percentile_disc_lc(v, 0.5) AS disc50,
    percentile_disc_lc(v, 0.75) AS disc75
FROM ${case_db}.t_agg_percentile_semantics;

-- query 2
SELECT
    percentile_cont(d, 0.5) AS date_p50,
    percentile_cont(dt, 0.5) AS dt_p50,
    percentile_disc_lc(d, 0.75) AS date_disc75
FROM ${case_db}.t_agg_percentile_semantics;

-- query 3
SELECT
    CAST(percentile_approx(v, 0.5) AS INT) AS approx50,
    CAST(percentile_approx(v, 0.9, 2048) AS INT) AS approx90,
    percentile_approx(v, CAST([0.25, 0.5, 0.75] AS array<double>), 2048) AS approx_quartiles
FROM ${case_db}.t_agg_percentile_semantics;

-- query 4
SELECT
    CAST(percentile_approx_weighted(v, w, 0.5, 2048) AS INT) AS weighted50,
    CAST(percentile_approx_weighted(v, w, 0.9, 2048) AS INT) AS weighted90,
    percentile_approx_weighted(v, w, CAST([0.25, 0.5, 0.75] AS array<double>), 2048) AS weighted_quartiles
FROM ${case_db}.t_agg_percentile_semantics;

-- query 5
SELECT CAST(percentile_approx_raw(percentile_union(percentile_hash(v)), 0.75) AS INT) AS union_p75
FROM ${case_db}.t_agg_percentile_semantics;

-- query 6
-- @expect_error=percentile_cont second parameter'value should be between 0 and 1
SELECT percentile_cont(dbl, 2)
FROM ${case_db}.t_agg_percentile_semantics;

-- query 7
-- @expect_error=percentile_disc_lc second parameter'value should be between 0 and 1
SELECT percentile_disc_lc(v, 1.1)
FROM ${case_db}.t_agg_percentile_semantics;

-- query 8
-- @expect_error=percentile parameter must be between 0 and 1
SELECT percentile_approx_weighted(v, w, 1.5)
FROM ${case_db}.t_agg_percentile_semantics;

-- query 9
-- @expect_error=compression parameter must be positive
SELECT percentile_approx_weighted(v, w, 0.5, 0)
FROM ${case_db}.t_agg_percentile_semantics;
