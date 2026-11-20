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
-- @tags=aggregate,regression,self_contained
-- Test Objective:
-- 1. Validate mixed aggregate metrics over customer and lineorder-like data.
-- 2. Prevent regressions where this case depends on shared SSB fixture tables.
-- Test Flow:
-- 1. Create/reset minimal source tables for customer and lineorder metrics.
-- 2. Insert deterministic rows that include both filter-hit and filter-miss branches.
-- 3. Aggregate metrics and order by metric name for stable comparison.
CREATE TABLE ${case_db}.t_agg_regression_customer_metrics (
    c_custkey INT,
    c_region VARCHAR(32)
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_agg_regression_lineorder_metrics (
    lo_revenue BIGINT,
    lo_quantity INT,
    lo_discount INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_regression_customer_metrics VALUES
    ROW(1, 'ASIA'),
    ROW(2, 'AMERICA'),
    ROW(3, 'EUROPE'),
    ROW(4, 'AFRICA'),
    ROW(5, 'MIDDLE EAST'),
    ROW(6, 'ASIA');

INSERT INTO ${case_db}.t_agg_regression_lineorder_metrics VALUES
    ROW(100, 10, 1),
    ROW(200, 24, 3),
    ROW(50, 25, 2),
    ROW(400, 5, 4),
    ROW(300, 20, 2),
    ROW(10, 1, 0);

SELECT metric, value
FROM (
    SELECT 'customer_count' AS metric, CAST(COUNT(*) AS BIGINT) AS value
    FROM ${case_db}.t_agg_regression_customer_metrics
    UNION ALL
    SELECT 'lineorder_filtered_sum_revenue', CAST(SUM(lo_revenue) AS BIGINT)
    FROM ${case_db}.t_agg_regression_lineorder_metrics
    WHERE lo_quantity < 25
      AND lo_discount BETWEEN 1 AND 3
    UNION ALL
    SELECT 'lineorder_sum_qty', CAST(SUM(lo_quantity) AS BIGINT)
    FROM ${case_db}.t_agg_regression_lineorder_metrics
) t
ORDER BY metric;
