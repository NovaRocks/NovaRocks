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
-- @tags=filter,metrics,self_contained
-- Test Objective:
-- 1. Validate filter metric counting on customer/date predicates.
-- 2. Prevent regressions where this case depends on shared SSB fixture state.
-- Test Flow:
-- 1. Create/reset minimal customer and dates tables.
-- 2. Insert deterministic rows covering IN, equality, and year filters.
-- 3. Compute metric counts and order by metric key for stable output.
DROP TABLE IF EXISTS ${case_db}.t_filter_customer_metrics;
DROP TABLE IF EXISTS ${case_db}.t_filter_dates_metrics;
CREATE TABLE ${case_db}.t_filter_customer_metrics (
    c_custkey INT,
    c_region VARCHAR(32)
)
TBLPROPERTIES ("format-version" = "3");
CREATE TABLE ${case_db}.t_filter_dates_metrics (
    d_datekey INT,
    d_year INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_filter_customer_metrics VALUES
    (1, 'ASIA'),
    (2, 'ASIA'),
    (3, 'AMERICA'),
    (4, 'EUROPE'),
    (5, 'AMERICA'),
    (6, 'AFRICA');

INSERT INTO ${case_db}.t_filter_dates_metrics VALUES
    (19920101, 1992),
    (19930101, 1993),
    (19931231, 1993),
    (19940101, 1994);

SELECT metric, value
FROM (
    SELECT 'asia_america_customers' AS metric, COUNT() AS value
    FROM ${case_db}.t_filter_customer_metrics
    WHERE c_region IN ('ASIA', 'AMERICA')
    UNION ALL
    SELECT 'asia_customers', COUNT()
    FROM ${case_db}.t_filter_customer_metrics
    WHERE c_region = 'ASIA'
    UNION ALL
    SELECT 'datekey_19920101', COUNT()
    FROM ${case_db}.t_filter_dates_metrics
    WHERE d_datekey = 19920101
    UNION ALL
    SELECT 'dates_1993', COUNT()
    FROM ${case_db}.t_filter_dates_metrics
    WHERE d_year = 1993
) t
ORDER BY metric;
