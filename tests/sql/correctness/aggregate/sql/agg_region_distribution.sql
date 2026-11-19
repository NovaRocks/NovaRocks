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
-- @tags=aggregate,group_by,self_contained
-- Test Objective:
-- 1. Validate GROUP BY region distribution counting behavior.
-- 2. Prevent regressions where this case assumes pre-existing SSB customer data.
-- Test Flow:
-- 1. Create/reset a minimal customer-like table.
-- 2. Insert deterministic rows spanning all expected regions.
-- 3. Aggregate by region and order output for stable comparison.
CREATE TABLE ${case_db}.t_agg_region_distribution (
    c_custkey INT,
    c_region VARCHAR(32)
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t_agg_region_distribution VALUES
    (1, 'AFRICA'),
    (2, 'ASIA'),
    (3, 'ASIA'),
    (4, 'EUROPE'),
    (5, 'AMERICA'),
    (6, 'MIDDLE EAST'),
    (7, 'AMERICA'),
    (8, 'AFRICA');

SELECT c_region, COUNT() AS customer_count
FROM ${case_db}.t_agg_region_distribution
GROUP BY c_region
ORDER BY c_region;
