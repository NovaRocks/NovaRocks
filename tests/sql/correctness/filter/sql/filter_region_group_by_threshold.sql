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
-- @tags=filter,group_by,self_contained
-- Test Objective:
-- 1. Validate filtered GROUP BY counting with a numeric threshold predicate.
-- 2. Prevent regressions where this case assumes SSB customer table availability.
-- Test Flow:
-- 1. Create/reset a minimal customer-like table.
-- 2. Insert deterministic key/region rows around the threshold.
-- 3. Aggregate filtered rows by region and order result deterministically.
DROP TABLE IF EXISTS ${case_db}.t_filter_group_by_customer;
CREATE TABLE ${case_db}.t_filter_group_by_customer (
    c_custkey INT,
    c_region VARCHAR(32)
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_filter_group_by_customer VALUES
    ROW(10000, 'AFRICA'),
    ROW(15001, 'AFRICA'),
    ROW(15002, 'AMERICA'),
    ROW(15003, 'AMERICA'),
    ROW(14999, 'ASIA'),
    ROW(17000, 'ASIA'),
    ROW(18000, 'EUROPE'),
    ROW(13000, 'MIDDLE EAST'),
    ROW(19000, 'MIDDLE EAST');

SELECT c_region, COUNT(*) AS customer_count
FROM ${case_db}.t_filter_group_by_customer
WHERE c_custkey > 15000
GROUP BY c_region
ORDER BY c_region;
