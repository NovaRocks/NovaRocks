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
-- @tags=set_op,union_distinct,float,nan
-- Test Objective:
-- 1. Validate UNION DISTINCT deduplicates FLOAT/DOUBLE NaN values.
-- 2. Prevent hash-key equality regressions where NaN rows are treated as distinct.
-- Test Flow:
-- 1. Build two identical NaN rows via constant SELECTs.
-- 2. Apply UNION DISTINCT.
-- 3. Assert only one distinct row remains.
SELECT COUNT() AS distinct_nan_count
FROM (
    SELECT CAST('NaN' AS DOUBLE) AS v
    UNION
    SELECT CAST('NaN' AS DOUBLE) AS v
) t;
