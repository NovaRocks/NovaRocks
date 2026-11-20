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
-- @tags=aggregate,min_max,self_contained
-- Test Objective:
-- 1. Validate MIN/MAX aggregation on lineorder-like numeric columns.
-- 2. Prevent regressions where this case relies on external SSB base tables.
-- Test Flow:
-- 1. Create/reset a minimal lineorder-like table.
-- 2. Insert deterministic quantity/discount rows with explicit boundaries.
-- 3. Compute MIN/MAX and compare a single deterministic row.
CREATE TABLE ${case_db}.t_agg_quantity_discount_ranges (
    lo_quantity INT,
    lo_discount INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_quantity_discount_ranges VALUES
    ROW(1, 0),
    ROW(50, 10),
    ROW(20, 3),
    ROW(7, 5),
    ROW(42, 2);

SELECT
    MIN(lo_quantity) AS min_qty,
    MAX(lo_quantity) AS max_qty,
    MIN(lo_discount) AS min_discount,
    MAX(lo_discount) AS max_discount
FROM ${case_db}.t_agg_quantity_discount_ranges;
