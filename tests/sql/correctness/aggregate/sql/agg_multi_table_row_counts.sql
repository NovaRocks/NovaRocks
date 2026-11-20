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
-- @tags=aggregate,row_count,self_contained
-- Test Objective:
-- 1. Validate row-count aggregation across multiple base tables.
-- 2. Prevent regressions where this case depends on SSB schema presence.
-- Test Flow:
-- 1. Create/reset five minimal source tables.
-- 2. Insert deterministic row counts per table.
-- 3. Union all COUNT(*) metrics and compare ordered output.
CREATE TABLE ${case_db}.t_agg_count_customer (id INT) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_agg_count_dates (id INT) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_agg_count_lineorder (id INT) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_agg_count_part (id INT) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_agg_count_supplier (id INT) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_count_customer VALUES
    ROW(1),
    ROW(2),
    ROW(3),
    ROW(4);
INSERT INTO ${case_db}.t_agg_count_dates VALUES
    ROW(1),
    ROW(2),
    ROW(3);
INSERT INTO ${case_db}.t_agg_count_lineorder VALUES
    ROW(1),
    ROW(2),
    ROW(3),
    ROW(4),
    ROW(5);
INSERT INTO ${case_db}.t_agg_count_part VALUES
    ROW(1),
    ROW(2);
INSERT INTO ${case_db}.t_agg_count_supplier VALUES
    ROW(1),
    ROW(2),
    ROW(3);

SELECT table_name, row_count
FROM (
    SELECT 'customer' AS table_name, COUNT(*) AS row_count FROM ${case_db}.t_agg_count_customer
    UNION ALL
    SELECT 'dates', COUNT(*) FROM ${case_db}.t_agg_count_dates
    UNION ALL
    SELECT 'lineorder', COUNT(*) FROM ${case_db}.t_agg_count_lineorder
    UNION ALL
    SELECT 'part', COUNT(*) FROM ${case_db}.t_agg_count_part
    UNION ALL
    SELECT 'supplier', COUNT(*) FROM ${case_db}.t_agg_count_supplier
) t
ORDER BY table_name;
