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
-- @tags=project,projection,limit,self_contained
-- Test Objective:
-- 1. Validate projection and LIMIT row-count semantics on customer/date-like tables.
-- 2. Prevent regressions where this case depends on SSB dataset preloading.
-- Test Flow:
-- 1. Create/reset minimal customer and dates source tables.
-- 2. Insert deterministic row counts larger/smaller than LIMIT thresholds.
-- 3. Assert projected LIMIT counts through scalar subqueries.
DROP TABLE IF EXISTS ${case_db}.t_project_projection_customer;
DROP TABLE IF EXISTS ${case_db}.t_project_projection_dates;
CREATE TABLE ${case_db}.t_project_projection_customer (
    c_custkey INT,
    c_name VARCHAR(32)
) WITH (format_version = 3);
CREATE TABLE ${case_db}.t_project_projection_dates (
    d_datekey INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_project_projection_customer VALUES
    ROW(1, 'c1'),
    ROW(2, 'c2'),
    ROW(3, 'c3'),
    ROW(4, 'c4'),
    ROW(5, 'c5'),
    ROW(6, 'c6'),
    ROW(7, 'c7'),
    ROW(8, 'c8'),
    ROW(9, 'c9'),
    ROW(10, 'c10'),
    ROW(11, 'c11'),
    ROW(12, 'c12');

INSERT INTO ${case_db}.t_project_projection_dates VALUES
    ROW(19920101),
    ROW(19920102),
    ROW(19920103),
    ROW(19920104),
    ROW(19920105),
    ROW(19920106),
    ROW(19920107),
    ROW(19920108);

SELECT
    (
        SELECT COUNT(*)
        FROM (
            SELECT c_custkey, c_name
            FROM ${case_db}.t_project_projection_customer
            LIMIT 100
        ) x
    ) AS projected_limit_rows,
    (
        SELECT COUNT(*)
        FROM (
            SELECT C_CUSTKEY
            FROM ${case_db}.t_project_projection_customer
            LIMIT 10
        ) y
    ) AS uppercase_projection_rows,
    (
        SELECT COUNT(*)
        FROM (
            SELECT *
            FROM ${case_db}.t_project_projection_dates
            LIMIT 10
        ) z
    ) AS dates_limit_rows;
