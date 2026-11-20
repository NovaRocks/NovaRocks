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
-- @tags=set_op,scan,float

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.set_float_build (k DOUBLE);

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.set_float_probe_a (k DOUBLE);

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.set_float_probe_b (k DOUBLE);

-- query 4
-- @skip_result_check=true
-- Zero times floating-point infinity yields NaN. A text cast yields NULL.
INSERT INTO ${case_db}.set_float_build VALUES
    ROW(NULL), ROW(0e0 * 1e999), ROW(0e0 * 1e999),
    ROW(0e0), ROW(-0e0), ROW(2e0), ROW(3e0), ROW(5e0);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.set_float_probe_a VALUES
    ROW(NULL), ROW(NULL), ROW(0e0 * 1e999), ROW(-0e0), ROW(2e0), ROW(8e0);

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.set_float_probe_b VALUES
    ROW(NULL), ROW(0e0 * 1e999), ROW(0e0), ROW(5e0), ROW(9e0);

-- query 7
SELECT CASE WHEN k = 0e0 THEN 0e0 ELSE k END AS common_key
FROM (
    SELECT k FROM ${case_db}.set_float_build
    INTERSECT SELECT k FROM ${case_db}.set_float_probe_a
    INTERSECT SELECT k FROM ${case_db}.set_float_probe_b
) s
ORDER BY common_key NULLS FIRST;

-- query 8
SELECT CAST(k AS INTEGER) AS unmatched_key
FROM (
    SELECT k FROM ${case_db}.set_float_build
    EXCEPT SELECT k FROM ${case_db}.set_float_probe_a
    EXCEPT SELECT k FROM ${case_db}.set_float_probe_b
) s
ORDER BY unmatched_key;
