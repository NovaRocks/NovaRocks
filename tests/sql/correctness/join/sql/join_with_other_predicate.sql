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

-- @tags=join,other_predicate,like
-- Test Objective:
-- 1. Validate INNER JOIN with LIKE-based other (residual) predicates.
-- 2. Prevent regressions where LIKE with concat patterns on join output fails or returns wrong counts.
-- Test Flow:
-- 1. Create two tables and insert 40960 rows plus one all-NULL row.
-- 2. Execute joins with various LIKE patterns (exact, prefix, contains, suffix) as residual predicates.
-- 3. Assert count is 40960 for each (NULL row is excluded by the equi-join).

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t0 (
  "c0" INTEGER,
  "c1" varchar(20),
  "c2" varchar(200),
  "c3" INTEGER
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  "c0" INTEGER,
  "c1" varchar(20),
  "c2" varchar(200),
  "c3" INTEGER
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 SELECT generate_series, CAST(generate_series AS VARCHAR), CAST(generate_series AS VARCHAR), generate_series FROM TABLE(sequence(1, 40960)) AS generate_series(generate_series);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t0 VALUES ROW(null, null, null, null);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t1 SELECT * FROM ${case_db}.t0;

-- query 6
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE ${case_db}.t1.c1;

-- query 7
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE concat(${case_db}.t1.c1, '%');

-- query 8
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE concat('%', ${case_db}.t1.c1, '%');

-- query 9
SELECT count(*) FROM ${case_db}.t0 JOIN ${case_db}.t1 ON ${case_db}.t0.c0=${case_db}.t1.c0 WHERE ${case_db}.t0.c1 LIKE concat('%', ${case_db}.t1.c1);
