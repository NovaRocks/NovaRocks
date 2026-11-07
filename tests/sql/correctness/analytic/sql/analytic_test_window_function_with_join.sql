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

-- Migrated from: dev/test/sql/test_window_function/T/test_window_function_with_join
-- Test Objective:
-- 1. Validate max() window function with CTEs across legacy bracketed join hints.
-- 2. Confirm result consistency for hint parsing without asserting distribution or placement semantics.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.`nt0` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.nt0 SELECT generate_series %4096, 4096 - generate_series, generate_series %4096 FROM TABLE(generate_series(1, 8192));
INSERT INTO ${case_db}.nt0 SELECT * FROM ${case_db}.nt0;

-- This analytic case runs on the suite's Hadoop Iceberg catalog, which does
-- not advertise atomic staged publication for CTAS. Keep CTAS coverage in the
-- dedicated Iceberg DML suite and build these fixtures through supported DDL
-- and INSERT paths.
CREATE TABLE ${case_db}.`nt1` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.`nt2` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.nt1 SELECT * FROM ${case_db}.nt0;
INSERT INTO ${case_db}.nt2 SELECT * FROM ${case_db}.nt0;

CREATE TABLE ${case_db}.`nt3` (
  `c0` bigint DEFAULT NULL,
  `c1` bigint DEFAULT NULL,
  `c2` bigint DEFAULT NULL
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.nt3 SELECT * FROM ${case_db}.nt0;

-- query 2
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN  ${case_db}.nt1 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;

-- query 3
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN  ${case_db}.nt1 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;

-- query 4
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN  ${case_db}.nt1 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;

-- query 5
WITH cte0 AS (
    SELECT max(c1) OVER (PARTITION BY c0 ORDER BY c2) mx, c0, c1, c2 FROM ${case_db}.nt0
),
cte1 AS (
    SELECT l.mx g1, l.c0 g2, l.c1 g3, l.c2 g4, max(r.c1) OVER (PARTITION BY l.c0 ORDER BY l.c2) g5, r.c0 g6, r.c1 g7, r.c2 g8 FROM cte0 l JOIN  ${case_db}.nt3 r ON l.c0 = r.c0
)
SELECT sum(g1), sum(g2), sum(g3), sum(g4), sum(g5), sum(g6), sum(g7), sum(g8) FROM cte1;
