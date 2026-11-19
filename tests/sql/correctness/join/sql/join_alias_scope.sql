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
-- @tags=join,alias_scope,correlated_subquery
-- Test Objective:
-- Keep aliases as the sole visible relation name and preserve outer references
-- in self-correlated EXISTS and NOT EXISTS queries.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.alias_scope_rows;
CREATE TABLE ${case_db}.alias_scope_rows (c1 INT, c2 INT);
INSERT INTO ${case_db}.alias_scope_rows VALUES ROW(1, 1), ROW(2, 20), ROW(3, 30), ROW(4, 40);

-- query 2
SELECT CAST(count(*) AS integer) AS matched
FROM ${case_db}.alias_scope_rows
WHERE EXISTS (
  SELECT 1 FROM ${case_db}.alias_scope_rows inner_t
  WHERE alias_scope_rows.c1 = inner_t.c1 AND inner_t.c2 < 10
);

-- query 3
SELECT CAST(count(*) AS integer) AS matched
FROM ${case_db}.alias_scope_rows outer_t
WHERE EXISTS (
  SELECT 1 FROM ${case_db}.alias_scope_rows inner_t
  WHERE outer_t.c1 = inner_t.c1 AND inner_t.c2 < 10
);

-- query 4
SELECT CAST(count(*) AS integer) AS unmatched
FROM ${case_db}.alias_scope_rows
WHERE NOT EXISTS (
  SELECT 1 FROM ${case_db}.alias_scope_rows inner_t
  WHERE alias_scope_rows.c1 = inner_t.c1 AND inner_t.c2 < 10
);

-- query 5
SELECT CAST(count(*) AS integer) AS unmatched
FROM ${case_db}.alias_scope_rows outer_t
WHERE NOT EXISTS (
  SELECT 1 FROM ${case_db}.alias_scope_rows inner_t
  WHERE outer_t.c1 = inner_t.c1 AND inner_t.c2 < 10
);

-- query 6
SELECT CAST(count(*) AS integer) AS matched
FROM ${case_db}.alias_scope_rows renamed
WHERE renamed.c1 = 1;

-- query 7
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT CAST(count(*) AS integer)
FROM ${case_db}.alias_scope_rows renamed
WHERE alias_scope_rows.c1 = 1;

-- query 8
-- @expect_trino_error=COLUMN_NOT_FOUND,USER_ERROR
SELECT CAST(count(*) AS integer)
FROM ${case_db}.alias_scope_rows renamed
WHERE ${case_db}.alias_scope_rows.c1 = 1;

-- query 9
-- @skip_result_check=true
DROP TABLE ${case_db}.alias_scope_rows;
