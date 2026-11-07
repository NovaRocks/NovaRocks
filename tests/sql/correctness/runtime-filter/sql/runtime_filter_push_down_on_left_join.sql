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

-- Test Objective:
-- 1. Validate that runtime filter is NOT incorrectly pushed through LEFT JOIN
--    onto columns produced by COALESCE on the left side.
-- 2. The outer join produces c2 via coalesce(t2.c2, 'unknown')='c2-1',
--    but t3 has c2='unknown', so the final join should return 0 rows.
-- 3. Cover both broadcast and shuffle variants of the left join.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
  c1 STRING,
  c2 STRING,
  c3 STRING
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.t2 (
  c1 STRING,
  c2 STRING,
  c3 STRING
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE ${case_db}.t3 (
  c1 STRING,
  c2 STRING,
  c3 STRING
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t3 SELECT 'c1-1', 'unknown', 'c3';
INSERT INTO ${case_db}.t2 SELECT 'c1-1', 'c2-1', 'c3';
INSERT INTO ${case_db}.t1 SELECT 'c1-1', 'c2-1', 'c3';

-- query 2
-- Left join [broadcast]: coalesce output c2='c2-1' must NOT be incorrectly used
-- to push RF into t1 scan, because the final join to t3 requires c2='unknown'.
-- Expected: 0 rows (coalesce result 'c2-1' != t3.c2 'unknown').
with
  w2 as (
    select
      t1.c1,
      coalesce(t2.c2, 'unknown') as c2
    from ${case_db}.t1 left join  ${case_db}.t2 on t1.c3 = t2.c3
  )
select
  w2.*
from
  w2
  join  ${case_db}.t3 on w2.c1 = t3.c1 and w2.c2 = t3.c2;

-- query 3
-- Shuffle variant: same semantics, same expected empty result.
with
  w2 as (
    select
      t1.c1,
      coalesce(t2.c2, 'unknown') as c2
    from ${case_db}.t1 left join  ${case_db}.t2 on t1.c3 = t2.c3
  )
select
  w2.*
from
  w2
  join  ${case_db}.t3 on w2.c1 = t3.c1 and w2.c2 = t3.c2;
