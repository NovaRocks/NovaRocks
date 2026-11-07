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
-- 1. Validate runtime bitset filter behavior when probe-side has OR predicates.
-- 2. w1 with non-matching OR predicate should return 0 rows in join.
-- 3. w1 with matching OR predicate should return 1 row in join.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.row_util_base (
  k1 BIGINT NULL
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.row_util_base SELECT generate_series FROM TABLE(generate_series(0, 16 - 1));

CREATE TABLE ${case_db}.row_util (
  idx BIGINT NULL
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.row_util SELECT row_number() over() AS idx FROM ${case_db}.row_util_base;

CREATE TABLE ${case_db}.t1 (
  k1 BIGINT NULL,
  c_str_1 STRING NULL,
  c_str_2 STRING NULL
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.t1
SELECT
    idx,
    concat('str-abc-', idx),
    concat('str-abc-', idx)
FROM ${case_db}.row_util;

-- query 2
-- OR predicate matches nothing: count = 0
with w1 as (
  select * from ${case_db}.t1 where k1 = 1 and (c_str_1 like 'non-exist%' or c_str_2 like 'non-exist%')
), w2 as (
  select * from ${case_db}.t1 where k1 = 1
)
select count(1) from w1 join  w2 on w1.k1 = w2.k1;

-- query 3
-- OR predicate matches the row: count = 1
with w1 as (
  select * from ${case_db}.t1 where k1 = 1 and (c_str_1 like 'str-abc%' or c_str_2 like 'str-abc%')
), w2 as (
  select * from ${case_db}.t1 where k1 = 1
)
select count(1) from w1 join  w2 on w1.k1 = w2.k1;
