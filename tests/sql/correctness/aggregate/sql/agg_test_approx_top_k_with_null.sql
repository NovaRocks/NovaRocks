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

-- Migrated from dev/test/sql/test_agg_function/R/test_approx_top_k_with_null
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_approx_top_k_with_null
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE __row_util_base (
  k1 bigint
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into __row_util_base select generate_series from TABLE(generate_series(0, 1000 - 1));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into __row_util_base select * from __row_util_base; -- 2000
insert into __row_util_base select * from __row_util_base; -- 4000
insert into __row_util_base select * from __row_util_base; -- 8000
insert into __row_util_base select * from __row_util_base; -- 16000
CREATE TABLE __row_util (
  idx bigint
) WITH (format_version = 3);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into __row_util select row_number() over() as idx from __row_util_base;

-- query 6
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
  k1 bigint ,
  c1 bigint ,
  c2 int
) WITH (format_version = 3);

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into t1 select idx, idx % 10, idx % 2 from __row_util;

-- query 8
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x from t1) -- non-group-by.
select array_sortby((x) -> x.item, x) from w1;

-- query 9
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x, c1 from t1 group by c1) -- group by.
select c1, array_sortby((x) -> x.item, x) from w1 order by c1;

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into t1 select idx, idx % 10, null from __row_util order by idx limit 1000;

-- query 11
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x from t1) -- non-group-by.
select array_sortby((x) -> x.item, x) from w1;

-- query 12
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x, c1 from t1 group by c1) -- group by.
select c1, array_sortby((x) -> x.item, x) from w1 order by c1;

-- query 13
-- @skip_result_check=true
USE ${case_db};
insert into t1 select idx, idx % 10, null from __row_util;

-- query 14
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x from t1) -- non-group-by.
select array_sortby((x) -> x.item, x) from w1;

-- query 15
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x, c1 from t1 group by c1) -- group by.
select c1, array_sortby((x) -> x.item, x) from w1 order by c1;

-- query 16
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (
  k1 bigint ,
  c1 bigint ,
  c2 int
) WITH (format_version = 3);

-- query 17
-- @skip_result_check=true
USE ${case_db};
insert into t2 select idx, null, null from __row_util;

-- query 18
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x from t2) -- non-group-by.
select array_sortby((x) -> x.item, x) from w1;

-- query 19
USE ${case_db};
with w1 as (select approx_top_k(c2, 3) as x, c1 from t2 group by c1) -- group by.
select c1, array_sortby((x) -> x.item, x) from w1 order by c1;
