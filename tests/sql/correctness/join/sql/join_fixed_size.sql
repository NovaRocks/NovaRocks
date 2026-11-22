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

-- @tags=join,fixed_size
-- Test Objective:
-- 1. Validate hash join correctness on fixed-size type keys (bool, tinyint, smallint, int, bigint, float, double, date, datetime).
-- 2. Verify both equality (=) and null-safe equality (<=>) join semantics on nullable columns.
-- 3. Test multi-key joins combining fixed-size types of different widths.
-- 4. Test full outer join with null-safe equality keys.
-- Test Flow:
-- 1. Create helper tables to generate 40K rows with row_number indexing.
-- 2. Create t1 with various fixed-size nullable and non-nullable columns.
-- 3. Self-join t1 on different column combinations and verify aggregated counts.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util_base (
  k1 bigint
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select generate_series from TABLE(sequence(0, 10000 - 1)) AS generate_series(generate_series);

-- query 3
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 4
-- @skip_result_check=true
insert into ${case_db}.__row_util_base select * from ${case_db}.__row_util_base;

-- query 5
-- @skip_result_check=true
CREATE TABLE ${case_db}.__row_util (
  idx bigint
) WITH (format_version = 3);

-- query 6
-- @skip_result_check=true
insert into ${case_db}.__row_util select row_number() over() as idx from ${case_db}.__row_util_base;

-- query 7
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (
    k1 bigint ,
    c_bool boolean,
    c_bool_null boolean ,
    c_tinyint tinyint,
    c_tinyint_null tinyint ,
    c_smallint smallint,
    c_smallint_null smallint ,
    c_int int,
    c_int_null int ,
    c_bigint bigint,
    c_bigint_null bigint ,
    c_float float,
    c_float_null float ,
    c_double double,
    c_double_null double ,
    c_date date,
    c_date_null date ,
    c_datetime datetime,
    c_datetime_null datetime
) WITH (format_version = 3);

-- query 8
-- @skip_result_check=true
insert into ${case_db}.t1
select
    idx,
    idx % 2 = 0,
    if (idx % 7 = 0, idx % 2 = 0, null),
    idx % 128,
    if (idx % 12 = 0, idx % 128, null),
    idx % 32768,
    if (idx % 13 = 0, idx % 32768, null),
    idx % 2147483648,
    if (idx % 14 = 0, idx % 2147483648, null),
    idx,
    if (idx % 15 = 0, idx, null),
    idx,
    if (idx % 16 = 0, idx, null),
    idx,
    if (idx % 16 = 0, idx, null),
    date_add('2023-01-01', idx % 365),
    if (idx % 17 = 0, date_add('2023-01-01', idx % 365), null),
    date_add('2023-01-01 00:00:00', idx % 365 * 24 * 3600 + idx % 86400),
    if (idx % 18 = 0, date_add('2023-01-01 00:00:00', idx % 365 * 24 * 3600 + idx % 86400), null)
from ${case_db}.__row_util;

-- join on tinyint_null + smallint_null
-- query 9
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_tinyint_null = t2.c_tinyint_null and t1.c_smallint_null = t2.c_smallint_null;

-- join on bool_null + tinyint_null + smallint_null
-- query 10
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bool_null = t2.c_bool_null and t1.c_tinyint_null = t2.c_tinyint_null and t1.c_smallint_null = t2.c_smallint_null;

-- join on smallint_null with expression key
-- query 11
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_smallint_null = t2.c_smallint_null and t1.c_smallint_null + 1 = t2.c_smallint_null + 1;

-- join with <=> on bool_null + tinyint_null and = on smallint_null
-- query 12
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bool_null  IS NOT DISTINCT FROM  t2.c_bool_null and t1.c_tinyint_null  IS NOT DISTINCT FROM  t2.c_tinyint_null and t1.c_smallint_null = t2.c_smallint_null;

-- full join with <=> on bool_null + tinyint_null and = on smallint_null
-- query 13
select count(1)
from ${case_db}.t1 full join ${case_db}.t1 t2 on t1.c_bool_null  IS NOT DISTINCT FROM  t2.c_bool_null and t1.c_tinyint_null  IS NOT DISTINCT FROM  t2.c_tinyint_null and t1.c_smallint_null = t2.c_smallint_null;

-- join on bool_null + tinyint_null + int
-- query 14
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bool_null = t2.c_bool_null and t1.c_tinyint_null = t2.c_tinyint_null and t1.c_int = t2.c_int;

-- join on bool_null + tinyint_null + smallint_null + int
-- query 15
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bool_null = t2.c_bool_null and t1.c_tinyint_null = t2.c_tinyint_null and t1.c_smallint_null = t2.c_smallint_null and t1.c_int = t2.c_int;

-- join on all non-nullable fixed-size keys
-- query 16
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bool = t2.c_bool and t1.c_tinyint = t2.c_tinyint and t1.c_smallint = t2.c_smallint and t1.c_int = t2.c_int;

-- join on int_null + int
-- query 17
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_int_null = t2.c_int_null and t1.c_int = t2.c_int;

-- join on expression key (c_int + 1) and c_int
-- query 18
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2
  on CAST(t1.c_int AS BIGINT) + 1 = CAST(t2.c_int AS BIGINT) + 1
 and t1.c_int = t2.c_int;

-- join on mismatched expression key (c_int + 1 vs c_int + 2)
-- query 19
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2
  on CAST(t1.c_int AS BIGINT) + 1 = CAST(t2.c_int AS BIGINT) + 2
 and t1.c_int = t2.c_int;

-- join on float + int
-- query 20
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_float = t2.c_float and t1.c_int = t2.c_int;

-- join on date + int
-- query 21
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_date = t2.c_date and t1.c_int = t2.c_int;

-- join on datetime + int
-- query 22
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_datetime = t2.c_datetime and t1.c_int = t2.c_int;

-- join with <=> on bool_null + = on tinyint_null + smallint_null + int
-- query 23
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bool_null  IS NOT DISTINCT FROM  t2.c_bool_null and t1.c_tinyint_null = t2.c_tinyint_null and t1.c_smallint_null = t2.c_smallint_null and t1.c_int = t2.c_int;

-- full join with <=> on bool_null + = on tinyint_null + smallint_null + int
-- query 24
select count(1)
from ${case_db}.t1 full join ${case_db}.t1 t2 on t1.c_bool_null  IS NOT DISTINCT FROM  t2.c_bool_null and t1.c_tinyint_null = t2.c_tinyint_null and t1.c_smallint_null = t2.c_smallint_null and t1.c_int = t2.c_int;

-- join on double + int
-- query 25
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_double = t2.c_double and t1.c_int = t2.c_int;

-- join on double_null + int
-- query 26
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_double_null = t2.c_double_null and t1.c_int = t2.c_int;

-- join with <=> on float_null + = on int
-- query 27
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_float_null  IS NOT DISTINCT FROM  t2.c_float_null and t1.c_int = t2.c_int;

-- join on bigint + int
-- query 28
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bigint = t2.c_bigint and t1.c_int = t2.c_int;

-- join on bigint_null + int
-- query 29
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null and t1.c_int = t2.c_int;

-- join on bigint_null + int + double
-- query 30
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_bigint_null = t2.c_bigint_null and t1.c_int = t2.c_int and t1.c_double = t2.c_double;

-- join on datetime + cast(int as string)
-- query 31
select count(1)
from ${case_db}.t1 join ${case_db}.t1 t2 on t1.c_datetime = t2.c_datetime and cast(t1.c_int as string) = cast(t2.c_int as string);
