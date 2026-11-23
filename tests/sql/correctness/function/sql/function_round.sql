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

-- Migrated from dev/test/sql/test_function/T/test_round
-- Test Objective:
-- 1. Validate round() and dround() with all-NULL column values produce correct null-safe equality results.
-- 2. Test one-arg and two-arg forms of round/dround with NULL propagation.
-- 3. Verify null-safe equality (<=>) behavior when comparing NULL-derived results.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_all_null (
    id_int int ,
    id_boolean boolean ,
    id_tinyint tinyint ,
    id_smallint smallint ,
    id_bigint bigint ,
    id_largeint largeint ,
    id_float REAL ,
    id_double double ,
    id_decimal32 DECIMAL(8, 5) ,
    id_decimal64 DECIMAL(8, 5) ,
    id_decimal128 DECIMAL(8, 5) ,
    id_date date ,
    id_datetime TIMESTAMP(6) ,
    id_varchar varchar(2000) ,
    id_json json,
    id_any_element int ,
    id_any_array array<int> ,
    id_array_boolean array<boolean> ,
    id_array_tinyint array<tinyint> ,
    id_array_smallint array<smallint> ,
    id_array_int array<int> ,
    id_array_bigint array<bigint> ,
    id_array_largeint array<largeint> ,
    id_array_float array<REAL> ,
    id_array_double array<double> ,
    id_array_varchar array<varchar(2000)> ,
    id_array_date array<date> ,
    id_array_datetime array<TIMESTAMP(6)>
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
USE ${case_db};
insert into test_all_null select t.* from (select 1 as d) x left join test_all_null t on x.d = t.id_int;

-- query 3
USE ${case_db};
select (round(id_double))  IS NOT DISTINCT FROM  (round(NULL)) from test_all_null;

-- query 4
USE ${case_db};
select (dround(id_double))  IS NOT DISTINCT FROM  (dround(NULL)) from test_all_null;

-- query 5
USE ${case_db};
select (round(id_double, id_int))  IS NOT DISTINCT FROM  (round(NULL, NULL)) from test_all_null;

-- query 6
USE ${case_db};
select (round(id_double, id_int))  IS NOT DISTINCT FROM  (round(id_double, NULL)) from test_all_null;

-- query 7
USE ${case_db};
select (round(id_double, id_int))  IS NOT DISTINCT FROM  (round(NULL, id_int)) from test_all_null;

-- query 8
USE ${case_db};
select (dround(id_double, id_int))  IS NOT DISTINCT FROM  (dround(NULL, NULL)) from test_all_null;

-- query 9
USE ${case_db};
select (dround(id_double, id_int))  IS NOT DISTINCT FROM  (dround(id_double, NULL)) from test_all_null;

-- query 10
USE ${case_db};
select (dround(id_double, id_int))  IS NOT DISTINCT FROM  (dround(NULL, id_int)) from test_all_null;
