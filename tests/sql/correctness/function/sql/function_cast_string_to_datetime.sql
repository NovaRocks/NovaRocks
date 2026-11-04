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

-- Migrated from dev/test/sql/test_function/T/test_cast_string_to_datetime
-- Test Objective:
-- 1. Validate CAST(string AS DATETIME) for various date/datetime string formats
--    including compact, dot-separated, ISO 8601, and whitespace-padded forms.
-- 2. Verify correct handling of invalid strings (returns NULL).
-- 3. Verify leap-year edge cases.
-- 4. Verify ISO 8601 Z-suffix (UTC) parsing with microsecond precision.
-- 5. Verify out-of-range hour/minute/second components yield NULL.
-- 6. Test both table-based (large volume) and constant-expression paths.

-- query 1
-- Setup: create tables and load bulk data
USE ${case_db};
create table t1 (
    k1 int NULL,
    date_str string,
    datetime_str string,
    date_str_with_whitespace string,
    datetime_str_with_whitespace string
)
TBLPROPERTIES ("format-version" = "3");

CREATE TABLE __row_util (
  k1 bigint null
)
TBLPROPERTIES ("format-version" = "3");
insert into __row_util select generate_series from TABLE(generate_series(0, 10000 - 1));
insert into __row_util select k1 + 20000 from __row_util;
insert into __row_util select k1 + 40000 from __row_util;
insert into __row_util select k1 + 80000 from __row_util;

insert into t1
select
    cast(random() *2e9 as int),
    cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar),
    concat(cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar), ' 01:02:03'),
    concat('  ', cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar), '  '),
    concat('   ', cast(cast(date_add('2020-01-01', INTERVAL row_number() over() DAY) as date) as varchar), ' 01:02:03  ')
from __row_util;

insert into t1 (date_str)
values
    ("20200101"),
    ("20200101010203"),
    ("20200101T010203"),
    ("20200101T0102033"),
    ("20200101T010203.123"),

    ("200101"),
    ("200101010203"),
    ("200101T010203"),
    ("200101T0102033"),
    ("200101T010203.123"),

    ("2020.01.01"),
    ("2020.01.01T01.02.03"),
    ("2020.01.01T01.02.033"),
    ("2020.01.01T01.0203.123"),

    ("  20200101 "),
    ("  20200101010203   "),
    ("  20200101T010203"),
    ("20200101T0102033  "),
    ("  20200101T010203.123    \n"),
    ("  20200101T010203.123    \n \t \v \f \r "),

    ("2020.13.29"),
    ("2020.13.61"),

    ("2020.02.29"),
    ("2020.02.28"),
    ("2000.02.29"),
    ("2000.02.28"),

    ("2021.02.28"),
    ("2100.02.28"),

    ("invalid"),

    ("2021.02.29"),
    ("2100.02.29"),

    ("?100.02.28"),
    ("2?00.02.28"),
    ("21?0.02.28"),
    ("210?.02.28"),
    ("2100902.28"),
    ("2100.?2.28"),
    ("2100.0?.28"),
    ("2100.02928"),
    ("2100.02.?8"),
    ("2100.02.2?"),
    ("2020-01-01T01:02:03Z"),
    ("2020-01-01T01:02:03.123Z"),
    ("2020-01-01T01:02:03.123456Z"),
    ("2020-01-01T01:02:03.1Z"),
    ("  2020-01-01T01:02:03Z  "),
    ("  2020-01-01T01:02:03.123Z  "),

    ("2020-01-01T01:02:03z"),
    ("2020-01-01T01:02:03Zextra"),
    ("2020-01-01T01:02:03.Z"),
    ("2020-01-01T01:02:03.1234567Z");

select
    ifnull(sum(murmur_hash3_32(
        cast(date_str as datetime)
    )), 0) +
    ifnull(sum(murmur_hash3_32(
        cast(date_str_with_whitespace as datetime)
    )), 0) +
    ifnull(sum(murmur_hash3_32(
        cast(datetime_str as datetime)
    )), 0) +
    ifnull(sum(murmur_hash3_32(
        cast(datetime_str_with_whitespace as datetime)
    )), 0)
from t1;

-- query 2
-- Query with null rows added
USE ${case_db};
insert into t1
select
    cast(random() *2e9 as int),
    null, null, null ,null
from __row_util;

select
    ifnull(sum(murmur_hash3_32(
        cast(date_str as datetime)
    )), 0) +
    ifnull(sum(murmur_hash3_32(
        cast(date_str_with_whitespace as datetime)
    )), 0) +
    ifnull(sum(murmur_hash3_32(
        cast(datetime_str as datetime)
    )), 0) +
    ifnull(sum(murmur_hash3_32(
        cast(datetime_str_with_whitespace as datetime)
    )), 0)
from t1;

-- query 3
USE ${case_db};
select cast(column_0 as datetime) from (values (NULL)) as tmp(column_0);

-- query 4
USE ${case_db};
select cast(column_0 as datetime) from (values ("20200101")) as tmp(column_0);

-- query 5
USE ${case_db};
select cast(column_0 as datetime) from (values ("20200101010203")) as tmp(column_0);

-- query 6
USE ${case_db};
select cast(column_0 as datetime) from (values ("20200101T010203")) as tmp(column_0);

-- query 7
USE ${case_db};
select cast(column_0 as datetime) from (values ("20200101T0102033")) as tmp(column_0);

-- query 8
USE ${case_db};
select cast(column_0 as datetime) from (values ("20200101T010203.123")) as tmp(column_0);

-- query 9
USE ${case_db};
select cast(column_0 as datetime) from (values ("200101")) as tmp(column_0);

-- query 10
USE ${case_db};
select cast(column_0 as datetime) from (values ("200101010203")) as tmp(column_0);

-- query 11
USE ${case_db};
select cast(column_0 as datetime) from (values ("200101T010203")) as tmp(column_0);

-- query 12
USE ${case_db};
select cast(column_0 as datetime) from (values ("200101T0102033")) as tmp(column_0);

-- query 13
USE ${case_db};
select cast(column_0 as datetime) from (values ("200101T010203.123")) as tmp(column_0);

-- query 14
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.01.01")) as tmp(column_0);

-- query 15
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.01.01T01.02.03")) as tmp(column_0);

-- query 16
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.01.01T01.02.033")) as tmp(column_0);

-- query 17
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.01.01T01.0203.123")) as tmp(column_0);

-- query 18
USE ${case_db};
select cast(column_0 as datetime) from (values ("  20200101 ")) as tmp(column_0);

-- query 19
USE ${case_db};
select cast(column_0 as datetime) from (values ("  20200101010203   ")) as tmp(column_0);

-- query 20
USE ${case_db};
select cast(column_0 as datetime) from (values ("  20200101T010203")) as tmp(column_0);

-- query 21
USE ${case_db};
select cast(column_0 as datetime) from (values ("20200101T0102033  ")) as tmp(column_0);

-- query 22
USE ${case_db};
select cast(column_0 as datetime) from (values ("  20200101T010203.123    \n")) as tmp(column_0);

-- query 23
USE ${case_db};
select cast(column_0 as datetime) from (values ("  20200101T010203.123    \n \t \v \f \r ")) as tmp(column_0);

-- query 24
-- Invalid month/day
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.13.29")) as tmp(column_0);

-- query 25
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.13.61")) as tmp(column_0);

-- query 26
-- Leap year: 2020 is leap
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.02.29")) as tmp(column_0);

-- query 27
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020.02.28")) as tmp(column_0);

-- query 28
-- Leap year: 2000 is leap (divisible by 400)
USE ${case_db};
select cast(column_0 as datetime) from (values ("2000.02.29")) as tmp(column_0);

-- query 29
USE ${case_db};
select cast(column_0 as datetime) from (values ("2000.02.28")) as tmp(column_0);

-- query 30
USE ${case_db};
select cast(column_0 as datetime) from (values ("2021.02.28")) as tmp(column_0);

-- query 31
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100.02.28")) as tmp(column_0);

-- query 32
-- Completely invalid string
USE ${case_db};
select cast(column_0 as datetime) from (values ("invalid")) as tmp(column_0);

-- query 33
-- Non-leap year Feb 29
USE ${case_db};
select cast(column_0 as datetime) from (values ("2021.02.29")) as tmp(column_0);

-- query 34
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100.02.29")) as tmp(column_0);

-- query 35
-- Invalid characters in various positions
USE ${case_db};
select cast(column_0 as datetime) from (values ("?100.02.28")) as tmp(column_0);

-- query 36
USE ${case_db};
select cast(column_0 as datetime) from (values ("2?00.02.28")) as tmp(column_0);

-- query 37
USE ${case_db};
select cast(column_0 as datetime) from (values ("21?0.02.28")) as tmp(column_0);

-- query 38
USE ${case_db};
select cast(column_0 as datetime) from (values ("210?.02.28")) as tmp(column_0);

-- query 39
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100902.28")) as tmp(column_0);

-- query 40
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100.?2.28")) as tmp(column_0);

-- query 41
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100.0?.28")) as tmp(column_0);

-- query 42
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100.02928")) as tmp(column_0);

-- query 43
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100.02.?8")) as tmp(column_0);

-- query 44
USE ${case_db};
select cast(column_0 as datetime) from (values ("2100.02.2?")) as tmp(column_0);

-- query 45
-- ISO 8601 Z-suffix (UTC) formats
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03Z")) as tmp(column_0);

-- query 46
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03.123Z")) as tmp(column_0);

-- query 47
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03.123456Z")) as tmp(column_0);

-- query 48
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03.1Z")) as tmp(column_0);

-- query 49
USE ${case_db};
select cast(column_0 as datetime) from (values ("  2020-01-01T01:02:03Z  ")) as tmp(column_0);

-- query 50
USE ${case_db};
select cast(column_0 as datetime) from (values ("  2020-01-01T01:02:03.123Z  ")) as tmp(column_0);

-- query 51
-- Invalid Z-suffix variations
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03z")) as tmp(column_0);

-- query 52
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03Zextra")) as tmp(column_0);

-- query 53
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03.Z")) as tmp(column_0);

-- query 54
USE ${case_db};
select cast(column_0 as datetime) from (values ("2020-01-01T01:02:03.1234567Z")) as tmp(column_0);

-- query 55
-- Invalid time components: minute >= 60
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 01:60:00")) as tmp(column_0);

-- query 56
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 01:61:00")) as tmp(column_0);

-- query 57
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 01:99:00")) as tmp(column_0);

-- query 58
-- Invalid time components: second >= 60
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 01:30:60")) as tmp(column_0);

-- query 59
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 01:30:61")) as tmp(column_0);

-- query 60
-- Invalid time components: hour >= 24
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 24:00:00")) as tmp(column_0);

-- query 61
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 25:00:00")) as tmp(column_0);

-- query 62
-- Valid edge cases for time components
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 23:59:59")) as tmp(column_0);

-- query 63
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 00:00:00")) as tmp(column_0);

-- query 64
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 01:59:00")) as tmp(column_0);

-- query 65
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01 01:30:59")) as tmp(column_0);

-- query 66
-- Invalid time in ISO 8601 format
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01T01:61:00")) as tmp(column_0);

-- query 67
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01T01:61:00Z")) as tmp(column_0);

-- query 68
USE ${case_db};
select cast(column_0 as datetime) from (values ("2024-01-01T24:00:00")) as tmp(column_0);
