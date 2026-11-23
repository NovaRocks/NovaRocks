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
-- Check timestamp casts on bulk stored strings and constant VALUES rows.
-- Trino 484 accepts date/time text separated by a space. Compact and dotted
-- date text and ISO 8601 T-separated input are outside this CAST grammar.
-- TRY_CAST captures invalid date, clock and text values as NULL.

-- query 1
USE ${case_db};
CREATE TABLE t1 (
    date_str varchar,
    datetime_str varchar,
    date_str_with_whitespace varchar,
    datetime_str_with_whitespace varchar,
    expected_date timestamp(6),
    expected_datetime timestamp(6)
) WITH (format_version = 3);

CREATE TABLE __row_util (
    k1 bigint
) WITH (format_version = 3);
INSERT INTO __row_util SELECT generate_series FROM TABLE(sequence(0, 10000 - 1)) AS generate_series(generate_series);
INSERT INTO __row_util SELECT k1 + 20000 FROM __row_util;
INSERT INTO __row_util SELECT k1 + 40000 FROM __row_util;
INSERT INTO __row_util SELECT k1 + 80000 FROM __row_util;

INSERT INTO t1
SELECT
    CAST(d AS varchar),
    CONCAT(CAST(d AS varchar), ' 01:02:03'),
    CONCAT('  ', CAST(d AS varchar), '  '),
    CONCAT('   ', CAST(d AS varchar), ' 01:02:03  '),
    CAST(d AS timestamp(6)),
    CAST(d AS timestamp(6)) + INTERVAL '01:02:03' HOUR TO SECOND
FROM (
    SELECT DATE '2020-01-01' + ROW_NUMBER() OVER () * INTERVAL '1' DAY AS d
    FROM __row_util
) AS generated_dates;

insert into t1 (date_str)
values
    ROW('20200101'),
    ROW('20200101010203'),
    ROW('20200101T010203'),
    ROW('20200101T0102033'),
    ROW('20200101T010203.123'),

    ROW('200101'),
    ROW('200101010203'),
    ROW('200101T010203'),
    ROW('200101T0102033'),
    ROW('200101T010203.123'),

    ROW('2020.01.01'),
    ROW('2020.01.01T01.02.03'),
    ROW('2020.01.01T01.02.033'),
    ROW('2020.01.01T01.0203.123'),

    ROW('  20200101 '),
    ROW('  20200101010203   '),
    ROW('  20200101T010203'),
    ROW('20200101T0102033  '),
    ROW(U&'  20200101T010203.123    \000A'),
    ROW(U&'  20200101T010203.123    \000A \0009 \000B \000C \000D '),

    ROW('2020.13.29'),
    ROW('2020.13.61'),

    ROW('2020.02.29'),
    ROW('2020.02.28'),
    ROW('2000.02.29'),
    ROW('2000.02.28'),

    ROW('2021.02.28'),
    ROW('2100.02.28'),

    ROW('invalid'),

    ROW('2021.02.29'),
    ROW('2100.02.29'),

    ROW('?100.02.28'),
    ROW('2?00.02.28'),
    ROW('21?0.02.28'),
    ROW('210?.02.28'),
    ROW('2100902.28'),
    ROW('2100.?2.28'),
    ROW('2100.0?.28'),
    ROW('2100.02928'),
    ROW('2100.02.?8'),
    ROW('2100.02.2?'),
    ROW('2020-01-01T01:02:03Z'),
    ROW('2020-01-01T01:02:03.123Z'),
    ROW('2020-01-01T01:02:03.123456Z'),
    ROW('2020-01-01T01:02:03.1Z'),
    ROW('  2020-01-01T01:02:03Z  '),
    ROW('  2020-01-01T01:02:03.123Z  '),

    ROW('2020-01-01T01:02:03z'),
    ROW('2020-01-01T01:02:03Zextra'),
    ROW('2020-01-01T01:02:03.Z'),
    ROW('2020-01-01T01:02:03.1234567Z');

SELECT
    COUNT(*) AS row_count,
    COUNT_IF(TRY_CAST(date_str AS timestamp(6)) IS DISTINCT FROM expected_date) AS date_mismatches,
    COUNT_IF(TRY_CAST(datetime_str AS timestamp(6)) IS DISTINCT FROM expected_datetime) AS datetime_mismatches,
    COUNT_IF(TRY_CAST(date_str_with_whitespace AS timestamp(6)) IS DISTINCT FROM expected_date) AS padded_date_mismatches,
    COUNT_IF(TRY_CAST(datetime_str_with_whitespace AS timestamp(6)) IS DISTINCT FROM expected_datetime) AS padded_datetime_mismatches
FROM t1;

-- query 2
USE ${case_db};
INSERT INTO t1 (date_str) SELECT CAST(NULL AS varchar) FROM __row_util;

SELECT
    COUNT(*) AS row_count,
    COUNT_IF(TRY_CAST(date_str AS timestamp(6)) IS DISTINCT FROM expected_date) AS date_mismatches,
    COUNT_IF(TRY_CAST(datetime_str AS timestamp(6)) IS DISTINCT FROM expected_datetime) AS datetime_mismatches,
    COUNT_IF(TRY_CAST(date_str_with_whitespace AS timestamp(6)) IS DISTINCT FROM expected_date) AS padded_date_mismatches,
    COUNT_IF(TRY_CAST(datetime_str_with_whitespace AS timestamp(6)) IS DISTINCT FROM expected_datetime) AS padded_datetime_mismatches
FROM t1;

-- query 3
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW(NULL)) as tmp(column_0);

-- query 4
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('20200101')) as tmp(column_0);

-- query 5
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('20200101010203')) as tmp(column_0);

-- query 6
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('20200101T010203')) as tmp(column_0);

-- query 7
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('20200101T0102033')) as tmp(column_0);

-- query 8
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('20200101T010203.123')) as tmp(column_0);

-- query 9
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('200101')) as tmp(column_0);

-- query 10
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('200101010203')) as tmp(column_0);

-- query 11
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('200101T010203')) as tmp(column_0);

-- query 12
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('200101T0102033')) as tmp(column_0);

-- query 13
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('200101T010203.123')) as tmp(column_0);

-- query 14
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.01.01')) as tmp(column_0);

-- query 15
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.01.01T01.02.03')) as tmp(column_0);

-- query 16
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.01.01T01.02.033')) as tmp(column_0);

-- query 17
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.01.01T01.0203.123')) as tmp(column_0);

-- query 18
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('  20200101 ')) as tmp(column_0);

-- query 19
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('  20200101010203   ')) as tmp(column_0);

-- query 20
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('  20200101T010203')) as tmp(column_0);

-- query 21
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('20200101T0102033  ')) as tmp(column_0);

-- query 22
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW(U&'  20200101T010203.123    \000A')) as tmp(column_0);

-- query 23
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW(U&'  20200101T010203.123    \000A \0009 \000B \000C \000D ')) as tmp(column_0);

-- query 24
-- Invalid month/day
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.13.29')) as tmp(column_0);

-- query 25
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.13.61')) as tmp(column_0);

-- query 26
-- A leap-day date with a non-Trino separator
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.02.29')) as tmp(column_0);

-- query 27
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020.02.28')) as tmp(column_0);

-- query 28
-- A leap-century date with a non-Trino separator
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2000.02.29')) as tmp(column_0);

-- query 29
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2000.02.28')) as tmp(column_0);

-- query 30
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2021.02.28')) as tmp(column_0);

-- query 31
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100.02.28')) as tmp(column_0);

-- query 32
-- Completely invalid string
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('invalid')) as tmp(column_0);

-- query 33
-- Non-leap year Feb 29
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2021.02.29')) as tmp(column_0);

-- query 34
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100.02.29')) as tmp(column_0);

-- query 35
-- Invalid characters in various positions
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('?100.02.28')) as tmp(column_0);

-- query 36
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2?00.02.28')) as tmp(column_0);

-- query 37
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('21?0.02.28')) as tmp(column_0);

-- query 38
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('210?.02.28')) as tmp(column_0);

-- query 39
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100902.28')) as tmp(column_0);

-- query 40
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100.?2.28')) as tmp(column_0);

-- query 41
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100.0?.28')) as tmp(column_0);

-- query 42
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100.02928')) as tmp(column_0);

-- query 43
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100.02.?8')) as tmp(column_0);

-- query 44
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2100.02.2?')) as tmp(column_0);

-- query 45
-- ISO 8601 T-separated text with a Z suffix
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03Z')) as tmp(column_0);

-- query 46
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03.123Z')) as tmp(column_0);

-- query 47
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03.123456Z')) as tmp(column_0);

-- query 48
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03.1Z')) as tmp(column_0);

-- query 49
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('  2020-01-01T01:02:03Z  ')) as tmp(column_0);

-- query 50
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('  2020-01-01T01:02:03.123Z  ')) as tmp(column_0);

-- query 51
-- Invalid Z-suffix variations
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03z')) as tmp(column_0);

-- query 52
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03Zextra')) as tmp(column_0);

-- query 53
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03.Z')) as tmp(column_0);

-- query 54
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2020-01-01T01:02:03.1234567Z')) as tmp(column_0);

-- query 55
-- Invalid time components: minute >= 60
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 01:60:00')) as tmp(column_0);

-- query 56
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 01:61:00')) as tmp(column_0);

-- query 57
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 01:99:00')) as tmp(column_0);

-- query 58
-- Invalid time components: second >= 60
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 01:30:60')) as tmp(column_0);

-- query 59
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 01:30:61')) as tmp(column_0);

-- query 60
-- Invalid time components: hour >= 24
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 24:00:00')) as tmp(column_0);

-- query 61
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 25:00:00')) as tmp(column_0);

-- query 62
-- Valid edge cases for time components
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 23:59:59')) as tmp(column_0);

-- query 63
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 00:00:00')) as tmp(column_0);

-- query 64
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 01:59:00')) as tmp(column_0);

-- query 65
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01 01:30:59')) as tmp(column_0);

-- query 66
-- Invalid time in ISO 8601 format
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01T01:61:00')) as tmp(column_0);

-- query 67
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01T01:61:00Z')) as tmp(column_0);

-- query 68
USE ${case_db};
select TRY_CAST(column_0 AS timestamp(6)) from (values ROW('2024-01-01T24:00:00')) as tmp(column_0);

-- query 69
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('2024-01-01 01:60:00' AS timestamp(6));

-- query 70
SELECT
    TRY_CAST('2020-01-01 01:02:03.123456 Z' AS timestamp(6)) AS wall_clock,
    TRY_CAST('2020-01-01 01:02:03.123456 Z' AS timestamp(6) WITH TIME ZONE) AS zoned;

-- query 71
SELECT
    TRY_CAST('2020-02-29' AS timestamp(6)) AS leap_day,
    TRY_CAST('2000-02-29' AS timestamp(6)) AS leap_century,
    TRY_CAST('2021-02-29' AS timestamp(6)) AS invalid_common_year,
    TRY_CAST('2100-02-29' AS timestamp(6)) AS invalid_century;
