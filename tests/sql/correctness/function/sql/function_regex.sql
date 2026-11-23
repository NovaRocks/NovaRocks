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

-- Migrated from dev/test/sql/test_function/T/test_regex
-- Test Objective:
-- 1. Validate regexp_replace with constant and table-based inputs, including
--    Unicode (Han characters), overlapping patterns, empty/NULL inputs.
-- 2. Validate regexp_extract_all with group indices, constant and non-constant args.
-- 3. Validate regexp_position with various start positions, occurrence counts,
--    empty patterns, Unicode strings, constant and non-constant patterns.
-- 4. Validate regexp_count with constant and table-based inputs.
-- 5. Verify error handling for invalid regex patterns across all regexp functions.
-- 6. Validate regexp_replace multi-row correctness (issue #65936).

-- query 1
-- Setup: create table ts for regexp_replace tests
USE ${case_db};
CREATE TABLE "ts" (
  "str" varchar(65533),
  "regex" varchar(65533),
  "replaced" varchar(65533)
) WITH (format_version = 3);

insert into ts values ROW('abcd', '.*', 'xx'), ROW('abcd', 'a.*', 'xx'), ROW('abcd', '.*abc.*', 'xx'), ROW('abcd', '.*cd', 'xx'), ROW('abcd', 'bc', 'xx'), ROW('', '', 'xx'), ROW(NULL, '', 'xx'), ROW('abc中文def', '[中文]+', 'xx');
insert into ts values ROW('a b c', ' ', '-'), ROW('           XXXX', '       ', '');
insert into ts values ROW('xxxx', 'x', '-'), ROW('xxxx', 'xx', '-'), ROW('xxxx', 'xxx', '-'), ROW('xxxx', 'xxxx', '-');
insert into ts values ROW('xxxx', 'not', 'xxxxxxxx'), ROW('xxaxx', 'xx', 'aaa'), ROW('xaxaxax', 'xax', '-');

select regexp_replace('abcd', '.*', 'xx');

-- query 2
USE ${case_db};
select regexp_replace('abcd', 'a.*', 'xx');

-- query 3
USE ${case_db};
select regexp_replace('abcd', '.*abc.*', 'xx');

-- query 4
USE ${case_db};
select regexp_replace('abcd', '.*cd', 'xx');

-- query 5
USE ${case_db};
select regexp_replace('abcd', 'bc', 'xx');

-- query 6
USE ${case_db};
select regexp_replace('', '', 'xx');

-- query 7
USE ${case_db};
select regexp_replace(NULL, '', 'xx');

-- query 8
USE ${case_db};
select regexp_replace('abc中文def', '中文', 'xx');

-- query 9
USE ${case_db};
select regexp_replace('abc中文def', '[中文]+', 'xx');

-- query 10
USE ${case_db};
select regexp_replace('a b c', ' ', '-');

-- query 11
USE ${case_db};
select regexp_replace('           XXXX', '       ', '');

-- query 12
USE ${case_db};
select regexp_replace('xxxx', 'x', '-');

-- query 13
USE ${case_db};
select regexp_replace('xxxx', 'xx', '-');

-- query 14
USE ${case_db};
select regexp_replace('xxxx', 'xxx', '-');

-- query 15
USE ${case_db};
select regexp_replace('xxxx', 'xxxx', '-');

-- query 16
USE ${case_db};
select regexp_replace('xxxx', 'not', 'xxxxxxxx');

-- query 17
USE ${case_db};
select regexp_replace('xxaxx', 'xx', 'aaa');

-- query 18
USE ${case_db};
select regexp_replace('xaxaxax', 'xax', '-');

-- query 19
-- Table-based regexp_replace
USE ${case_db};
select str, regex, replaced, regexp_replace(str, regex, replaced) from ts order by str, regex, replaced;

-- query 20
-- Setup: create table tsr for regexp_extract_all tests
USE ${case_db};
CREATE TABLE "tsr" (
  "str" varchar(65533),
  "regex" varchar(65533),
  "pos" int
) WITH (format_version = 3);

insert into tsr values ROW('AbCdExCeF', '([a-z]+)C([a-z]+)', 1), ROW('AbCdExCeF', '([a-z]+)C([a-z]+)', 0);

SELECT regexp_extract_all('AbCdExCeF', '([a-z]+)C([a-z]+)', 0);

-- query 21
USE ${case_db};
SELECT regexp_extract_all(str, '([a-z]+)C([a-z]+)', 0) from tsr;

-- query 22
USE ${case_db};
SELECT regexp_extract_all(str, regex, 0) from tsr;

-- query 23
USE ${case_db};
SELECT regexp_extract_all(str, regex, pos) from tsr;

-- query 24
USE ${case_db};
SELECT regexp_extract_all(str, '([a-z]+)C([a-z]+)', pos) from tsr;

-- query 25
USE ${case_db};
SELECT regexp_extract_all('AbCdExCeF', '([a-z]+)C([a-z]+)', pos) from tsr;

-- query 26
USE ${case_db};
SELECT regexp_extract_all('AbCdExCeF', regex, pos) from tsr;

-- query 27
USE ${case_db};
SELECT regexp_extract_all(str, '([a-z]+)C([a-z]+)', pos) from tsr;

-- query 28
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_extract_all('AbCdExCeF', '([a-z]+)C([a-z]+)', 3);

-- query 29
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_extract_all(str, '([a-z]+)C([a-z]+)', 3) from tsr;

-- query 30
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_extract_all(str, regex, 3) from tsr;

-- query 31
-- regexp_position constant tests
USE ${case_db};
SELECT regexp_position('a.b:c;d', '[.]');

-- query 32
USE ${case_db};
SELECT regexp_position('a.b:c;d', '[\.:;]', 2);

-- query 33
USE ${case_db};
SELECT regexp_position('a.b:c;d', ':');

-- query 34
USE ${case_db};
SELECT regexp_position('a,b,c', ',');

-- query 35
USE ${case_db};
SELECT regexp_position('a1b2c3d', '[0-9]');

-- query 36
USE ${case_db};
SELECT regexp_position('a,b,c', ',', 3);

-- query 37
USE ${case_db};
SELECT regexp_position('a1b2c3d', '[0-9]', 5);

-- query 38
USE ${case_db};
SELECT regexp_position('a1b2c3d4e', '[0-9]', 4, 2);

-- query 39
USE ${case_db};
SELECT regexp_position('a1b2c3d',  '[0-9]', 4, 3);

-- query 40
-- Empty pattern tests
USE ${case_db};
SELECT regexp_position('a1b2c3d', '', 1);

-- query 41
USE ${case_db};
SELECT regexp_position('a1b2c3d', '', 2);

-- query 42
USE ${case_db};
SELECT regexp_position('a1b2c3d', '', 2, 2);

-- query 43
USE ${case_db};
SELECT regexp_position('a1b2c3d', '', 2, 6);

-- query 44
USE ${case_db};
SELECT regexp_position('a1b2c3d', '', 2, 7);

-- query 45
USE ${case_db};
SELECT regexp_position('a1b2c3d', '', 2, 8);

-- query 46
-- Empty string input
USE ${case_db};
SELECT regexp_position('', ',');

-- query 47
USE ${case_db};
SELECT regexp_position('', ',', 4);

-- query 48
USE ${case_db};
SELECT regexp_position('', ',', 4, 2);

-- query 49
USE ${case_db};
SELECT regexp_position('a,b,c',  ',', 2);

-- query 50
USE ${case_db};
SELECT regexp_position('a1b2c3d', '[0-9]', 4);

-- query 51
-- Start position beyond string length
USE ${case_db};
SELECT regexp_position('a,b,c', ',', 1000);

-- query 52
USE ${case_db};
SELECT regexp_position('a,b,c', ',', 8);

-- query 53
-- Unicode string tests
USE ${case_db};
SELECT regexp_position('有朋$%X自9远方来', '[0-9]', 7);

-- query 54
USE ${case_db};
SELECT regexp_position('有朋$%X自9远方9来', '[0-9]', 10, 1);

-- query 55
USE ${case_db};
SELECT regexp_position('有朋$%X自9远方9来', '[0-9]', 10, 2);

-- query 56
USE ${case_db};
SELECT regexp_position('有朋$%X自9远方9来', '来', 999);

-- query 57
USE ${case_db};
SELECT regexp_position('行成于思str而毁123于随', '于', 3, 2);

-- query 58
USE ${case_db};
SELECT regexp_position('行成于思str而毁123于随', '',  3, 1);

-- query 59
USE ${case_db};
SELECT regexp_position('行成于思str而毁123于随', '',  3, 2);

-- query 60
-- VALUES clause inputs
USE ${case_db};
SELECT regexp_position(column_0, 'T') FROM (VALUES ROW('2024-01-01T01:61:00')) AS tmp(column_0);

-- query 61
USE ${case_db};
SELECT regexp_position(column_0, '61') FROM (VALUES ROW('2024-01-01T01:61:00')) AS tmp(column_0);

-- query 62
USE ${case_db};
SELECT regexp_position(column_0, '0', 1, 3) FROM (VALUES ROW('2024-01-01T01:61:00')) AS tmp(column_0);

-- query 63
USE ${case_db};
SELECT regexp_position(column_0, 'Z', 20) FROM (VALUES ROW('2024-01-01T01:61:00')) AS tmp(column_0);

-- query 64
USE ${case_db};
SELECT regexp_position(column_0, '01', 12, 2) FROM (VALUES ROW('2024-01-01T01:61:00')) AS tmp(column_0);

-- query 65
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position(column_0, '(unclosed') FROM (VALUES ROW('oops')) AS tmp(column_0);

-- query 66
-- NULL input through VALUES
USE ${case_db};
SELECT regexp_position(column_0, 'x') FROM (VALUES ROW(NULL)) AS tmp(column_0);

-- query 67
-- Setup: create table tsp_general for general-path regexp_position tests
USE ${case_db};
CREATE TABLE "tsp_general" (
  "str" varchar(65533),
  "pattern" varchar(65533),
  "start_pos" int,
  "occurrence" int
) WITH (format_version = 3);

insert into tsp_general values
ROW('abcdef123ghi', '[0-9]+', 1, 1),
ROW('abc def ghi', '\s+', 1, 1),
ROW('test@email.com', '@[a-zA-Z]+', 1, 1),
ROW('hello world hello universe', 'hello', 1, 2),
ROW('complex(pattern)test', '\([^)]*\)', 1, 1),
ROW('abc123def456ghi', '[0-9][0-9]+', 1, 2),
ROW('', '.', 1, 1),
ROW(NULL, '[0-9]', 1, 1),
ROW('test string', NULL, 1, 1),
ROW(NULL, NULL, 1, 1);

SELECT str, pattern, regexp_position(str, pattern) from tsp_general order by str, pattern;

-- query 68
USE ${case_db};
SELECT str, pattern, start_pos, regexp_position(str, pattern, start_pos) from tsp_general where start_pos is not null order by str, pattern;

-- query 69
USE ${case_db};
SELECT str, pattern, start_pos, occurrence, regexp_position(str, pattern, start_pos, occurrence) from tsp_general where start_pos is not null and occurrence is not null order by str, pattern;

-- query 70
-- NULL input tests for regexp_position
USE ${case_db};
SELECT regexp_position(NULL, '[0-9]');

-- query 71
USE ${case_db};
SELECT regexp_position('test', NULL);

-- query 72
USE ${case_db};
SELECT regexp_position(NULL, NULL);

-- query 73
USE ${case_db};
SELECT regexp_position(NULL, '[0-9]', 1);

-- query 74
USE ${case_db};
SELECT regexp_position('test', NULL, 1);

-- query 75
USE ${case_db};
SELECT regexp_position('test', '[0-9]', NULL);

-- query 76
USE ${case_db};
SELECT regexp_position(NULL, '[0-9]', 1, 1);

-- query 77
USE ${case_db};
SELECT regexp_position('test', NULL, 1, 1);

-- query 78
USE ${case_db};
SELECT regexp_position('test', '[0-9]', NULL, 1);

-- query 79
USE ${case_db};
SELECT regexp_position('test', '[0-9]', 1, NULL);

-- query 80
-- Invalid regex patterns for regexp_position
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test string', '[0-9');

-- query 81
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test string', '(unclosed');

-- query 82
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test string', '?invalid');

-- query 83
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test string', '*invalid');

-- query 84
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test string', '+invalid');

-- query 85
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test string', '\');

-- query 86
-- Edge cases with invalid start/occurrence parameters
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test', '[0-9]', 0);

-- query 87
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test', '[0-9]', -1);

-- query 88
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test', '[0-9]', 1, 0);

-- query 89
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position('test', '[0-9]', 1, -1);

-- query 90
-- Setup: create table tsc for regexp_count tests
USE ${case_db};
CREATE TABLE "tsc" (
  "str" varchar(65533),
  "regex" varchar(65533)
) WITH (format_version = 3);

insert into tsc values ROW('abc123def456', '[0-9]'), ROW('test.com test.net test.org', '\.'), ROW('a b  c   d', '\s+'), ROW('ababababab', 'ab'), ROW('', '.'), ROW(NULL, '.');

select regexp_count('abc123def456', '[0-9]');

-- query 91
USE ${case_db};
select regexp_count('test.com test.net test.org', '\.');

-- query 92
USE ${case_db};
select regexp_count('a b  c   d', '\s+');

-- query 93
USE ${case_db};
select regexp_count('ababababab', 'ab');

-- query 94
USE ${case_db};
select regexp_count('', '.');

-- query 95
USE ${case_db};
select regexp_count(NULL, '.');

-- query 96
USE ${case_db};
select regexp_count('abc', NULL);

-- query 97
USE ${case_db};
select regexp_count('abc中文def', '[中文]+');

-- query 98
USE ${case_db};
select regexp_count('AbCdExCeF', 'C');

-- query 99
USE ${case_db};
select regexp_count('1a 2b 14m', '\d+');

-- query 100
-- Table-based regexp_count
USE ${case_db};
select str, regex, regexp_count(str, regex) from tsc order by str, regex;

-- query 101
-- Setup: create table tsc_invalid with invalid regex patterns
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,4
USE ${case_db};
CREATE TABLE "tsc_invalid" (
  "str" varchar(65533),
  "regex" varchar(65533)
) WITH (format_version = 3);

insert into tsc_invalid values
ROW('abc123def456', '[0-9'),
ROW('test string', '(unclosed'),
ROW('repetition test', '?invalid'),
ROW('valid test', 'valid');

select str, regex, regexp_count(str, regex) from tsc_invalid order by str, regex;

-- query 102
-- Invalid regex constant tests for regexp_count
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
select regexp_count('test string', '[0-9');

-- query 103
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
select regexp_count('test string', '(unclosed');

-- query 104
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
select regexp_count('test string', '?invalid');

-- query 105
USE ${case_db};
select regexp_count('test string', 'a{,}');

-- query 106
USE ${case_db};
select regexp_count(NULL, '[0-9');

-- query 107
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
select regexp_count('', '[0-9');

-- query 108
USE ${case_db};
select regexp_count('test', NULL);

-- query 109
-- Setup: create table for multi-row regexp_replace correctness test (issue #65936)
USE ${case_db};
CREATE TABLE "test_regexp_replace_multirow" (
  "column_value" varchar(100)
) WITH (format_version = 3);

insert into test_regexp_replace_multirow values
ROW('activity_60_package_2'),
ROW('activity_50_package_7'),
ROW('activity_70_package_1'),
ROW('nomatch_value'),
ROW('test_package_suffix');

SELECT regexp_replace(column_value, '_package_.*', '')
FROM (VALUES
    ROW('activity_60_package_2'),
    ROW('activity_50_package_7')
) AS t(column_value);

-- query 110
USE ${case_db};
SELECT regexp_replace(column_value, '_package_.*', '')
FROM test_regexp_replace_multirow
ORDER BY column_value;

-- query 111
USE ${case_db};
SELECT regexp_replace(column_value, '_[0-9]+', '_X')
FROM test_regexp_replace_multirow
ORDER BY column_value;

-- query 112
-- Evaluation order with actual typed source, pattern, and late arguments.
USE ${case_db};
CREATE TABLE regex_evaluation_order (
    id integer, source varchar(10), pattern varchar(10), replacement varchar(10),
    group_id bigint, start_position integer
) WITH (format_version = 3);
INSERT INTO regex_evaluation_order VALUES
    ROW(1, 'a', '[', NULL, NULL, NULL),
    ROW(2, 'a', '(a)', 'X', 1, 1),
    ROW(3, NULL, '[', 'X', 0, 1);
SELECT id,
       TRY(regexp_replace(source, pattern, replacement)) AS replaced,
       TRY(regexp_extract(source, pattern, group_id)) AS extracted,
       TRY(regexp_position(source, pattern, start_position)) AS position,
       IF(id = 1, BIGINT '0', regexp_count(source, pattern)) AS lazy_count
FROM regex_evaluation_order
ORDER BY id;

-- query 113
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_replace(source, pattern, replacement) FROM regex_evaluation_order WHERE id = 1;

-- query 114
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_extract(source, pattern, group_id) FROM regex_evaluation_order WHERE id = 1;

-- query 115
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR,2
USE ${case_db};
SELECT regexp_position(source, pattern, start_position) FROM regex_evaluation_order WHERE id = 1;

-- query 116
USE ${case_db};
SELECT regexp_replace('a', '[', CAST(NULL AS varchar)) AS known_null_skip,
       regexp_replace(source, pattern, replacement) AS null_source_skip,
       regexp_extract(source, pattern, group_id) AS null_extract,
       regexp_position(source, pattern, start_position) AS null_position
FROM regex_evaluation_order WHERE id = 3;

-- query 117
SELECT regexp_count('١', '\d') AS ascii_digit_class,
       regexp_position('é中a中', '中', 2, 2) AS multibyte_position,
       regexp_position('ab', '^b', 2) AS absolute_anchor,
       regexp_extract_all('ab', '(x)?a', 1) AS missing_capture,
       regexp_replace('abcd', '.*', 'xx') AS repeated_empty_match,
       regexp_replace('no match', '(a)b', '$2') AS unused_replacement,
       regexp_count('a{,}a{,}', 'a{,}') AS literal_braces;

-- query 118
SELECT regexp_extract('ab', '(?:a)?b') AS optional_piece,
       regexp_extract('aabb', '(?:a?b)+') AS advancing_repeat,
       regexp_count('é中', '') AS empty_pattern,
       regexp_count(U&'a\000Ab', '.') AS plain_dot,
       regexp_count(U&'a\000Ab', '(?s).') AS dotall;

-- query 119
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT regexp_extract('ab', '\p{Han}');

-- query 120
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT regexp_extract('ab', '[[:lower:]]');

-- query 121
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT regexp_extract('ab', 'a{2,4}');

-- query 122
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT regexp_extract('ab', 'a(?=b)');

-- query 123
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT regexp_extract('ab', '(a)\1');

-- query 124
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT regexp_extract('ab', '(?:a?|b)*');

-- query 125
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT regexp_extract('ab', '(?m)^');

-- query 126
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT regexp_extract('ab', 'a{2,1}');

-- query 127
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT regexp_extract('ab', 'a(?=b)(');

-- query 128
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT regexp_extract('ab', '(a)\1[');

-- query 129
-- @expect_trino_error=INVALID_FUNCTION_ARGUMENT,USER_ERROR
SELECT regexp_replace('a', '(a)', '$2');

-- query 130
USE ${case_db};
SELECT str, regex, regexp_count(str, regex)
FROM tsc_invalid WHERE str = 'valid test';
