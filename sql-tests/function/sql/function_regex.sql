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
CREATE TABLE `ts` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT "",
  `replaced` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

insert into ts values ('abcd', '.*', 'xx'), ('abcd', 'a.*', 'xx'), ('abcd', '.*abc.*', 'xx'), ('abcd', '.*cd', 'xx'), ('abcd', 'bc', 'xx'), ('', '', 'xx'), (NULL, '', 'xx'), ('abc中文def', '[\\p{Han}]+', 'xx');
insert into ts values ('a b c', " ", "-"), ('           XXXX', '       ', '');
insert into ts values ('xxxx', "x", "-"), ('xxxx', "xx", "-"), ('xxxx', "xxx", "-"), ('xxxx', "xxxx", "-");
insert into ts values ('xxxx', "not", "xxxxxxxx"), ('xxaxx', 'xx', 'aaa'), ('xaxaxax', 'xax', '-');

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
select regexp_replace('abc中文def', '[\\p{Han}]+', 'xx');

-- query 10
USE ${case_db};
select regexp_replace('a b c', " ", "-");

-- query 11
USE ${case_db};
select regexp_replace('           XXXX', '       ', '');

-- query 12
USE ${case_db};
select regexp_replace('xxxx', "x", "-");

-- query 13
USE ${case_db};
select regexp_replace('xxxx', "xx", "-");

-- query 14
USE ${case_db};
select regexp_replace('xxxx', "xxx", "-");

-- query 15
USE ${case_db};
select regexp_replace('xxxx', "xxxx", "-");

-- query 16
USE ${case_db};
select regexp_replace('xxxx', "not", "xxxxxxxx");

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
CREATE TABLE `tsr` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT "",
  `pos` int NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

insert into tsr values ("AbCdExCeF", "([[:lower:]]+)C([[:lower:]]+)", 3), ("AbCdExCeF", "([[:lower:]]+)C([[:lower:]]+)", 0);

SELECT regexp_extract_all("AbCdExCeF", "([[:lower:]]+)C([[:lower:]]+)", 0);

-- query 21
USE ${case_db};
SELECT regexp_extract_all(str, "([[:lower:]]+)C([[:lower:]]+)", 0) from tsr;

-- query 22
USE ${case_db};
SELECT regexp_extract_all(str, regex, 0) from tsr;

-- query 23
USE ${case_db};
SELECT regexp_extract_all(str, regex, pos) from tsr;

-- query 24
USE ${case_db};
SELECT regexp_extract_all(str, "([[:lower:]]+)C([[:lower:]]+)", pos) from tsr;

-- query 25
USE ${case_db};
SELECT regexp_extract_all("AbCdExCeF", "([[:lower:]]+)C([[:lower:]]+)", pos) from tsr;

-- query 26
USE ${case_db};
SELECT regexp_extract_all("AbCdExCeF", regex, pos) from tsr;

-- query 27
USE ${case_db};
SELECT regexp_extract_all(str, "([[:lower:]]+)C([[:lower:]]+)", pos) from tsr;

-- query 28
USE ${case_db};
SELECT regexp_extract_all("AbCdExCeF", "([[:lower:]]+)C([[:lower:]]+)", 3);

-- query 29
USE ${case_db};
SELECT regexp_extract_all(str, "([[:lower:]]+)C([[:lower:]]+)", 3) from tsr;

-- query 30
USE ${case_db};
SELECT regexp_extract_all(str, regex, 3) from tsr;

-- query 31
-- regexp_position constant tests
USE ${case_db};
SELECT regexp_position('a.b:c;d', '[.]');

-- query 32
USE ${case_db};
SELECT regexp_position('a.b:c;d', '[\\.:;]', 2);

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
SELECT regexp_position(column_0, 'T') FROM (VALUES ('2024-01-01T01:61:00')) AS tmp;

-- query 61
USE ${case_db};
SELECT regexp_position(column_0, '61') FROM (VALUES ('2024-01-01T01:61:00')) AS tmp;

-- query 62
USE ${case_db};
SELECT regexp_position(column_0, '0', 1, 3) FROM (VALUES ('2024-01-01T01:61:00')) AS tmp;

-- query 63
USE ${case_db};
SELECT regexp_position(column_0, 'Z', 20) FROM (VALUES ('2024-01-01T01:61:00')) AS tmp;

-- query 64
USE ${case_db};
SELECT regexp_position(column_0, '01', 12, 2) FROM (VALUES ('2024-01-01T01:61:00')) AS tmp;

-- query 65
-- @expect_error=Invalid regex expression
USE ${case_db};
SELECT regexp_position(column_0, '(unclosed') FROM (VALUES ('oops')) AS tmp;

-- query 66
-- NULL input through VALUES
USE ${case_db};
SELECT regexp_position(column_0, 'x') FROM (VALUES (NULL)) AS tmp;

-- query 67
-- Setup: create table tsp_general for general-path regexp_position tests
USE ${case_db};
CREATE TABLE `tsp_general` (
  `str` varchar(65533) NULL COMMENT "",
  `pattern` varchar(65533) NULL COMMENT "",
  `start_pos` int NULL COMMENT "",
  `occurrence` int NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

insert into tsp_general values
('abcdef123ghi', '[0-9]+', 1, 1),
('abc def ghi', '\\s+', 1, 1),
('test@email.com', '@[a-zA-Z]+', 1, 1),
('hello world hello universe', 'hello', 1, 2),
('complex(pattern)test', '\\([^)]*\\)', 1, 1),
('abc123def456ghi', '[0-9]{2,}', 1, 2),
('', '.', 1, 1),
(NULL, '[0-9]', 1, 1),
('test string', NULL, 1, 1),
(NULL, NULL, 1, 1);

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
-- @expect_error=Invalid regex expression
USE ${case_db};
SELECT regexp_position('test string', '[0-9');

-- query 81
-- @expect_error=Invalid regex expression
USE ${case_db};
SELECT regexp_position('test string', '(unclosed');

-- query 82
-- @expect_error=Invalid regex expression
USE ${case_db};
SELECT regexp_position('test string', '?invalid');

-- query 83
-- @expect_error=Invalid regex expression
USE ${case_db};
SELECT regexp_position('test string', '*invalid');

-- query 84
-- @expect_error=Invalid regex expression
USE ${case_db};
SELECT regexp_position('test string', '+invalid');

-- query 85
-- @expect_error=Invalid regex expression
USE ${case_db};
SELECT regexp_position('test string', '\\');

-- query 86
-- Edge cases with invalid start/occurrence parameters
USE ${case_db};
SELECT regexp_position('test', '[0-9]', 0);

-- query 87
USE ${case_db};
SELECT regexp_position('test', '[0-9]', -1);

-- query 88
USE ${case_db};
SELECT regexp_position('test', '[0-9]', 1, 0);

-- query 89
USE ${case_db};
SELECT regexp_position('test', '[0-9]', 1, -1);

-- query 90
-- Setup: create table tsc for regexp_count tests
USE ${case_db};
CREATE TABLE `tsc` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

insert into tsc values ('abc123def456', '[0-9]'), ('test.com test.net test.org', '\\.'), ('a b  c   d', '\\s+'), ('ababababab', 'ab'), ('', '.'), (NULL, '.');

select regexp_count('abc123def456', '[0-9]');

-- query 91
USE ${case_db};
select regexp_count('test.com test.net test.org', '\\.');

-- query 92
USE ${case_db};
select regexp_count('a b  c   d', '\\s+');

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
select regexp_count('abc中文def', '[\\p{Han}]+');

-- query 98
USE ${case_db};
select regexp_count('AbCdExCeF', 'C');

-- query 99
USE ${case_db};
select regexp_count('1a 2b 14m', '\\d+');

-- query 100
-- Table-based regexp_count
USE ${case_db};
select str, regex, regexp_count(str, regex) from tsc order by str, regex;

-- query 101
-- Setup: create table tsc_invalid with invalid regex patterns
USE ${case_db};
CREATE TABLE `tsc_invalid` (
  `str` varchar(65533) NULL COMMENT "",
  `regex` varchar(65533) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`str`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`str`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

insert into tsc_invalid values
('abc123def456', '[0-9'),
('test string', '(unclosed'),
('repetition test', '?invalid'),
('valid test', 'valid');

select str, regex, regexp_count(str, regex) from tsc_invalid order by str, regex;

-- query 102
-- Invalid regex constant tests for regexp_count
-- @expect_error=Invalid regex expression
USE ${case_db};
select regexp_count('test string', '[0-9');

-- query 103
-- @expect_error=Invalid regex expression
USE ${case_db};
select regexp_count('test string', '(unclosed');

-- query 104
-- @expect_error=Invalid regex expression
USE ${case_db};
select regexp_count('test string', '?invalid');

-- query 105
USE ${case_db};
select regexp_count('test string', 'a{,}');

-- query 106
USE ${case_db};
select regexp_count(NULL, '[0-9');

-- query 107
-- @expect_error=Invalid regex expression
USE ${case_db};
select regexp_count('', '[0-9');

-- query 108
USE ${case_db};
select regexp_count('test', NULL);

-- query 109
-- Setup: create table for multi-row regexp_replace correctness test (issue #65936)
USE ${case_db};
CREATE TABLE `test_regexp_replace_multirow` (
  `column_value` varchar(100) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`column_value`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`column_value`) BUCKETS 1 PROPERTIES ("replication_num" = "1");

insert into test_regexp_replace_multirow values
('activity_60_package_2'),
('activity_50_package_7'),
('activity_70_package_1'),
('nomatch_value'),
('test_package_suffix');

SELECT regexp_replace(column_value, '_package_.*', '')
FROM (VALUES
    ('activity_60_package_2'),
    ('activity_50_package_7')
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
USE ${case_db};
SELECT regexp_replace(column_value, '_package_', '_pkg_')
FROM test_regexp_replace_multirow
ORDER BY column_value;

-- query 113
-- @skip_result_check=true
USE ${case_db};
DROP TABLE test_regexp_replace_multirow;
