-- Migrated from dev/test/sql/test_function/T/test_split
-- Test Objective:
-- 1. Validate split() with constant source and delimiter strings.
-- 2. Validate behavior with empty delimiter (splits into individual characters).
-- 3. Validate behavior with multi-byte (Chinese) characters in both source and delimiter.
-- 4. Validate behavior when delimiter is not found in the source string.

-- query 1
select split('测隔试隔试', '');

-- query 2
select split('测隔试隔试', '隔');

-- query 3
select split('测隔试隔试', 'a');

-- query 4
select split('测abc隔试隔试', '');

-- query 5
select split('测abc隔试隔试', '隔');

-- query 6
select split('测abc隔试abc隔试', 'a');

-- query 7
select split('a|b|c|d', '');

-- query 8
select split('a|b|c|d', '|');

-- query 9
select split('a|b|c|d', '隔');
