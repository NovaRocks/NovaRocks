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

-- name: test_decimal256_string_functions
-- Tests: String conversion of decimal256, CONCAT, LENGTH, SUBSTR, REGEXP operations

-- Create test table with Iceberg-compatible decimal types
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal_string_test (
    id INT,
    d50_15 DECIMAL(38,15),
    d76_20 DECIMAL(38,20),
    d76_0 DECIMAL(38,0),
    category VARCHAR(10)
) WITH (format_version = 3);

-- Insert test data with various decimal patterns - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_string_test VALUES ROW(1, DECIMAL '12345678901234567890123.123456789012345', DECIMAL '123456789012345678.12345678901234567890', CAST('12345678901234567890123456789012345678' AS DECIMAL(38, 0)), 'A'), ROW(2, (- (DECIMAL '98765432109876543210987.456789012345678')), (- (DECIMAL '987654321098765432.45678901234567890123')), CAST('-98765432109876543210987654321098765432' AS DECIMAL(38, 0)), 'B'), ROW(3, DECIMAL '0.000000000000001', DECIMAL '0.00000000000000000001', 0, 'C'), ROW(4, DECIMAL '99999888887777766666555.999999999999999', DECIMAL '999998888877777666.99999999999999999999', CAST('99999888887777766666555554444433333222' AS DECIMAL(38, 0)), 'A'), ROW(5, (- (DECIMAL '88888777776666655555444.888888888888888')), (- (DECIMAL '888887777766666555.88888888888888888888')), CAST('-88888777776666655555444443333322222111' AS DECIMAL(38, 0)), 'B'), ROW(6, DECIMAL '11111222223333344444555.111111111111111', DECIMAL '111112222233333444.11111111111111111111', CAST('11111222223333344444555556666677777888' AS DECIMAL(38, 0)), 'C'), ROW(7, (- (DECIMAL '77777666665555544444333.777777777777777')), (- (DECIMAL '777776666655555444.77777777777777777777')), CAST('-77777666665555544444333332222211111000' AS DECIMAL(38, 0)), 'A');

-- query 2
SELECT
    'Test1_BASIC_STRING_CONVERSION' as test_name,
    id,
    d50_15,
    CAST(d50_15 AS VARCHAR) as d50_as_string,
    CONCAT('Value: ', CAST(d50_15 AS VARCHAR)) as formatted_d50,
    CONCAT('D50=', CAST(d50_15 AS VARCHAR), ', D76=', CAST(d76_20 AS VARCHAR)) as combined_string
FROM ${case_db}.decimal_string_test
ORDER BY id;

-- query 3
SELECT
    'Test2_STRING_LENGTH_TRUNCATION' as test_name,
    id,
    d76_0,
    CAST(d76_0 AS VARCHAR) as d76_string,
    LENGTH(CAST(d76_0 AS VARCHAR)) as string_length,
    SUBSTR(CAST(d76_0 AS VARCHAR), 1, 10) as left_10_chars,
    SUBSTR(CAST(d76_0 AS VARCHAR), GREATEST(1, LENGTH(CAST(d76_0 AS VARCHAR)) - 9), 10) as right_10_chars,
    SUBSTR(CAST(d76_0 AS VARCHAR), 1, 20) as first_20_chars
FROM ${case_db}.decimal_string_test
ORDER BY id;

-- query 4
SELECT
    'Test3_STRING_PATTERN_MATCHING' as test_name,
    id,
    d50_15,
    CAST(d50_15 AS VARCHAR) as decimal_string,
    STRPOS(CAST(d50_15 AS VARCHAR), '.') as decimal_point_position,
    STRPOS(CAST(d50_15 AS VARCHAR), '-') as negative_sign_position,
    CASE WHEN CAST(d50_15 AS VARCHAR) LIKE '%-%.%' THEN 'NEGATIVE_DECIMAL'
         WHEN CAST(d50_15 AS VARCHAR) LIKE '%.%' THEN 'POSITIVE_DECIMAL'
         WHEN CAST(d50_15 AS VARCHAR) LIKE '-%' THEN 'NEGATIVE_INTEGER'
         ELSE 'POSITIVE_INTEGER'
    END as number_type
FROM ${case_db}.decimal_string_test
ORDER BY id;

-- query 5
SELECT
    'Test4_REGEX_OPERATIONS' as test_name,
    id,
    d76_20,
    CAST(d76_20 AS VARCHAR) as decimal_string,
    REGEXP_EXTRACT(CAST(d76_20 AS VARCHAR), '^(-?[0-9]+)', 1) as integer_part,
    REGEXP_EXTRACT(CAST(d76_20 AS VARCHAR), '\.([0-9]+)\z', 1) as decimal_part,
    REGEXP_REPLACE(CAST(d76_20 AS VARCHAR), '0+\z', '') as trimmed_trailing_zeros,
    REGEXP_EXTRACT(CAST(d76_20 AS VARCHAR), '^(-?[0-9]+\.[0-9]?[0-9]?[0-9]?[0-9]?[0-9]?)', 1) as first_5_decimal_places
FROM ${case_db}.decimal_string_test
WHERE d76_20 != 0
ORDER BY id;

-- query 6
SELECT
    'Test5_STRING_SPLITTING' as test_name,
    id,
    d50_15,
    CAST(d50_15 AS VARCHAR) as original_string,
    SPLIT_PART(CAST(ABS(d50_15) AS VARCHAR), '.', 1) as integer_part_split,
    SPLIT_PART(CAST(ABS(d50_15) AS VARCHAR), '.', 2) as decimal_part_split,
    CASE WHEN d50_15 < 0 THEN '-' ELSE '+' END as sign_part
FROM ${case_db}.decimal_string_test
ORDER BY id;

-- query 7
SELECT
    'Test6_STRING_FORMATTING' as test_name,
    id,
    d76_0,
    CAST(d76_0 AS VARCHAR) as original_string,
    LPAD(CAST(ABS(d76_0) AS VARCHAR), 80, '0') as zero_padded,
    RPAD(CAST(d76_0 AS VARCHAR), 20, ' ') as space_padded
FROM ${case_db}.decimal_string_test
ORDER BY id;

-- query 8
SELECT
    'Test7_CASE_SPECIAL_FORMATTING' as test_name,
    id,
    CAST(category AS VARCHAR(10)) as category,
    d50_15,
    UPPER(CONCAT(category, '_', CAST(d50_15 AS VARCHAR))) as upper_formatted,
    LOWER(CONCAT(category, '_', CAST(d50_15 AS VARCHAR))) as lower_formatted
FROM ${case_db}.decimal_string_test
ORDER BY id;

-- query 9
SELECT
    'Test8_COMPLEX_STRING_OPERATIONS' as test_name,
    id,
    d50_15,
    d76_20,
    d76_0,
    CONCAT_WS(' | ',
        CONCAT('D50: ', CAST(d50_15 AS VARCHAR)),
        CONCAT('D76_20: ', CAST(d76_20 AS VARCHAR)),
        CONCAT('D76_0: ', CAST(d76_0 AS VARCHAR))
    ) as pipe_separated,
    REPLACE(
        REPLACE(CAST(d50_15 AS VARCHAR), '.', '_DOT_'),
        '-', '_MINUS_'
    ) as symbol_replaced,
    REVERSE(CAST(ABS(d50_15) AS VARCHAR)) as reversed_string
FROM ${case_db}.decimal_string_test
ORDER BY id;
