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
-- 1. Validate ARRAY<DECIMAL256> element access and cardinality operations.
-- 2. Validate MAP with DECIMAL256 values and keys (basic ops, large values, decimal keys).
-- 3. Validate STRUCT with DECIMAL256 fields including nested structs.
-- 4. Validate complex nested structures (ARRAY<STRUCT<..., MAP<...>>>, MAP<STRING, ARRAY<DECIMAL256>>).
-- Migrated from dev/test/sql/test_decimal/T/test_decimal256_complex_types.sql
-- @no_arrow_flight_sql

-- =============================================================================
-- Part 1: ARRAY with decimal256 tests
-- =============================================================================

-- Create table with ARRAY<decimal256> columns
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal_array_test (
    id INT,
    decimal_array_50 ARRAY<DECIMAL(38,15)>,
    decimal_array_76 ARRAY<DECIMAL(38,20)>,
    simple_decimals ARRAY<DECIMAL(38,10)>
)
TBLPROPERTIES ("format-version" = "3");

-- Insert test data for arrays - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_array_test VALUES
(1, [12345678901234567890123.123456789012345, 98765432109876543210987.456789012345678, 55555555555555555555555.789012345678901],
    [123456789012345678.12345678901234567890, 987654321098765432.45678901234567890123, 555555555555555555.78901234567890123456],
    [1234567890123456789012345678.1234567890, 9876543210987654321098765432.2345678901, 5555555555555555555555555555.3456789012]),
(2, [77777777777777777777777.999999999999999, -44444444444444444444444.123456789012345, 0.000000000000001],
    [777777777777777777.99999999999999999999, -444444444444444444.12345678901234567890, 0.00000000000000000001],
    [7777777777777777777777777777.9999999999, -4444444444444444444444444444.1234567890, 0.0000000001]),
(3, [99999888887777766666555.999999999999999],
    [999998888877777666.99999999999999999999],
    [9999988888777776666655555444.9999999999]),
(4, [], [], []),
(5, [0.000000000000001, -0.000000000000001, 11111111111111111111111111111111111.0, -22222222222222222222222222222222222.0],
    [0.00000000000000000001, -0.00000000000000000001, 1111111111111111111111111111111111111.0, -2222222222222222222222222222222222222.0],
    [0.0000000001, -0.0000000001, 11111111111111111111111111111.0, -22222222222222222222222222222.0]);

-- =============================================================================
-- Part 2: MAP with decimal256 tests
-- =============================================================================

-- Create table with MAP<string, decimal256> columns
CREATE TABLE ${case_db}.decimal_map_test (
    id INT,
    decimal_map_50 MAP<STRING, DECIMAL(38,15)>,
    decimal_map_76 MAP<STRING, DECIMAL(38,0)>,
    key_decimal_map MAP<DECIMAL(38,10), STRING>
)
TBLPROPERTIES ("format-version" = "3");

-- Insert test data for maps - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_map_test VALUES
(1, MAP{'price': 12345678901234567890123.123456789012345, 'cost': 98765432109876543210987.456789012345678, 'profit': 55555555555555555555555.666666666666666},
    MAP{'large_num1': CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)), 'large_num2': CAST('98765432109876543210987654321098765432' AS DECIMAL(38,0))},
    MAP{1234567890123456789012345678.1234567890: 'huge_ten', 9876543210987654321098765432.2345678901: 'huge_twenty', 5555555555555555555555555555.3456789012: 'huge_thirty'}),
(2, MAP{'balance': -77777777777777777777777.999999999999999, 'limit': 88888888888888888888888.000000000000000, 'available': 44444444444444444444444.000000000000000},
    MAP{'negative': CAST('-99999888887777766666555554444433333222' AS DECIMAL(38,0)), 'zero': 0, 'positive': CAST('88888777776666655555444443333322222111' AS DECIMAL(38,0))},
    MAP{-7777777777777777777777777777.9876543210: 'huge_negative', 0.0000000000: 'zero', 8888888888888888888888888888.1111111111: 'huge_positive'}),
(3, MAP{},
    MAP{},
    MAP{}),
(4, MAP{'small': 0.000000000000001, 'tiny': 0.000000000000000},
    MAP{'one': 1, 'max': CAST('99999888887777766666555554444433333222' AS DECIMAL(38,0))},
    MAP{0.0000000001: 'very_small', 9999988888777776666655555444.9999999999: 'very_large'});

-- =============================================================================
-- Part 3: STRUCT with decimal256 tests
-- =============================================================================

-- Create table with STRUCT containing decimal256 fields
CREATE TABLE ${case_db}.decimal_struct_test (
    id INT,
    financial_data STRUCT<
        balance DECIMAL(38,15),
        credit_limit DECIMAL(38,15),
        interest_rate DECIMAL(38,10)
    >,
    large_numbers STRUCT<
        max_value DECIMAL(38,0),
        min_value DECIMAL(38,0),
        precision_value DECIMAL(38,38)
    >,
    account_info STRUCT<
        account_id BIGINT,
        balance DECIMAL(38,15),
        metadata STRUCT<
            created_date STRING,
            last_transaction DECIMAL(38,10)
        >
    >
)
TBLPROPERTIES ("format-version" = "3");

-- Insert test data for structs - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_struct_test VALUES
(1, ROW(12345678901234567890123.123456789012345, 98765432109876543210987.000000000000000, 7777777777777777777777777777.5000000000),
    ROW(CAST('99999888887777766666555554444433333222' AS DECIMAL(38,0)), CAST('-88888777776666655555444443333322222111' AS DECIMAL(38,0)), 0.45678901234567890123456789012345678901),
    ROW(12345, 77777777777777777777777.750000000000000, ROW('2024-01-01', 5555555555555555555555555555.2500000000))),
(2, ROW(-44444444444444444444444.999999999999999, 66666666666666666666666.000000000000000, 3333333333333333333333333333.9999999999),
    ROW(CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)), 0, -0.99999999999999999999999999999999999999),
    ROW(67890, -22222222222222222222222.250000000000000, ROW('2024-02-15', -1111111111111111111111111111.7500000000))),
(3, ROW(0.000000000000001, 0.000000000000000, 0.0000000001),
    ROW(1, -1, 0.00000000000000000000000000000000000001),
    ROW(11111, 0.000000000000001, ROW('2024-03-30', 0.0100000000))),
(4, NULL,
    NULL,
    ROW(99999, 88888777776666655555444.999999999999999, ROW('2024-12-31', 7777766666555554444433333222.9999999999)));

-- =============================================================================
-- Part 4: Complex nested structures
-- =============================================================================

-- Create table with nested complex types containing decimal256
CREATE TABLE ${case_db}.complex_nested_test (
    id INT,
    portfolio ARRAY<STRUCT<
        asset_name STRING,
        quantity DECIMAL(38,15),
        price DECIMAL(38,15),
        metadata MAP<STRING, DECIMAL(38,10)>
    >>,
    risk_metrics MAP<STRING, ARRAY<DECIMAL(38,20)>>
)
TBLPROPERTIES ("format-version" = "3");

-- Insert test data for complex nested structures - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.complex_nested_test VALUES
(1, [
        ROW('STOCK_A', 12345678901234567890123.500000000000000, 98765432109876543210987.750000000000000, MAP{'daily_change': 7777777777777777777777777777.2500000000, 'volume_weight': 3333333333333333333333333333.8500000000}),
        ROW('BOND_B', 55555555555555555555555.000000000000000, 44444444444444444444444.250000000000000, MAP{'yield': 1111111111111111111111111111.7500000000, 'duration': 2222222222222222222222222222.2500000000})
    ],
    MAP{
        'volatility': [123456789012345678.15000000000000000000, 987654321098765432.18000000000000000000, 555555555555555555.12000000000000000000],
        'correlation': [777777777777777777.65000000000000000000, -444444444444444444.25000000000000000000, 888888888888888888.85000000000000000000]
    }),
(2, [
        ROW('CRYPTO_C', 11111111111111111111111.001500000000000, 88888888888888888888888.999999999999999, MAP{'market_cap': 9999988888777776666655555444.9999999999, 'circulating': 7777766666555554444433333222.0000000000})
    ],
    MAP{
        'beta': [666666666666666666.25000000000000000000, 333333333333333333.35000000000000000000],
        'sharpe': [222222222222222222.75000000000000000000, 111111111111111111.95000000000000000000, 999999999999999999.15000000000000000000]
    });

-- query 2
-- Test 1: Basic array operations with decimal256
SELECT
    'Test1_ARRAY_BASIC_OPERATIONS' as test_name,
    id,
    decimal_array_50,
    CARDINALITY(decimal_array_50) as array_size,
    decimal_array_50[1] as first_element,
    decimal_array_50[CARDINALITY(decimal_array_50)] as last_element
FROM ${case_db}.decimal_array_test
ORDER BY id;

-- query 3
-- Test 2: Array element access
SELECT
    'Test2_ARRAY_ELEMENT_ACCESS' as test_name,
    id,
    simple_decimals,
    simple_decimals[1] as first_decimal,
    simple_decimals[2] as second_decimal,
    simple_decimals[3] as third_decimal
FROM ${case_db}.decimal_array_test
WHERE CARDINALITY(simple_decimals) >= 2
ORDER BY id;

-- query 4
-- Test 4: Basic map operations with decimal256
SELECT
    'Test4_MAP_BASIC_OPERATIONS' as test_name,
    id,
    decimal_map_50,
    MAP_SIZE(decimal_map_50) as map_size,
    decimal_map_50['price'] as price_value,
    MAP_KEYS(decimal_map_50) as all_keys,
    MAP_VALUES(decimal_map_50) as all_values
FROM ${case_db}.decimal_map_test
ORDER BY id;

-- query 5
-- Test 5: Map operations with large decimal256 values
SELECT
    'Test5_MAP_LARGE_DECIMALS' as test_name,
    id,
    decimal_map_76,
    MAP_SIZE(decimal_map_76) as map_size,
    decimal_map_76['large_num1'] as large_value1,
    decimal_map_76['negative'] as negative_value
FROM ${case_db}.decimal_map_test
WHERE MAP_SIZE(decimal_map_76) > 0
ORDER BY id;

-- query 6
-- Test 6: Map with decimal keys
SELECT
    'Test6_MAP_DECIMAL_KEYS' as test_name,
    id,
    key_decimal_map,
    MAP_SIZE(key_decimal_map) as map_size,
    key_decimal_map[1234567890123456789012345678.1234567890] as value_for_key,
    MAP_KEYS(key_decimal_map) as decimal_keys
FROM ${case_db}.decimal_map_test
WHERE MAP_SIZE(key_decimal_map) > 0
ORDER BY id;

-- query 7
-- Test 7: Basic struct field access
SELECT
    'Test7_STRUCT_FIELD_ACCESS' as test_name,
    id,
    financial_data,
    financial_data.balance as balance,
    financial_data.credit_limit as credit_limit,
    financial_data.interest_rate as interest_rate,
    financial_data.balance + financial_data.credit_limit as total_available
FROM ${case_db}.decimal_struct_test
WHERE financial_data IS NOT NULL
ORDER BY id;

-- query 8
-- Test 8: Nested struct operations
SELECT
    'Test8_NESTED_STRUCT_OPERATIONS' as test_name,
    id,
    account_info,
    account_info.account_id as account_id,
    account_info.balance as account_balance,
    account_info.metadata.created_date as created_date,
    account_info.metadata.last_transaction as last_transaction,
    account_info.balance - account_info.metadata.last_transaction as net_balance
FROM ${case_db}.decimal_struct_test
WHERE account_info IS NOT NULL
ORDER BY id;

-- query 9
-- Test 9: Large decimal256 values in structs
SELECT
    'Test9_LARGE_DECIMAL_STRUCTS' as test_name,
    id,
    large_numbers,
    large_numbers.max_value as max_val,
    large_numbers.min_value as min_val,
    large_numbers.precision_value as precision_val,
    CASE
        WHEN large_numbers.max_value > 0 AND large_numbers.min_value < 0 THEN 'MIXED_RANGE'
        WHEN large_numbers.max_value > 0 THEN 'POSITIVE_RANGE'
        ELSE 'OTHER'
    END as range_type
FROM ${case_db}.decimal_struct_test
WHERE large_numbers IS NOT NULL
ORDER BY id;

-- query 10
-- Test 10: Complex nested structure operations
SELECT
    'Test10_COMPLEX_NESTED_OPERATIONS' as test_name,
    id,
    CARDINALITY(portfolio) as portfolio_size,
    portfolio[1].asset_name as first_asset,
    portfolio[1].quantity as first_quantity,
    portfolio[1].price as first_price,
    portfolio[1].quantity * portfolio[1].price as first_total_value,
    portfolio[1].metadata['daily_change'] as first_daily_change
FROM ${case_db}.complex_nested_test
ORDER BY id;

-- query 11
-- Test 11: Risk metrics analysis
SELECT
    'Test11_RISK_METRICS_ANALYSIS' as test_name,
    id,
    MAP_SIZE(risk_metrics) as metrics_count,
    CARDINALITY(risk_metrics['volatility']) as volatility_points,
    risk_metrics['volatility'][1] as first_volatility
FROM ${case_db}.complex_nested_test
WHERE risk_metrics['volatility'] IS NOT NULL
ORDER BY id;

-- query 12
-- Test 12: Array element operations on nested structures
SELECT
    'Test12_ARRAY_ELEMENT_OPERATIONS' as test_name,
    id,
    portfolio[1].metadata as first_asset_metadata,
    MAP_KEYS(portfolio[1].metadata) as metadata_keys,
    MAP_VALUES(portfolio[1].metadata) as metadata_values
FROM ${case_db}.complex_nested_test
WHERE CARDINALITY(portfolio) > 0
ORDER BY id;
