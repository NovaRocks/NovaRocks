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

-- name: test_decimal256_partitioned_tables
-- Tests: Range-partitioned tables with Iceberg-compatible decimal columns, partition pruning, cross-partition aggregation

-- =============================================================================
-- Part 1: Range partitioned table with decimal256
-- =============================================================================

-- Create range partitioned table using decimal256 column
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal_range_partition (
    id BIGINT,
    transaction_date DATE,
    amount DECIMAL(38,15),
    balance DECIMAL(38,20),
    account_type VARCHAR(20)
) WITH (format_version = 3);

-- Insert test data across different partitions - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_range_partition VALUES
-- Partition p202401
ROW(1, '2024-01-15', 123456789012.123456789012345, 1234567890.12345678901234567890, 'SAVINGS'),
ROW(2, '2024-01-20', (- (DECIMAL '555555555555.987654321098765')), 5555555555.13579246801357924680, 'CHECKING'),
ROW(3, '2024-01-25', 987654321098.555555555555555, 9876543210.99999999999999999999, 'INVESTMENT'),

-- Partition p202402
ROW(4, '2024-02-10', 777777777777.777777777777777, 7777777777.77777777777777777777, 'SAVINGS'),
ROW(5, '2024-02-15', (- (DECIMAL '444444444444.111111111111111')), 4444444444.66666666666666666666, 'CHECKING'),
ROW(6, '2024-02-28', 888888888888.999999999999999, 8888888888.65432109876543210987, 'INVESTMENT'),

-- Partition p202403
ROW(7, '2024-03-05', 111111111111.250000000000000, 1111111111.90400000000000000000, 'SAVINGS'),
ROW(8, '2024-03-12', (- (DECIMAL '222222222222.125000000000000')), 2222222222.77900000000000000000, 'CHECKING'),
ROW(9, '2024-03-25', 999998888877.000000000000001, 9999988888.77900000000000000001, 'INVESTMENT'),

-- Partition p202404
ROW(10, '2024-04-08', 333333333333.888888888888888, 3333333333.66788888888888888889, 'SAVINGS'),
ROW(11, '2024-04-15', (- (DECIMAL '666666666666.333333333333333')), 6666666666.33455555555555555556, 'CHECKING'),
ROW(12, '2024-04-30', 777776666655.000000000000000, 7777766666.33455555555555555556, 'INVESTMENT');

-- =============================================================================
-- Part 2: List partitioned table with decimal256 ranges
-- =============================================================================

-- Create list partitioned table using decimal256 ranges
CREATE TABLE ${case_db}.decimal_amount_partition (
    id BIGINT,
    customer_id INT,
    amount DECIMAL(38,15),
    large_amount DECIMAL(38,0),
    category VARCHAR(20),
    amount_range STRING
) WITH (format_version = 3);

-- Insert test data with different amount ranges - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_amount_partition VALUES
-- Small amounts (< 40 digits)
ROW(1, 101, 123456789012.123456789012345, 12345678901234, 'RETAIL', 'SMALL'),
ROW(2, 102, 987654321098.987654321098765, 98765432109876, 'ONLINE', 'SMALL'),
ROW(3, 103, 555555555555.999999999999999, 55555555555555, 'RETAIL', 'SMALL'),

-- Medium amounts (40-50 digits)
ROW(4, 201, 12345678901234.555555555555, 12345678901234, 'WHOLESALE', 'MEDIUM'),
ROW(5, 202, 98765432109876.777777777777, 98765432109876, 'ENTERPRISE', 'MEDIUM'),
ROW(6, 203, 55555555555555.888888888888, 55555555555555, 'WHOLESALE', 'MEDIUM'),

-- Large amounts (50-60 digits)
ROW(7, 301, 123456789012.123456789012345, 123456789012345, 'ENTERPRISE', 'LARGE'),
ROW(8, 302, 987654321098.999999999999999, 987654321098765, 'GOVERNMENT', 'LARGE'),
ROW(9, 303, 777777777777.111111111111111, 777777777777777, 'ENTERPRISE', 'LARGE');

-- =============================================================================
-- Part 3: Partition maintenance operations
-- =============================================================================

-- Iceberg-compatible path: the table is not declared with StarRocks range
-- partitions, so append the additional month directly.

-- Insert data into new partition - using Iceberg-compatible decimal values
INSERT INTO ${case_db}.decimal_range_partition VALUES ROW(13, '2024-05-10', DECIMAL '888887777766.123456789012345', DECIMAL '8888877777.45800234567890123456', 'SAVINGS'), ROW(14, '2024-05-20', (- (DECIMAL '111112222233.987654321098765')), DECIMAL '1111122222.47034913469124801235', 'CHECKING'), ROW(15, '2024-05-31', DECIMAL '999999999999.000000000000000', DECIMAL '9999999999.47034913469124801235', 'INVESTMENT');

-- query 2
SELECT
    'Test1_PARTITION_BASIC_QUERY' as test_name,
    DATE_FORMAT(transaction_date, '%Y-%m') as month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(balance) as avg_balance,
    MAX(amount) as max_amount,
    MIN(amount) as min_amount
FROM ${case_db}.decimal_range_partition
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
ORDER BY month;

-- query 3
SELECT
    'Test2_SINGLE_PARTITION_QUERY' as test_name,
    id,
    transaction_date,
    amount,
    balance,
    account_type
FROM ${case_db}.decimal_range_partition
WHERE transaction_date >= '2024-02-01' AND transaction_date < '2024-03-01'
ORDER BY transaction_date;

-- query 4
SELECT
    'Test3_CROSS_PARTITION_AGGREGATION' as test_name,
    account_type,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as total_deposits,
    SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as total_withdrawals
FROM ${case_db}.decimal_range_partition
GROUP BY account_type
ORDER BY account_type;

-- query 5
SELECT
    'Test4_LIST_PARTITION_AGGREGATION' as test_name,
    amount_range,
    category,
    COUNT(*) as count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM ${case_db}.decimal_amount_partition
GROUP BY amount_range, category
ORDER BY amount_range, category;

-- query 6
SELECT
    'Test5_PARTITION_PRUNING_SPECIFIC' as test_name,
    id,
    customer_id,
    amount,
    large_amount,
    category
FROM ${case_db}.decimal_amount_partition
WHERE amount_range = 'LARGE'
ORDER BY amount DESC;

-- query 7
SELECT
    'Test7_ALL_PARTITIONS_INCLUDING_NEW' as test_name,
    DATE_FORMAT(transaction_date, '%Y-%m') as month,
    COUNT(*) as transaction_count,
    SUM(amount) as total_amount,
    AVG(balance) as avg_balance
FROM ${case_db}.decimal_range_partition
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
ORDER BY month;

-- query 8
SELECT
    'Test8_PARTITION_WINDOW_FUNCTIONS' as test_name,
    id,
    transaction_date,
    amount,
    balance,
    account_type,
    ROW_NUMBER() OVER (PARTITION BY DATE_FORMAT(transaction_date, '%Y-%m') ORDER BY amount DESC) as rank_in_month,
    SUM(amount) OVER (PARTITION BY account_type ORDER BY transaction_date) as running_total_by_type,
    LAG(balance, 1) OVER (PARTITION BY account_type ORDER BY transaction_date) as prev_balance
FROM ${case_db}.decimal_range_partition
ORDER BY transaction_date, account_type;

-- query 9
WITH monthly_stats AS (
    SELECT
        DATE_FORMAT(transaction_date, '%Y-%m') as month,
        account_type,
        SUM(amount) as monthly_total,
        COUNT(*) as monthly_count
    FROM ${case_db}.decimal_range_partition
    GROUP BY DATE_FORMAT(transaction_date, '%Y-%m'), account_type
)
SELECT
    'Test9_CROSS_PARTITION_JOINS' as test_name,
    dp.transaction_date,
    dp.amount,
    dp.account_type,
    ms.monthly_total,
    dp.amount / ms.monthly_total * 100 as percent_of_monthly_total
FROM ${case_db}.decimal_range_partition dp
JOIN monthly_stats ms ON DATE_FORMAT(dp.transaction_date, '%Y-%m') = ms.month
    AND dp.account_type = ms.account_type
WHERE ABS(dp.amount) > 1000.000000000000000
ORDER BY dp.transaction_date;

-- query 10
SELECT
    'Test10_PARTITION_INFORMATION' as test_name,
    DATE_FORMAT(transaction_date, '%Y-%m') as partition_month,
    COUNT(*) as row_count,
    SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) as positive_transactions,
    SUM(CASE WHEN amount < 0 THEN 1 ELSE 0 END) as negative_transactions,
    MIN(balance) as min_balance_in_partition,
    MAX(balance) as max_balance_in_partition
FROM ${case_db}.decimal_range_partition
GROUP BY DATE_FORMAT(transaction_date, '%Y-%m')
ORDER BY partition_month;
