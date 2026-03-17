-- Test Objective:
-- 1. Verify DECIMAL type default values work with Fast Schema Evolution.
-- 2. Verify decimal defaults work with PRIMARY KEY partial updates (column mode and row mode).
-- 3. Cover edge cases: different decimal precisions/scales, zero, negative, Decimal256.
-- 4. Cover AGGREGATE and UNIQUE key tables with decimal defaults.

-- query 1
-- Test 1: Fast Schema Evolution - add decimal columns with different defaults
USE ${case_db};
CREATE TABLE users_basic (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_basic VALUES
    (1, 'alice'),
    (2, 'bob'),
    (3, 'charlie');
ALTER TABLE users_basic ADD COLUMN balance DECIMAL(10, 2) DEFAULT '1000.50';
ALTER TABLE users_basic ADD COLUMN interest_rate DECIMAL(5, 4) DEFAULT '0.0525';
ALTER TABLE users_basic ADD COLUMN score DECIMAL(8, 3) DEFAULT '85.125';
ALTER TABLE users_basic ADD COLUMN percentage DECIMAL(5, 2) DEFAULT '99.99';
SELECT * FROM users_basic ORDER BY id;

-- query 2
-- Test 1 continued: Verify decimal default values are correct
USE ${case_db};
SELECT
    id,
    CASE WHEN balance = 1000.50 THEN 'PASS' ELSE 'FAIL' END as test_balance,
    CASE WHEN interest_rate = 0.0525 THEN 'PASS' ELSE 'FAIL' END as test_rate,
    CASE WHEN score = 85.125 THEN 'PASS' ELSE 'FAIL' END as test_score,
    CASE WHEN percentage = 99.99 THEN 'PASS' ELSE 'FAIL' END as test_percentage
FROM users_basic
ORDER BY id;

-- query 3
-- Test 1 continued: Mixed old and new data
USE ${case_db};
INSERT INTO users_basic VALUES (4, 'david', 2500.75, 0.0625, 92.5, 87.65);
SELECT * FROM users_basic ORDER BY id;

-- query 4
-- Test 3: Column UPSERT Mode (Primary Key with column mode)
USE ${case_db};
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    customer_name VARCHAR(50),
    unit_price DECIMAL(10, 2) DEFAULT '0.00',
    tax_rate DECIMAL(5, 4) DEFAULT '0.0800',
    discount DECIMAL(5, 2) DEFAULT '0.00',
    total_amount DECIMAL(12, 2) DEFAULT '0.00'
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
SET partial_update_mode = 'column';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (1, 'alice');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (2, 'bob');
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (3, 'charlie');
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 5
-- Test 3 continued: Insert with some columns specified
USE ${case_db};
INSERT INTO orders_column_mode (order_id, customer_name, unit_price) VALUES (4, 'david', 299.99);
INSERT INTO orders_column_mode (order_id, customer_name, tax_rate) VALUES (5, 'eve', 0.1000);
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 6
-- Test 3 continued: Add new decimal column and insert more rows
USE ${case_db};
ALTER TABLE orders_column_mode ADD COLUMN shipping_fee DECIMAL(8, 2) DEFAULT '15.50';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
INSERT INTO orders_column_mode (order_id, customer_name, unit_price) VALUES (7, 'grace', 499.99);
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 7
-- Reset partial_update_mode to auto
USE ${case_db};
SET partial_update_mode = 'auto';
SELECT 1;

-- query 8
-- Test 4: Primary Key Partial Update (general case) - insert with partial columns
USE ${case_db};
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    account_balance DECIMAL(12, 2) DEFAULT '1000.00',
    credit_limit DECIMAL(10, 2) DEFAULT '5000.00',
    interest_rate DECIMAL(5, 4) DEFAULT '0.0350'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 9
-- Test 4 continued: Partial update
USE ${case_db};
INSERT INTO users_pk_table (user_id, username, account_balance) VALUES (1, 'alice_updated', 2500.00);
INSERT INTO users_pk_table (user_id, username, credit_limit) VALUES (2, 'bob_updated', 10000.00);
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 10
-- Test 4 continued: Add new decimal column, insert new and partial update existing
USE ${case_db};
ALTER TABLE users_pk_table ADD COLUMN reward_points DECIMAL(10, 2) DEFAULT '100.00';
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, reward_points) VALUES (1, 'alice_v2', 500.00);
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 11
-- Test 4 continued: UPDATE with DEFAULT keyword
USE ${case_db};
UPDATE users_pk_table SET account_balance = DEFAULT, credit_limit = DEFAULT WHERE user_id = 3;
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 12
-- Test 5: Combined test (PK table with ALTER and partial updates)
USE ${case_db};
CREATE TABLE event_logs (
    log_id INT NOT NULL,
    message VARCHAR(100)
) PRIMARY KEY(log_id)
DISTRIBUTED BY HASH(log_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO event_logs VALUES (1, 'event_1'), (2, 'event_2');
ALTER TABLE event_logs ADD COLUMN processing_time DECIMAL(8, 3) DEFAULT '1.500';
SELECT * FROM event_logs ORDER BY log_id;

-- query 13
-- Test 5 continued: Insert new row with partial columns and read all
USE ${case_db};
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
SELECT * FROM event_logs ORDER BY log_id;

-- query 14
-- Edge Cases: Different decimal precisions and scales
USE ${case_db};
CREATE TABLE edge_case_decimals (
    id INT NOT NULL,
    dec_small DECIMAL(5, 2) DEFAULT '123.45',
    dec_medium DECIMAL(10, 4) DEFAULT '12345.6789',
    dec_large DECIMAL(18, 6) DEFAULT '123456789.123456',
    dec_zero DECIMAL(10, 2) DEFAULT '0.00',
    dec_negative DECIMAL(10, 2) DEFAULT '-999.99',
    dec_high_precision DECIMAL(38, 10) DEFAULT '12345678.1234567890',
    dec_no_fraction DECIMAL(10, 0) DEFAULT '12345'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO edge_case_decimals (id) VALUES (1), (2), (3);
SELECT * FROM edge_case_decimals ORDER BY id;

-- query 15
-- Edge Cases continued: Verify all decimal values evaluate correctly
USE ${case_db};
SELECT
    id,
    CASE WHEN dec_small = 123.45 THEN 'PASS' ELSE 'FAIL' END as test_small,
    CASE WHEN dec_medium = 12345.6789 THEN 'PASS' ELSE 'FAIL' END as test_medium,
    CASE WHEN dec_large = 123456789.123456 THEN 'PASS' ELSE 'FAIL' END as test_large,
    CASE WHEN dec_zero = 0.00 THEN 'PASS' ELSE 'FAIL' END as test_zero,
    CASE WHEN dec_negative = -999.99 THEN 'PASS' ELSE 'FAIL' END as test_negative,
    CASE WHEN dec_high_precision = 12345678.1234567890 THEN 'PASS' ELSE 'FAIL' END as test_high_precision,
    CASE WHEN dec_no_fraction = 12345 THEN 'PASS' ELSE 'FAIL' END as test_no_fraction
FROM edge_case_decimals
ORDER BY id;

-- query 16
-- Test 6: Aggregate Table with Decimal Defaults
USE ${case_db};
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    total_revenue DECIMAL(15, 2) SUM DEFAULT '0.00',
    avg_price DECIMAL(10, 2) REPLACE DEFAULT '0.00',
    max_discount DECIMAL(5, 2) MAX DEFAULT '0.00'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN avg_tax DECIMAL(5, 4) REPLACE DEFAULT '0.0800';
SELECT * FROM sales_summary ORDER BY product_id, region;

-- query 17
-- Test 7: Unique Key Table with Decimal Defaults
USE ${case_db};
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    unit_cost DECIMAL(10, 2) DEFAULT '50.00',
    selling_price DECIMAL(10, 2) DEFAULT '100.00'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN markup_rate DECIMAL(5, 2) DEFAULT '20.00';
SELECT * FROM inventory_items ORDER BY item_id;
