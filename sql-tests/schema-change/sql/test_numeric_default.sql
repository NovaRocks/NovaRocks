-- Test Objective:
-- 1. Verify numeric type default values work with Fast Schema Evolution (ALTER TABLE ADD COLUMN).
-- 2. Verify numeric defaults work with Traditional Schema Change.
-- 3. Verify numeric defaults work with PRIMARY KEY partial updates (column mode and row mode).
-- 4. Cover edge cases: boundary values, negative, zero, all numeric types.
-- 5. Cover AGGREGATE and UNIQUE key tables with numeric defaults.

-- query 1
-- Test 1: Fast Schema Evolution - add multiple numeric columns with defaults
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
ALTER TABLE users_basic ADD COLUMN age TINYINT DEFAULT '25';
ALTER TABLE users_basic ADD COLUMN score SMALLINT DEFAULT '100';
ALTER TABLE users_basic ADD COLUMN salary INT DEFAULT '50000';
ALTER TABLE users_basic ADD COLUMN revenue BIGINT DEFAULT '1000000';
ALTER TABLE users_basic ADD COLUMN rating FLOAT DEFAULT '4.5';
ALTER TABLE users_basic ADD COLUMN percentage DOUBLE DEFAULT '95.5';
SELECT * FROM users_basic ORDER BY id;

-- query 2
-- Test 1 continued: Verify values are correct
USE ${case_db};
SELECT
    id,
    CASE WHEN age = 25 THEN 'PASS' ELSE 'FAIL' END as test_age,
    CASE WHEN score = 100 THEN 'PASS' ELSE 'FAIL' END as test_score,
    CASE WHEN salary = 50000 THEN 'PASS' ELSE 'FAIL' END as test_salary,
    CASE WHEN revenue = 1000000 THEN 'PASS' ELSE 'FAIL' END as test_revenue,
    CASE WHEN abs(rating - 4.5) < 0.01 THEN 'PASS' ELSE 'FAIL' END as test_rating,
    CASE WHEN abs(percentage - 95.5) < 0.01 THEN 'PASS' ELSE 'FAIL' END as test_percentage
FROM users_basic
ORDER BY id;

-- query 3
-- Test 1 continued: Mixed old and new data
USE ${case_db};
INSERT INTO users_basic VALUES (4, 'david', 30, 200, 60000, 2000000, 3.8, 88.9);
SELECT * FROM users_basic ORDER BY id;

-- query 4
-- Test 2: Traditional Schema Change
USE ${case_db};
CREATE TABLE products_with_key (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);
INSERT INTO products_with_key VALUES (1, 'product1'), (2, 'product2'), (3, 'product3');
ALTER TABLE products_with_key ADD COLUMN price1 DOUBLE DEFAULT '99.99';
SET @a = sleep(3);
SELECT COUNT(*) FROM products_with_key;

-- query 5
-- Test 3: Column UPSERT Mode (Primary Key with column mode)
USE ${case_db};
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    quantity INT DEFAULT '1',
    price DOUBLE DEFAULT '0.0',
    discount FLOAT DEFAULT '0.0',
    amount BIGINT DEFAULT '0'
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
SET partial_update_mode = 'column';
INSERT INTO orders_column_mode (order_id, product_name) VALUES (1, 'laptop');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (2, 'phone');
INSERT INTO orders_column_mode (order_id, product_name) VALUES (3, 'tablet');
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 6
-- Test 3 continued: Insert with some columns specified
USE ${case_db};
INSERT INTO orders_column_mode (order_id, product_name, quantity) VALUES (4, 'monitor', 2);
INSERT INTO orders_column_mode (order_id, product_name, price) VALUES (5, 'keyboard', 299.99);
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 7
-- Test 3 continued: Add new column and insert more rows
USE ${case_db};
ALTER TABLE orders_column_mode ADD COLUMN tax_rate DOUBLE DEFAULT '0.08';
SET @a = sleep(5);
INSERT INTO orders_column_mode (order_id, product_name) VALUES (6, 'mouse');
INSERT INTO orders_column_mode (order_id, product_name, quantity) VALUES (7, 'headset', 3);
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 8
-- Reset partial_update_mode to auto
USE ${case_db};
SET partial_update_mode = 'auto';
SELECT 1;

-- query 9
-- Test 4: Primary Key Partial Update (general case) - insert with partial columns
USE ${case_db};
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    age TINYINT DEFAULT '18',
    score INT DEFAULT '0',
    balance BIGINT DEFAULT '1000'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 10
-- Test 4 continued: Partial update
USE ${case_db};
INSERT INTO users_pk_table (user_id, username, score) VALUES (1, 'alice_updated', 100);
INSERT INTO users_pk_table (user_id, username, age) VALUES (2, 'bob_updated', 25);
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 11
-- Test 4 continued: Add new numeric column and insert
USE ${case_db};
ALTER TABLE users_pk_table ADD COLUMN credit_limit BIGINT DEFAULT '5000';
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, credit_limit) VALUES (1, 'alice_v2', 10000);
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 12
-- Test 4 continued: UPDATE with DEFAULT keyword
USE ${case_db};
UPDATE users_pk_table SET age = DEFAULT, score = DEFAULT WHERE user_id = 3;
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 13
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
ALTER TABLE event_logs ADD COLUMN event_count INT DEFAULT '1';
SELECT * FROM event_logs ORDER BY log_id;

-- query 14
-- Test 5 continued: Insert partial and read all
USE ${case_db};
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
SELECT * FROM event_logs ORDER BY log_id;

-- query 15
-- Edge Cases: Boundary numeric values
USE ${case_db};
CREATE TABLE edge_case_numerics (
    id INT NOT NULL,
    tiny_val TINYINT DEFAULT '127',
    small_val SMALLINT DEFAULT '32767',
    int_val INT DEFAULT '2147483647',
    big_val BIGINT DEFAULT '9223372036854775807',
    float_val FLOAT DEFAULT '3.14159',
    double_val DOUBLE DEFAULT '2.718281828',
    zero_val INT DEFAULT '0',
    negative_val INT DEFAULT '-100'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO edge_case_numerics (id) VALUES (1), (2), (3);
SELECT * FROM edge_case_numerics ORDER BY id;

-- query 16
-- Edge Cases continued: Verify all boundary values
USE ${case_db};
SELECT
    id,
    CASE WHEN tiny_val = 127 THEN 'PASS' ELSE 'FAIL' END as test_tiny,
    CASE WHEN small_val = 32767 THEN 'PASS' ELSE 'FAIL' END as test_small,
    CASE WHEN int_val = 2147483647 THEN 'PASS' ELSE 'FAIL' END as test_int,
    CASE WHEN big_val = 9223372036854775807 THEN 'PASS' ELSE 'FAIL' END as test_big,
    CASE WHEN abs(float_val - 3.14159) < 0.001 THEN 'PASS' ELSE 'FAIL' END as test_float,
    CASE WHEN abs(double_val - 2.718281828) < 0.000001 THEN 'PASS' ELSE 'FAIL' END as test_double,
    CASE WHEN zero_val = 0 THEN 'PASS' ELSE 'FAIL' END as test_zero,
    CASE WHEN negative_val = -100 THEN 'PASS' ELSE 'FAIL' END as test_negative
FROM edge_case_numerics
ORDER BY id;

-- query 17
-- Test 6: Aggregate Table with Numeric Defaults
USE ${case_db};
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    total_quantity BIGINT SUM DEFAULT '0',
    total_revenue DOUBLE SUM DEFAULT '0.0',
    max_price DOUBLE MAX DEFAULT '0.0',
    min_price DOUBLE MIN DEFAULT '999999.99'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN avg_discount DOUBLE REPLACE DEFAULT '0.05';
SELECT * FROM sales_summary ORDER BY product_id, region;

-- query 18
-- Test 7: Unique Key Table with Numeric Defaults
USE ${case_db};
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    stock_quantity INT DEFAULT '100',
    reorder_point INT DEFAULT '20'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN max_stock INT DEFAULT '1000';
SELECT * FROM inventory_items ORDER BY item_id;
