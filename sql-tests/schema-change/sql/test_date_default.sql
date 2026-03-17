-- Test Objective:
-- 1. Verify DATE and DATETIME default values work with Fast Schema Evolution.
-- 2. Verify date defaults work with Traditional Schema Change.
-- 3. Verify date defaults work with PRIMARY KEY partial updates (column mode and row mode).
-- 4. Cover edge cases: various date formats and boundary date values.
-- 5. Cover AGGREGATE and UNIQUE key tables with date defaults.

-- query 1
-- Test 1: Fast Schema Evolution - add date/datetime columns with different defaults
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
ALTER TABLE users_basic ADD COLUMN birth_date DATE DEFAULT '2000-01-01';
ALTER TABLE users_basic ADD COLUMN created_at DATETIME DEFAULT '2024-01-01 00:00:00';
ALTER TABLE users_basic ADD COLUMN updated_at DATETIME DEFAULT '2024-12-17 10:00:00';
SELECT * FROM users_basic ORDER BY id;

-- query 2
-- Test 1 continued: Verify date default values are correct
USE ${case_db};
SELECT
    id,
    CASE WHEN birth_date = '2000-01-01' THEN 'PASS' ELSE 'FAIL' END as test_date,
    CASE WHEN created_at = '2024-01-01 00:00:00' THEN 'PASS' ELSE 'FAIL' END as test_datetime1,
    CASE WHEN updated_at = '2024-12-17 10:00:00' THEN 'PASS' ELSE 'FAIL' END as test_datetime2
FROM users_basic
ORDER BY id;

-- query 3
-- Test 1 continued: Mixed old and new data
USE ${case_db};
INSERT INTO users_basic VALUES (4, 'david', '1995-05-15', '2024-06-01 12:30:00', '2024-12-18 15:45:30');
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
ALTER TABLE products_with_key
    ADD COLUMN launch_date DATE DEFAULT '2024-01-01',
    ADD COLUMN last_updated DATETIME DEFAULT '2024-12-17 00:00:00';
SET @a = sleep(3);
SELECT count(*) FROM products_with_key;

-- query 5
-- Test 3: Column UPSERT Mode (Primary Key with column mode)
USE ${case_db};
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    customer_name VARCHAR(50),
    order_date DATE DEFAULT '2024-01-01',
    shipped_date DATE DEFAULT '2024-01-02',
    created_at DATETIME DEFAULT '2024-01-01 00:00:00',
    updated_at DATETIME DEFAULT '2024-01-01 00:00:00'
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

-- query 6
-- Test 3 continued: Insert with some columns specified
USE ${case_db};
INSERT INTO orders_column_mode (order_id, customer_name, order_date) VALUES (4, 'david', '2024-06-15');
INSERT INTO orders_column_mode (order_id, customer_name, created_at) VALUES (5, 'eve', '2024-07-20 10:30:00');
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 7
-- Test 3 continued: Add new column and insert more rows
USE ${case_db};
ALTER TABLE orders_column_mode ADD COLUMN delivery_date DATE DEFAULT '2024-01-10';
INSERT INTO orders_column_mode (order_id, customer_name) VALUES (6, 'frank');
INSERT INTO orders_column_mode (order_id, customer_name, order_date) VALUES (7, 'grace', '2024-08-01');
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
    registered_date DATE DEFAULT '2024-01-01',
    last_login DATETIME DEFAULT '2024-01-01 00:00:00',
    birth_date DATE DEFAULT '2000-01-01'
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
INSERT INTO users_pk_table (user_id, username, registered_date) VALUES (1, 'alice_updated', '2024-06-01');
INSERT INTO users_pk_table (user_id, username, last_login) VALUES (2, 'bob_updated', '2024-12-17 10:30:00');
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 11
-- Test 4 continued: Add new date column, insert new and partial update existing
USE ${case_db};
ALTER TABLE users_pk_table ADD COLUMN account_expires DATE DEFAULT '2025-12-31';
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, account_expires) VALUES (1, 'alice_v2', '2026-06-30');
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 12
-- Test 4 continued: UPDATE with DEFAULT keyword
USE ${case_db};
UPDATE users_pk_table SET registered_date = DEFAULT, last_login = DEFAULT WHERE user_id = 3;
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
ALTER TABLE event_logs ADD COLUMN event_date DATE DEFAULT '2024-01-01';
SELECT * FROM event_logs ORDER BY log_id;

-- query 14
-- Test 5 continued: Insert new row with partial columns and read all
USE ${case_db};
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
SELECT * FROM event_logs ORDER BY log_id;

-- query 15
-- Edge Cases: Different date formats and boundary values
USE ${case_db};
CREATE TABLE edge_case_dates (
    id INT NOT NULL,
    date_early DATE DEFAULT '1970-01-01',
    date_recent DATE DEFAULT '2024-12-17',
    datetime_midnight DATETIME DEFAULT '2024-01-01 00:00:00',
    datetime_noon DATETIME DEFAULT '2024-06-15 12:00:00',
    datetime_full DATETIME DEFAULT '2024-12-17 23:59:59'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO edge_case_dates (id) VALUES (1), (2), (3);
SELECT * FROM edge_case_dates ORDER BY id;

-- query 16
-- Edge Cases continued: Verify all date values evaluate correctly
USE ${case_db};
SELECT
    id,
    CASE WHEN date_early = '1970-01-01' THEN 'PASS' ELSE 'FAIL' END as test_early,
    CASE WHEN date_recent = '2024-12-17' THEN 'PASS' ELSE 'FAIL' END as test_recent,
    CASE WHEN datetime_midnight = '2024-01-01 00:00:00' THEN 'PASS' ELSE 'FAIL' END as test_midnight,
    CASE WHEN datetime_noon = '2024-06-15 12:00:00' THEN 'PASS' ELSE 'FAIL' END as test_noon,
    CASE WHEN datetime_full = '2024-12-17 23:59:59' THEN 'PASS' ELSE 'FAIL' END as test_full
FROM edge_case_dates
ORDER BY id;

-- query 17
-- Test 6: Aggregate Table with Date Defaults
USE ${case_db};
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    last_sale_date DATE REPLACE DEFAULT '2024-01-01',
    total_quantity BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN first_sale_date DATE REPLACE DEFAULT '2023-01-01';
SELECT * FROM sales_summary ORDER BY product_id, region;

-- query 18
-- Test 7: Unique Key Table with Date Defaults
USE ${case_db};
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    added_date DATE DEFAULT '2024-01-01',
    last_checked DATETIME DEFAULT '2024-01-01 00:00:00'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN expiry_date DATE DEFAULT '2025-12-31';
SELECT * FROM inventory_items ORDER BY item_id;
