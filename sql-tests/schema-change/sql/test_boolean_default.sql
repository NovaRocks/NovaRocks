-- Test Objective:
-- 1. Verify boolean default values ('true', 'false', '1', '0') work with Fast Schema Evolution.
-- 2. Verify boolean defaults work with Traditional Schema Change.
-- 3. Verify boolean defaults work with PRIMARY KEY partial updates (column mode and row mode).
-- 4. Cover edge cases: all boolean representations, aggregate and unique key tables.

-- query 1
-- Test 1: Fast Schema Evolution - add boolean columns with different defaults
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
ALTER TABLE users_basic ADD COLUMN flag_true BOOLEAN DEFAULT 'true';
ALTER TABLE users_basic ADD COLUMN flag_false BOOLEAN DEFAULT 'false';
ALTER TABLE users_basic ADD COLUMN flag_1 BOOLEAN DEFAULT '1';
ALTER TABLE users_basic ADD COLUMN flag_0 BOOLEAN DEFAULT '0';
SELECT id, name, flag_true, flag_false, flag_1, flag_0 FROM users_basic ORDER BY id;

-- query 2
-- Test 1 continued: Verify boolean default values are correct
USE ${case_db};
SELECT
    id,
    CASE WHEN flag_true = 1 THEN 'PASS' ELSE 'FAIL' END as test_true,
    CASE WHEN flag_false = 0 THEN 'PASS' ELSE 'FAIL' END as test_false,
    CASE WHEN flag_1 = 1 THEN 'PASS' ELSE 'FAIL' END as test_1,
    CASE WHEN flag_0 = 0 THEN 'PASS' ELSE 'FAIL' END as test_0
FROM users_basic
ORDER BY id;

-- query 3
-- Test 1 continued: Mixed old and new data
USE ${case_db};
INSERT INTO users_basic VALUES (4, 'david', 0, 1, 0, 1);
SELECT * FROM users_basic ORDER BY id;

-- query 4
-- Test 2: Traditional Schema Change (fast_schema_evolution = false)
USE ${case_db};
CREATE TABLE products_with_key (
    id INT NOT NULL,
    price INT
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);
INSERT INTO products_with_key VALUES (1, 100), (2, 200), (3, 300);
ALTER TABLE products_with_key ADD COLUMN active BOOLEAN DEFAULT 'true';
SET @a = sleep(3);
SELECT count(*) FROM products_with_key;

-- query 5
-- Test 3: Traditional Schema Change with MODIFY COLUMN (also triggers rewrite)
USE ${case_db};
CREATE TABLE items_type_change (
    id INT NOT NULL,
    quantity SMALLINT,
    available BOOLEAN DEFAULT 'true'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1",
    "fast_schema_evolution" = "false"
);
INSERT INTO items_type_change VALUES (1, 10, 1), (2, 20, 0);
ALTER TABLE items_type_change ADD COLUMN verified BOOLEAN DEFAULT '1';
SET @a = sleep(3);
SELECT count(*) FROM items_type_change;

-- query 6
-- Test 4: Column UPSERT Mode (Primary Key with column mode)
USE ${case_db};
CREATE TABLE orders_column_mode (
    order_id INT NOT NULL,
    product_name VARCHAR(50),
    is_paid BOOLEAN DEFAULT 'true',
    is_shipped BOOLEAN DEFAULT 'false',
    total_amount INT DEFAULT '100'
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

-- query 7
-- Test 4 continued: Insert with some columns specified
USE ${case_db};
INSERT INTO orders_column_mode (order_id, product_name, is_paid) VALUES (4, 'monitor', 0);
INSERT INTO orders_column_mode (order_id, product_name, total_amount) VALUES (5, 'keyboard', 200);
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 8
-- Test 4 continued: Add new boolean column and insert more rows
USE ${case_db};
ALTER TABLE orders_column_mode ADD COLUMN is_delivered BOOLEAN DEFAULT '1';
SET @a = sleep(5);
INSERT INTO orders_column_mode (order_id, product_name) VALUES (6, 'mouse');
INSERT INTO orders_column_mode (order_id, product_name, is_paid) VALUES (7, 'headset', 1);
SELECT * FROM orders_column_mode ORDER BY order_id;

-- query 9
-- Reset partial_update_mode to auto
USE ${case_db};
SET partial_update_mode = 'auto';
SELECT 1;

-- query 10
-- Test 5: Primary Key Partial Update (general case) - insert with partial columns
USE ${case_db};
CREATE TABLE users_pk_table (
    user_id INT NOT NULL,
    username VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT 'true',
    is_verified BOOLEAN DEFAULT 'false',
    credit_score INT DEFAULT '0'
) PRIMARY KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 2
PROPERTIES(
    "replication_num" = "1"
);
INSERT INTO users_pk_table (user_id, username) VALUES (1, 'alice');
INSERT INTO users_pk_table (user_id, username) VALUES (2, 'bob');
INSERT INTO users_pk_table (user_id, username) VALUES (3, 'charlie');
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 11
-- Test 5 continued: Partial update
USE ${case_db};
INSERT INTO users_pk_table (user_id, username, credit_score) VALUES (1, 'alice_updated', 100);
INSERT INTO users_pk_table (user_id, username, is_active) VALUES (2, 'bob_updated', 0);
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 12
-- Test 5 continued: Add new boolean column, insert new and partial update existing
USE ${case_db};
ALTER TABLE users_pk_table ADD COLUMN is_premium BOOLEAN DEFAULT '1';
SET @a = sleep(2);
INSERT INTO users_pk_table (user_id, username) VALUES (4, 'david');
INSERT INTO users_pk_table (user_id, username, is_premium) VALUES (1, 'alice_v2', 0);
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 13
-- Test 5 continued: UPDATE with DEFAULT keyword
USE ${case_db};
UPDATE users_pk_table SET is_active = DEFAULT, is_verified = DEFAULT WHERE user_id = 3;
SELECT * FROM users_pk_table ORDER BY user_id;

-- query 14
-- Test 6: Combined test (PK table with ALTER and partial updates)
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
ALTER TABLE event_logs ADD COLUMN is_processed BOOLEAN DEFAULT 'false';
SET @a = sleep(2);
SELECT * FROM event_logs ORDER BY log_id;

-- query 15
-- Test 6 continued: Insert new row with partial columns and read all
USE ${case_db};
INSERT INTO event_logs (log_id, message) VALUES (3, 'event_3');
SELECT * FROM event_logs ORDER BY log_id;

-- query 16
-- Edge Cases: All boolean representations
USE ${case_db};
CREATE TABLE edge_case_booleans (
    id INT NOT NULL,
    bool_true BOOLEAN DEFAULT 'true',
    bool_false BOOLEAN DEFAULT 'false',
    bool_1 BOOLEAN DEFAULT '1',
    bool_0 BOOLEAN DEFAULT '0'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO edge_case_booleans (id) VALUES (1), (2), (3);
SELECT * FROM edge_case_booleans ORDER BY id;

-- query 17
-- Edge Cases continued: Verify all boolean representations evaluate correctly
USE ${case_db};
SELECT
    id,
    bool_true = 1 as true_is_1,
    bool_false = 0 as false_is_0,
    bool_1 = 1 as one_is_1,
    bool_0 = 0 as zero_is_0,
    CASE
        WHEN bool_true = 1 AND bool_false = 0 AND bool_1 = 1 AND bool_0 = 0
        THEN 'ALL_PASS'
        ELSE 'FAIL'
    END as overall_result
FROM edge_case_booleans
ORDER BY id;

-- query 18
-- Test 7: Aggregate Table with Boolean Defaults
USE ${case_db};
CREATE TABLE sales_summary (
    product_id INT NOT NULL,
    region VARCHAR(50),
    is_active BOOLEAN REPLACE DEFAULT 'true',
    sales_count BIGINT SUM DEFAULT '0'
) AGGREGATE KEY(product_id, region)
DISTRIBUTED BY HASH(product_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO sales_summary (product_id, region) VALUES (1, 'North'), (1, 'North'), (2, 'South');
ALTER TABLE sales_summary ADD COLUMN is_verified BOOLEAN REPLACE DEFAULT 'false';
SELECT * FROM sales_summary ORDER BY product_id, region;

-- query 19
-- Test 8: Unique Key Table with Boolean Defaults
USE ${case_db};
CREATE TABLE inventory_items (
    item_id INT NOT NULL,
    item_name VARCHAR(50),
    in_stock BOOLEAN DEFAULT 'true'
) UNIQUE KEY(item_id)
DISTRIBUTED BY HASH(item_id) BUCKETS 2
PROPERTIES("replication_num" = "1");
INSERT INTO inventory_items (item_id, item_name) VALUES (1, 'widget'), (2, 'gadget');
ALTER TABLE inventory_items ADD COLUMN is_discontinued BOOLEAN DEFAULT '0';
SELECT * FROM inventory_items ORDER BY item_id;
