-- Test Objective:
-- 1. Verify VARBINARY columns accept DEFAULT "" (empty string) in CREATE TABLE and ALTER TABLE.
-- 2. Verify non-empty default values are rejected for VARBINARY columns.
-- 3. Test Fast Schema Evolution and Traditional Schema Change for VARBINARY DEFAULT.
-- 4. Test PRIMARY KEY table with VARBINARY DEFAULT and edge cases.

-- query 1
-- Test 1: CREATE TABLE with VARBINARY DEFAULT "" (DUPLICATE KEY)
USE ${case_db};
CREATE TABLE test_varbinary_create (
    id INT NOT NULL,
    name VARCHAR(50),
    binary_col VARBINARY DEFAULT "",
    binary_no_default VARBINARY
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_varbinary_create (id, name) VALUES (1, 'user1');
INSERT INTO test_varbinary_create (id, name, binary_no_default) VALUES (2, 'user2', 'custom');
INSERT INTO test_varbinary_create VALUES (3, 'user3', 'explicit', 'data');
INSERT INTO test_varbinary_create VALUES (4, 'user4', '', NULL);
SELECT
    id,
    name,
    binary_col,
    binary_col IS NULL AS col_is_null,
    HEX(binary_col),
    LENGTH(binary_col) AS col_len,
    binary_no_default,
    binary_no_default IS NULL AS no_default_is_null,
    LENGTH(binary_no_default) AS no_default_len,
    HEX(binary_col) AS col_hex
FROM test_varbinary_create
ORDER BY id;

-- query 2
-- Test 2: Fast Schema Evolution (ALTER TABLE ADD COLUMN)
USE ${case_db};
CREATE TABLE test_varbinary_alter (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "true");
INSERT INTO test_varbinary_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE test_varbinary_alter ADD COLUMN binary_col VARBINARY DEFAULT "";
SELECT
    id,
    name,
    binary_col,
    binary_col IS NULL AS is_null,
    LENGTH(binary_col) AS len
FROM test_varbinary_alter
ORDER BY id;

-- query 3
-- Test 2 continued: Insert new data and re-query
USE ${case_db};
INSERT INTO test_varbinary_alter VALUES (4, 'david', 'new_data');
SELECT
    id,
    name,
    CAST(binary_col AS VARCHAR) AS binary_str,
    LENGTH(binary_col) AS len
FROM test_varbinary_alter
ORDER BY id;

-- query 4
-- Test 3: Traditional Schema Change (fast_schema_evolution = false)
USE ${case_db};
CREATE TABLE test_varbinary_traditional (
    id INT NOT NULL,
    name VARCHAR(50)
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1", "fast_schema_evolution" = "false");
INSERT INTO test_varbinary_traditional VALUES (1, 'eve'), (2, 'frank');
ALTER TABLE test_varbinary_traditional ADD COLUMN binary_col VARBINARY DEFAULT "";
SET @a = sleep(3);
SELECT count(*) FROM test_varbinary_traditional;

-- query 5
-- Test 4: PRIMARY KEY table with VARBINARY DEFAULT
USE ${case_db};
CREATE TABLE test_varbinary_primary (
    order_id INT NOT NULL,
    customer VARCHAR(50),
    signature VARBINARY DEFAULT ""
) PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_varbinary_primary (order_id, customer) VALUES (1, 'customer1');
INSERT INTO test_varbinary_primary VALUES (2, 'customer2', 'sign123');
INSERT INTO test_varbinary_primary VALUES (3, 'customer3', '');
SELECT
    order_id,
    customer,
    CAST(signature AS VARCHAR) AS sign_str,
    LENGTH(signature) AS sign_len,
    signature IS NULL AS is_null
FROM test_varbinary_primary
ORDER BY order_id;

-- query 6
-- Test 5: Edge Cases (omit, explicit NULL, empty string)
USE ${case_db};
CREATE TABLE test_varbinary_edge (
    id INT NOT NULL,
    binary_col VARBINARY DEFAULT ""
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_varbinary_edge (id) VALUES (1);
INSERT INTO test_varbinary_edge VALUES (2, NULL);
INSERT INTO test_varbinary_edge VALUES (3, '');
SELECT
    id,
    binary_col IS NULL AS is_null,
    LENGTH(binary_col) AS len,
    HEX(binary_col) AS hex,
    CAST(binary_col AS VARCHAR) AS str
FROM test_varbinary_edge
ORDER BY id;

-- query 7
-- Test 6: Verify non-empty default is rejected (non-empty string)
-- @expect_error=Invalid default value
USE ${case_db};
CREATE TABLE test_varbinary_invalid1 (
    id INT,
    binary_col VARBINARY DEFAULT "test"
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- query 8
-- Test 6 continued: Verify non-empty default is rejected (hex literal)
-- @expect_error=Invalid default value
USE ${case_db};
CREATE TABLE test_varbinary_invalid2 (
    id INT,
    binary_col VARBINARY DEFAULT x'CAFE'
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- query 9
-- Test 7: VARBINARY with size constraints
USE ${case_db};
CREATE TABLE test_varbinary_sized (
    id INT NOT NULL,
    small_binary VARBINARY(10) DEFAULT "",
    large_binary VARBINARY(100) DEFAULT ""
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_varbinary_sized (id) VALUES (1);
INSERT INTO test_varbinary_sized VALUES (2, 'short', 'this_is_longer');
SELECT
    id,
    CAST(small_binary AS VARCHAR) AS small_val,
    LENGTH(small_binary) AS small_len,
    CAST(large_binary AS VARCHAR) AS large_val,
    LENGTH(large_binary) AS large_len
FROM test_varbinary_sized
ORDER BY id;

-- query 10
-- Test 8: Compare with and without DEFAULT
USE ${case_db};
CREATE TABLE with_default (
    id INT,
    binary_col VARBINARY DEFAULT ""
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
CREATE TABLE without_default (
    id INT,
    binary_col VARBINARY
) DUPLICATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO with_default (id) VALUES (1);
INSERT INTO without_default (id) VALUES (1);
SELECT
    'with_default' AS type,
    binary_col IS NULL AS is_null,
    LENGTH(binary_col) AS len
FROM with_default
UNION ALL
SELECT
    'without_default',
    binary_col IS NULL,
    LENGTH(binary_col)
FROM without_default;
