-- Test Objective:
-- 1. Verify HLL and BITMAP columns accept DEFAULT "" (empty string) in CREATE TABLE and ALTER TABLE.
-- 2. Verify non-empty default values are rejected for HLL and BITMAP columns.
-- 3. Compare behavior of HLL/BITMAP columns with and without DEFAULT.

-- query 1
-- Test 1: BITMAP with DEFAULT "" in CREATE TABLE
USE ${case_db};
CREATE TABLE test_bitmap_create (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_bitmap_create (id) VALUES (1);
INSERT INTO test_bitmap_create (id) VALUES (2);
INSERT INTO test_bitmap_create VALUES (3, to_bitmap(100));
SELECT id, bitmap_to_string(bm), bitmap_count(bm) FROM test_bitmap_create ORDER BY id;

-- query 2
-- Test 2: HLL with DEFAULT "" in CREATE TABLE
USE ${case_db};
CREATE TABLE test_hll_create (
    id INT,
    h HLL HLL_UNION DEFAULT ""
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_hll_create (id) VALUES (1);
INSERT INTO test_hll_create (id) VALUES (2);
INSERT INTO test_hll_create VALUES (3, hll_hash(100));
SELECT id, hll_cardinality(h) FROM test_hll_create ORDER BY id;

-- query 3
-- Test 3: ALTER TABLE ADD COLUMN with BITMAP DEFAULT (Fast Schema Evolution)
USE ${case_db};
CREATE TABLE test_bitmap_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_bitmap_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE test_bitmap_alter ADD COLUMN bm BITMAP BITMAP_UNION DEFAULT "";
SET @a = sleep(2);
SELECT id, name, bitmap_to_string(bm), bitmap_count(bm) FROM test_bitmap_alter ORDER BY id;

-- query 4
-- Test 4: ALTER TABLE ADD COLUMN with HLL DEFAULT (Fast Schema Evolution)
USE ${case_db};
CREATE TABLE test_hll_alter (
    id INT,
    name VARCHAR(50)
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_hll_alter VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
ALTER TABLE test_hll_alter ADD COLUMN h HLL HLL_UNION DEFAULT "";
SET @a = sleep(2);
SELECT id, name, hll_cardinality(h) FROM test_hll_alter ORDER BY id;

-- query 5
-- Test 5: Verify non-empty default value is rejected for BITMAP
-- @expect_error=Invalid default value
USE ${case_db};
CREATE TABLE test_bitmap_nonempty (
    id INT,
    bm BITMAP BITMAP_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- query 6
-- Test 5 continued: Verify non-empty default value is rejected for HLL
-- @expect_error=Invalid default value
USE ${case_db};
CREATE TABLE test_hll_nonempty (
    id INT,
    h HLL HLL_UNION DEFAULT "test"
) AGGREGATE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");

-- query 7
-- Test 6: Compare behavior with and without DEFAULT for BITMAP
USE ${case_db};
CREATE TABLE test_bitmap_with_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION DEFAULT ""
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
CREATE TABLE test_bitmap_without_default (
    id INT,
    name VARCHAR(50),
    bm BITMAP BITMAP_UNION
) AGGREGATE KEY(id, name)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES("replication_num" = "1");
INSERT INTO test_bitmap_with_default (id, name) VALUES (1, 'alice');
INSERT INTO test_bitmap_without_default (id, name) VALUES (1, 'alice');
SELECT 'with_default' as type, id, name, bitmap_count(bm) FROM test_bitmap_with_default
UNION ALL
SELECT 'without_default', id, name, bitmap_count(bm) FROM test_bitmap_without_default
ORDER BY type, id;
