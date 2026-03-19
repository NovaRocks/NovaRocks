-- Migrated from: dev/test/sql/test_map/T/test_map (case: testMapTopN)
-- Test Objective:
-- 1. Validate MAP subscript access via [] operator and element_at() on literals and table columns.
-- 2. Validate ORDER BY on a MAP subscript value with LIMIT/OFFSET.
-- 3. Cover NULL-value lookup, empty-map lookup, column-indexed map literal, and null-key access.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.map_top_n (
    c1 INT,
    c2 MAP<VARCHAR(8), INT>
) PRIMARY KEY(c1)
DISTRIBUTED BY HASH(c1) BUCKETS 1
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.map_top_n VALUES
    (1, map{"key1":1}),
    (2, map{"key1":5, "key2":6}),
    (3, map{"key1":2, "key2":3, "key4":4}),
    (4, map{"key1":12, "key2":13, "key3":14, "key4":15}),
    (5, map{"key1":7, "key2":8, "key3":9, "key4":10, "key5":11});

-- query 2
-- Full result ordered by map subscript value
SELECT * FROM ${case_db}.map_top_n ORDER BY c2["key1"];

-- query 3
-- Skip 1, take rest
SELECT * FROM ${case_db}.map_top_n ORDER BY c2["key1"] LIMIT 1, 10;

-- query 4
-- Skip 2, take rest
SELECT * FROM ${case_db}.map_top_n ORDER BY c2["key1"] LIMIT 2, 10;

-- query 5
-- Skip 3, take rest
SELECT * FROM ${case_db}.map_top_n ORDER BY c2["key1"] LIMIT 3, 10;

-- query 6
-- Skip 4, take rest (last row only)
SELECT * FROM ${case_db}.map_top_n ORDER BY c2["key1"] LIMIT 4, 10;

-- query 7
-- Skip 5 → empty result
SELECT * FROM ${case_db}.map_top_n ORDER BY c2["key1"] LIMIT 5, 10;

-- query 8
-- element_at() on a map literal
SELECT element_at(map{'a':1,'b':2}, 'a');

-- query 9
-- [] subscript on a map literal
SELECT map{'a':1,'b':2}['a'];

-- query 10
-- Null-value lookup: key exists but maps to NULL
SELECT map{'a':NULL,'b':2}['a'];

-- query 11
-- Empty map lookup → NULL
SELECT map{}['a'];

-- query 12
-- Subscript access on a table column; order by pk for determinism
SELECT c2["key1"] FROM ${case_db}.map_top_n ORDER BY c1;

-- query 13
-- Map literal indexed by row column: c1=1→"a", c1=2→"b", c1>=3→NULL
SELECT map{1: "a", 2: "b"}[c1] FROM ${case_db}.map_top_n ORDER BY c1;

-- query 14
-- Null-key map access: map{null:"a",...}[null] → "a"
SELECT map{null: "a", 2: "b"}[null];
