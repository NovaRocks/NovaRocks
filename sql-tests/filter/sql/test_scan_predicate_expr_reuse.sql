-- Test Objective:
-- 1. Verify scan predicate expression reuse optimization produces correct results.
-- 2. Verify correctness with enable_scan_predicate_expr_reuse enabled (default) and disabled.
-- 3. Cover AND/OR predicates, column-vs-column comparisons, lambda expressions (array_map).
-- 4. Cover complex types: array subscript, struct field access, nested struct, map_keys, JSON path.
-- 5. Cover cardinality() with expression reuse across multiple predicate arms.

-- query 1
-- Basic predicate reuse with DUPLICATE KEY table
USE ${case_db};
CREATE TABLE t (
    v1 BIGINT NOT NULL,
    v2 BIGINT NULL,
    v3 BIGINT NULL,
    v4 ARRAY<STRING> NULL
) DUPLICATE KEY(v1)
DISTRIBUTED BY RANDOM
PROPERTIES("replication_num" = "1");
INSERT INTO t SELECT generate_series, generate_series, generate_series,
    array_repeat(CAST(generate_series AS STRING), 5)
FROM TABLE(generate_series(1, 100));
SELECT * FROM t WHERE v1 = 10 AND v2 = v3;

-- query 2
USE ${case_db};
SELECT * FROM t WHERE v1 = 10 OR v2 = v3 ORDER BY v1 LIMIT 3;

-- query 3
USE ${case_db};
SELECT * FROM t WHERE v1 = v3 AND (v2 = 5 OR v3 = 10) ORDER BY v1;

-- query 4
-- Lambda expression in predicate
USE ${case_db};
SELECT * FROM t WHERE v1 < 10 AND array_max(array_map(x->length(x) + v2, v4)) > 0 ORDER BY v1;

-- query 5
-- Same queries with enable_scan_predicate_expr_reuse disabled
USE ${case_db};
SET enable_scan_predicate_expr_reuse = false;
SELECT * FROM t WHERE v1 = 10 AND v2 = v3;

-- query 6
USE ${case_db};
SELECT * FROM t WHERE v1 = 10 OR v2 = v3 ORDER BY v1 LIMIT 3;

-- query 7
USE ${case_db};
SELECT * FROM t WHERE v1 = v3 AND (v2 = 5 OR v3 = 10) ORDER BY v1;

-- query 8
USE ${case_db};
SELECT * FROM t WHERE v1 < 10 AND array_max(array_map(x->length(x) + v2, v4)) > 0 ORDER BY v1;

-- query 9
-- Complex types: PRIMARY KEY table with arrays, structs, maps, JSON
USE ${case_db};
SET enable_scan_predicate_expr_reuse = true;
CREATE TABLE t0 (
    k BIGINT NOT NULL,
    v1 ARRAY<BIGINT> NULL,
    v2 ARRAY<BIGINT> NULL,
    v3 ARRAY<BIGINT> NULL,
    v4 STRUCT<a INT, b STRUCT<a INT>> NULL,
    v5 STRUCT<a INT, b STRUCT<a ARRAY<BIGINT>>> NULL,
    v6 MAP<INT, INT> NULL,
    v7 MAP<INT, INT> NULL,
    v8 JSON NULL,
    v9 JSON NULL
) PRIMARY KEY(k)
DISTRIBUTED BY HASH(k) BUCKETS 3
PROPERTIES("replication_num" = "1");
INSERT INTO t0 VALUES
(1,[1],[1],[1],row(1,row(1)),row(1,row([1])),map{1:1},map{1:1,2:2},parse_json('{"a":{"b":1}}'),parse_json('{"a":1,"b":[1]}')),
(2,[2],[2],[2],row(2,row(2)),row(2,row([2])),map{1:1},map{1:2,2:2},parse_json('{"a":{"b":2}}'),parse_json('{"a":1,"b":[1]}')),
(3,[3],[3],[3],row(3,row(3)),row(3,row([3])),map{1:1},map{1:3,2:2},parse_json('{"a":{"b":3}}'),parse_json('{"a":1,"b":[1]}')),
(4,[4],[4],[4],row(4,row(4)),row(4,row([4])),map{1:1},map{1:4,2:2},parse_json('{"a":{"b":4}}'),parse_json('{"a":1,"b":[1]}')),
(5,[5],[5],[5],row(5,row(5)),row(5,row([5])),map{1:1},map{1:5,2:2},parse_json('{"a":{"b":5}}'),parse_json('{"a":1,"b":[1]}'));
SELECT k FROM t0 WHERE v1[1] = v2[1] ORDER BY k;

-- query 10
USE ${case_db};
SELECT k FROM t0 WHERE v1[1] = v2[1] ORDER BY k LIMIT 1;

-- query 11
USE ${case_db};
SELECT k, v1 FROM t0 WHERE v1[1] = v2[1] ORDER BY k;

-- query 12
USE ${case_db};
SELECT k, v2 FROM t0 WHERE v1[1] = v2[1] ORDER BY k;

-- query 13
USE ${case_db};
SELECT k FROM t0 WHERE array_length(v1) = array_length(v2) ORDER BY k;

-- query 14
-- Struct nested field access in predicate
USE ${case_db};
SELECT k FROM t0 WHERE array_length(v1) = array_length(v5.b.a) ORDER BY k;

-- query 15
USE ${case_db};
SELECT k, v5 FROM t0 WHERE array_length(v1) = array_length(v5.b.a) ORDER BY k;

-- query 16
USE ${case_db};
SELECT k, v5.a FROM t0 WHERE array_length(v1) = array_length(v5.b.a) ORDER BY k;

-- query 17
-- Lambda with struct field and column reference
USE ${case_db};
SELECT k FROM t0 WHERE array_max(array_map(x->x+k, v5.b.a)) > 0 ORDER BY k;

-- query 18
-- Multi-arg lambda with struct, map_keys, and array
USE ${case_db};
SELECT k FROM t0 WHERE array_max(array_map((x,y,z)->x+y+z, v5.b.a, map_keys(v6), v1)) > 0 ORDER BY k;

-- query 19
USE ${case_db};
SELECT k, v5.a FROM t0 WHERE array_max(array_map((x,y,z)->x+y+z, v5.b.a, map_keys(v6), v1)) > 0 ORDER BY k;

-- query 20
-- JSON path access in predicate
USE ${case_db};
SELECT k FROM t0 WHERE v8->'$.a.b' = v9->'$.a' ORDER BY k;

-- query 21
USE ${case_db};
SELECT k, v9->'$.b' FROM t0 WHERE v8->'$.a.b' = v9->'$.a' ORDER BY k;

-- query 22
-- Cardinality with expression reuse across AND arms
USE ${case_db};
SELECT k FROM t0 WHERE cardinality(v1) + cardinality(v5.b.a) > 0
    AND cardinality(v1) + cardinality(v5.b.a) + cardinality(v6) > 0 ORDER BY k;
