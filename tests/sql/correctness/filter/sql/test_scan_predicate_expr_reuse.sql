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
    v2 BIGINT ,
    v3 BIGINT ,
    v4 ARRAY<VARCHAR>
) WITH (format_version = 3);
INSERT INTO t SELECT generate_series, generate_series, generate_series,
    repeat(CAST(generate_series AS VARCHAR), 5)
FROM TABLE(sequence(1, 100)) AS generate_series(generate_series);
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
SELECT * FROM t WHERE v1 < 10 AND array_max(transform(v4, x->length(x) + v2)) > 0 ORDER BY v1;

-- query 5
-- Same queries with enable_scan_predicate_expr_reuse disabled
USE ${case_db};
SELECT * FROM t WHERE v1 = 10 AND v2 = v3;

-- query 6
USE ${case_db};
SELECT * FROM t WHERE v1 = 10 OR v2 = v3 ORDER BY v1 LIMIT 3;

-- query 7
USE ${case_db};
SELECT * FROM t WHERE v1 = v3 AND (v2 = 5 OR v3 = 10) ORDER BY v1;

-- query 8
USE ${case_db};
SELECT * FROM t WHERE v1 < 10 AND array_max(transform(v4, x->length(x) + v2)) > 0 ORDER BY v1;

-- query 9
-- Complex types: PRIMARY KEY table with arrays, structs, maps, JSON
USE ${case_db};
CREATE TABLE t0 (
    k BIGINT NOT NULL,
    v1 ARRAY<BIGINT> ,
    v2 ARRAY<BIGINT> ,
    v3 ARRAY<BIGINT> ,
    v4 ROW(a INT, b ROW(a INT)) ,
    v5 ROW(a INT, b ROW(a ARRAY<BIGINT>)) ,
    v6 MAP<INT, INT> ,
    v7 MAP<INT, INT> ,
    v8 VARCHAR ,
    v9 VARCHAR
) WITH (format_version = 3);
INSERT INTO t0 VALUES
ROW(1,[1],[1],[1],row(1,row(1)),row(1,row([1])),MAP(ARRAY[1], ARRAY[1]),MAP(ARRAY[1, 2], ARRAY[1, 2]),json_parse('{"a":{"b":1}}'),json_parse('{"a":1,"b":[1]}')),
ROW(2,[2],[2],[2],row(2,row(2)),row(2,row([2])),MAP(ARRAY[1], ARRAY[1]),MAP(ARRAY[1, 2], ARRAY[2, 2]),json_parse('{"a":{"b":2}}'),json_parse('{"a":1,"b":[1]}')),
ROW(3,[3],[3],[3],row(3,row(3)),row(3,row([3])),MAP(ARRAY[1], ARRAY[1]),MAP(ARRAY[1, 2], ARRAY[3, 2]),json_parse('{"a":{"b":3}}'),json_parse('{"a":1,"b":[1]}')),
ROW(4,[4],[4],[4],row(4,row(4)),row(4,row([4])),MAP(ARRAY[1], ARRAY[1]),MAP(ARRAY[1, 2], ARRAY[4, 2]),json_parse('{"a":{"b":4}}'),json_parse('{"a":1,"b":[1]}')),
ROW(5,[5],[5],[5],row(5,row(5)),row(5,row([5])),MAP(ARRAY[1], ARRAY[1]),MAP(ARRAY[1, 2], ARRAY[5, 2]),json_parse('{"a":{"b":5}}'),json_parse('{"a":1,"b":[1]}'));
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
SELECT k FROM t0 WHERE cardinality(v1) = cardinality(v2) ORDER BY k;

-- query 14
-- Struct nested field access in predicate
USE ${case_db};
SELECT k FROM t0 WHERE cardinality(v1) = cardinality(v5.b.a) ORDER BY k;

-- query 15
USE ${case_db};
SELECT k, v5 FROM t0 WHERE cardinality(v1) = cardinality(v5.b.a) ORDER BY k;

-- query 16
USE ${case_db};
SELECT k, v5.a FROM t0 WHERE cardinality(v1) = cardinality(v5.b.a) ORDER BY k;

-- query 17
-- Lambda with struct field and column reference
USE ${case_db};
SELECT k FROM t0 WHERE array_max(transform(v5.b.a, x->x+k)) > 0 ORDER BY k;

-- query 18
-- Multi-arg lambda with struct, map_keys, and array
USE ${case_db};
SELECT k FROM t0 WHERE array_max(transform(zip(v5.b.a, map_keys(v6), v1), inputs -> inputs[1]+inputs[2]+inputs[3])) > 0 ORDER BY k;

-- query 19
USE ${case_db};
SELECT k, v5.a FROM t0 WHERE array_max(transform(zip(v5.b.a, map_keys(v6), v1), inputs -> inputs[1]+inputs[2]+inputs[3])) > 0 ORDER BY k;

-- query 20
-- JSON path access in predicate
USE ${case_db};
SELECT k FROM t0 WHERE JSON_QUERY(v8, '$.a.b') = JSON_QUERY(v9, '$.a') ORDER BY k;

-- query 21
USE ${case_db};
SELECT k, JSON_QUERY(v9, '$.b') FROM t0 WHERE JSON_QUERY(v8, '$.a.b') = JSON_QUERY(v9, '$.a') ORDER BY k;

-- query 22
-- Cardinality with expression reuse across AND arms
USE ${case_db};
SELECT k FROM t0 WHERE cardinality(v1) + cardinality(v5.b.a) > 0
    AND cardinality(v1) + cardinality(v5.b.a) + cardinality(v6) > 0 ORDER BY k;
