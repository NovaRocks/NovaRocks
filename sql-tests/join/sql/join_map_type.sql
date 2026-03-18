-- @tags=join,map
-- Test Objective:
-- Validate JOIN correctness for MAP-typed columns across all join types.
-- Covers: INNER, LEFT/RIGHT/FULL OUTER, EXISTS/NOT EXISTS, IN/NOT IN subqueries,
-- NULL-safe equal (<=>), and map_apply expressions in join predicates.
-- MAP value types include: simple (int→string), nested array, nested map, and struct.
-- Also verifies FE rejects unsupported MAP comparisons (cross-type, JSON, non-equality).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.map_test;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.map_test (
  pk BIGINT NOT NULL,
  map0 MAP<INT, STRING>,
  map1 MAP<DECIMAL(16,3), VARCHAR(30)>,
  map2 MAP<INT, ARRAY<STRING>>,
  map3 MAP<STRING, MAP<INT, VARCHAR(30)>>,
  map4 MAP<INT, JSON>,
  map5 MAP<INT, STRUCT<c INT, b STRING>>
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.map_test VALUES
  (0, map(0,'ab'), map(0,'ab'), map(0,['1','2']), map('1',map(1,'abc')), map(1,json_object('name','abc','age',23)), map(0, row(1,'a')));

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.map_test VALUES
  (1, map(1, null), map(null,''), map(1,[]), map('11',map(1,'abc'),'', map(2,null)), map(null,json_object('name',null)), map(null, row(null,null)));

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.map_test VALUES
  (2, map(null,null), map(null,null), map(null,null), map(null,map(null,null)), map(null,null), map(null, row(null,null)));

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.map_test VALUES
  (3, map(3,'',null,null), map(3,'',null,null), map(3,['3',null], null,null), map('3',map(3,'a33c'),null,null), map(null,null,1,json_object('name','abc','age',23)), map(null,null,3, row(3,'a')));

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.map_test VALUES (4, null, null, null, null, null, null);

-- ========== INNER JOIN (same type) ==========

-- query 8
-- map<int, array<string>> self join
SELECT t.map2, s.map2
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map2 = t.map2
ORDER BY t.pk;

-- query 9
-- map<string, map<int, varchar>> self join
SELECT t.map3, s.map3
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map3 = t.map3
ORDER BY t.pk;

-- query 10
-- map<int, struct> self join
SELECT t.map5, s.map5
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map5 = t.map5
ORDER BY t.pk;

-- query 11
-- map<int, string> self join (WHERE syntax)
SELECT t.map0, s.map0
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s WHERE s.map0 = t.map0
ORDER BY t.pk;

-- query 12
-- map<decimal, varchar> self join (WHERE syntax)
SELECT t.map1, s.map1
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s WHERE s.map1 = t.map1
ORDER BY t.pk;

-- ========== FE error: JSON MAP not supported in join ==========

-- query 13
-- @expect_error=not support join
SELECT t.map4, s.map4
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map4 = t.map4
ORDER BY t.pk;

-- ========== FE error: cross-type MAP comparison ==========

-- query 14
-- @expect_error=does not support binary predicate operation
SELECT t.map0, s.map5
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s WHERE s.map0 = t.map5
ORDER BY t.pk;

-- ========== FE error: non-equality comparison on MAP ==========

-- query 15
-- @expect_error=does not support binary predicate operation
SELECT t.map0, s.map0
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map0 < t.map0
ORDER BY t.pk;

-- ========== NULL-safe equal (<=>)  ==========

-- query 16
-- map3 <=> map3: includes NULL row in result
SELECT t.map3, s.map3
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s ON s.map3 <=> t.map3
ORDER BY t.pk;

-- ========== LEFT / RIGHT JOIN with MAP key ==========

-- query 17
-- LEFT JOIN map<int, struct>
SELECT t.map5, s.map5
FROM ${case_db}.map_test t LEFT JOIN ${case_db}.map_test s ON s.map5 = t.map5
ORDER BY t.pk;

-- query 18
-- RIGHT JOIN map<int, struct>
SELECT t.map5, s.map5
FROM ${case_db}.map_test t RIGHT JOIN ${case_db}.map_test s ON s.map5 = t.map5
ORDER BY t.pk;

-- ========== EXISTS / NOT EXISTS ==========

-- query 19
-- EXISTS with map3 self join
SELECT map3
FROM ${case_db}.map_test t
WHERE EXISTS (SELECT 1 FROM ${case_db}.map_test s WHERE t.map3 = s.map3)
ORDER BY t.pk;

-- query 20
-- NOT EXISTS with map0 self
SELECT map0
FROM ${case_db}.map_test t
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.map_test s WHERE t.map0 = s.map0)
ORDER BY t.pk;

-- query 21
-- NOT EXISTS with map1 vs map0 (cross column, compatible types auto-cast)
SELECT map1
FROM ${case_db}.map_test t
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.map_test s WHERE t.map1 = s.map0)
ORDER BY t.pk;

-- ========== IN / NOT IN subquery ==========

-- query 22
-- IN subquery with map3
SELECT map3
FROM ${case_db}.map_test t
WHERE map3 IN (SELECT map3 FROM ${case_db}.map_test s)
ORDER BY t.pk;

-- query 23
-- IN subquery with map5
SELECT map5
FROM ${case_db}.map_test t
WHERE map5 IN (SELECT map5 FROM ${case_db}.map_test s)
ORDER BY t.pk;

-- query 24
-- NOT IN subquery with map5
SELECT map5
FROM ${case_db}.map_test t
WHERE map5 NOT IN (SELECT map5 FROM ${case_db}.map_test s)
ORDER BY t.pk;

-- ========== IN / NOT IN as expression (boolean output) ==========

-- query 25
-- FE generates a column alias containing ${case_db}, which is dynamic; skip result snapshot
-- and verify row count + key values via wrapper.
-- @skip_result_check=true
SELECT map0 IN (SELECT map0 FROM ${case_db}.map_test s) FROM ${case_db}.map_test ORDER BY 1;

-- query 26
-- @skip_result_check=true
SELECT map5 IN (SELECT map5 FROM ${case_db}.map_test s) FROM ${case_db}.map_test ORDER BY 1;

-- query 27
-- @skip_result_check=true
SELECT map0 NOT IN (SELECT map0 FROM ${case_db}.map_test s) FROM ${case_db}.map_test ORDER BY 1;

-- query 28
-- @skip_result_check=true
SELECT map1 NOT IN (SELECT map0 FROM ${case_db}.map_test s) FROM ${case_db}.map_test ORDER BY 1;

-- query 29
-- @skip_result_check=true
SELECT map5 NOT IN (SELECT map5 FROM ${case_db}.map_test s) FROM ${case_db}.map_test ORDER BY 1;

-- ========== map_apply in join predicate ==========

-- query 30
SELECT t.map2, s.map2
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s
  ON map_apply((k,v)->(k+1,array_length(v)),s.map2) = map_apply((k,v)->(k+1,array_length(v)),t.map2)
ORDER BY t.pk;

-- query 31
SELECT t.map2, s.map2
FROM ${case_db}.map_test t JOIN ${case_db}.map_test s
  ON map_apply((k,v)->(k+1,v),s.map2) = map_apply((k,v)->(k+1,v),t.map2)
ORDER BY t.pk;
