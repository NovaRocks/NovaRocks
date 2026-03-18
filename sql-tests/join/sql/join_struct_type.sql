-- @tags=join,struct
-- Test Objective:
-- Validate JOIN correctness for STRUCT-typed columns across all join types.
-- Covers: INNER, LEFT/RIGHT/FULL OUTER with ON and WHERE syntax,
-- NULL-safe equal (<=>), EXISTS/NOT EXISTS, IN/NOT IN subqueries.
-- STRUCT value types include: simple (int,string), decimal+varchar, nested array,
-- nested map, JSON, and nested struct.
-- Also verifies FE rejects unsupported struct comparisons (cross-type, non-equality).

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.struct_test;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.struct_test (
  pk bigint not null,
  s0 struct<c0 int, c1 string>,
  s1 struct<c0 DECIMAL(16, 3), c1 varchar(30)>,
  s2 struct<c0 int, c1 array<string>>,
  s3 struct<c0 string, c1 map<int, varchar(30)>>,
  s4 struct<c0 int, c1 json>,
  s5 struct<c0 INT, c1 STRUCT<c INT, b string>>
) ENGINE=OLAP
DUPLICATE KEY(pk)
DISTRIBUTED BY HASH(pk) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
-- FE error: cannot cast row(3,'',null,null) to struct<c0 int, c1 string>
-- @expect_error=Cannot cast
INSERT INTO ${case_db}.struct_test (pk, s0) VALUES (7, row(3, '', null, null));

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.struct_test VALUES
  (0, row(0,'ab'), row(0,'ab'), row(0,['1','2']), row('1',map(1,'abc')), row(1,json_object('name','abc','age',23)), row(0, row(1,'a')));

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.struct_test VALUES
  (1, row(1, null), row(null,''), row(1,[]), row('11',map(1,'abc','',null)), row(null,json_object('name',null)), row(null, row(null,null)));

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.struct_test VALUES
  (2, row(null,null), row(null,null), row(null,null), row(null,map(null,null)), row(null,null), row(null, row(null,null)));

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.struct_test VALUES
  (3, row(3,''), row(3,''), row(3,['3',null, null,null]), row('3',map(3,'a33c',null,null)), row(null,json_object('name','abc','age',23)), row(null, row(3,'a')));

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.struct_test VALUES (4, null, null, null, null, null, null);

-- ========== INNER JOIN (ON syntax, same type) ==========

-- query 9
-- @order_sensitive=true
-- struct<int, string> self join
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s0 = t.s0
ORDER BY t.pk;

-- query 10
-- @order_sensitive=true
-- struct<decimal, varchar> self join
SELECT t.s1, s.s1
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s1 = t.s1
ORDER BY t.pk;

-- query 11
-- @order_sensitive=true
-- struct<int, array<string>> self join
SELECT t.s2, s.s2
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s2 = t.s2
ORDER BY t.pk;

-- query 12
-- @order_sensitive=true
-- struct<int, struct<int, string>> self join
SELECT t.s5, s.s5
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s5 = t.s5
ORDER BY t.pk;

-- ========== INNER JOIN (WHERE syntax, same type) ==========

-- query 13
-- @order_sensitive=true
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s0 = t.s0
ORDER BY t.pk;

-- query 14
-- @order_sensitive=true
SELECT t.s1, s.s1
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s1 = t.s1
ORDER BY t.pk;

-- query 15
-- @order_sensitive=true
SELECT t.s2, s.s2
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s2 = t.s2
ORDER BY t.pk;

-- query 16
-- @order_sensitive=true
-- struct<string, map<int, varchar>> self join WHERE
SELECT t.s3, s.s3
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s3 = t.s3
ORDER BY t.pk;

-- ========== FE error: cross-type struct comparison ==========

-- query 17
-- @expect_error=does not support binary predicate operation
SELECT t.s2, s.s3
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s2 = t.s3
ORDER BY t.pk;

-- ========== NULL-safe equal (<=>) ON syntax ==========

-- query 18
-- @order_sensitive=true
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s0 <=> t.s0
ORDER BY t.pk;

-- query 19
-- @order_sensitive=true
SELECT t.s1, s.s1
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s1 <=> t.s1
ORDER BY t.pk;

-- query 20
-- @order_sensitive=true
SELECT t.s5, s.s5
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s5 <=> t.s5
ORDER BY t.pk;

-- ========== NULL-safe equal (<=>) WHERE syntax ==========

-- query 21
-- @order_sensitive=true
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s0 <=> t.s0
ORDER BY t.pk;

-- query 22
-- @order_sensitive=true
SELECT t.s1, s.s1
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s1 <=> t.s1
ORDER BY t.pk;

-- query 23
-- @order_sensitive=true
SELECT t.s2, s.s2
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s2 <=> t.s2
ORDER BY t.pk;

-- query 24
-- @order_sensitive=true
SELECT t.s3, s.s3
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s3 <=> t.s3
ORDER BY t.pk;

-- ========== FE error: <=> cross-type struct comparison ==========

-- query 25
-- @expect_error=does not support binary predicate operation
SELECT t.s4, s.s5
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s WHERE s.s4 <=> t.s5
ORDER BY t.pk;

-- ========== FE error: non-equality comparison on struct ==========

-- query 26
-- @expect_error=does not support binary predicate operation
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t JOIN ${case_db}.struct_test s ON s.s0 < t.s0
ORDER BY t.pk;

-- ========== EXISTS / NOT EXISTS ==========

-- query 27
-- @expect_error=Not support exists correlation subquery with Non-EQ predicate
SELECT s2 FROM ${case_db}.struct_test t
WHERE EXISTS (SELECT 1 FROM ${case_db}.struct_test s WHERE t.s2 <=> s.s2)
ORDER BY t.pk;

-- query 28
-- @expect_error=does not support binary predicate operation
SELECT s2 FROM ${case_db}.struct_test t
WHERE EXISTS (SELECT 1 FROM ${case_db}.struct_test s WHERE t.s2 = s.s5)
ORDER BY t.pk;

-- query 29
-- @expect_error=Not support exists correlation subquery with Non-EQ predicate
SELECT s5 FROM ${case_db}.struct_test t
WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.struct_test s WHERE t.s5 <=> s.s5)
ORDER BY t.pk;

-- ========== IN / NOT IN subquery ==========

-- query 30
-- @expect_error=of in predict are not compatible
SELECT s2 FROM ${case_db}.struct_test t
WHERE s2 IN (SELECT s0 FROM ${case_db}.struct_test s)
ORDER BY t.pk;

-- query 31
-- @order_sensitive=true
SELECT s4 FROM ${case_db}.struct_test t
WHERE s4 IN (SELECT s1 FROM ${case_db}.struct_test s)
ORDER BY t.pk;

-- query 32
-- @order_sensitive=true
SELECT s5 FROM ${case_db}.struct_test t
WHERE s5 NOT IN (SELECT s5 FROM ${case_db}.struct_test s)
ORDER BY t.pk;

-- ========== LEFT JOIN ==========

-- query 33
-- @order_sensitive=true
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t LEFT JOIN ${case_db}.struct_test s ON s.s0 = t.s0
ORDER BY t.pk;

-- ========== RIGHT JOIN ==========

-- query 34
-- @order_sensitive=true
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t RIGHT JOIN ${case_db}.struct_test s ON s.s0 = t.s0
ORDER BY t.pk;

-- query 35
-- @order_sensitive=true
-- duplicate to verify right join stability
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t RIGHT JOIN ${case_db}.struct_test s ON s.s0 = t.s0
ORDER BY t.pk;

-- ========== FULL OUTER JOIN ==========

-- query 36
-- @order_sensitive=true
SELECT t.s0, s.s0
FROM ${case_db}.struct_test t FULL JOIN ${case_db}.struct_test s ON s.s0 = t.s0
ORDER BY t.pk;

-- ========== FE error: LEFT JOIN cross-type ==========

-- query 37
-- @expect_error=does not support binary predicate operation
SELECT t.s1, s.s2
FROM ${case_db}.struct_test t LEFT JOIN ${case_db}.struct_test s ON s.s1 = t.s2
ORDER BY t.pk;
