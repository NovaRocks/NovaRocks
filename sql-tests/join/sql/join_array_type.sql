-- @tags=join,array_type
-- Test Objective:
-- 1. Validate join correctness on array-typed columns including Array<Int>,
--    Array<String>, Array<BigInt>, Array<Double>, Array<DECIMAL>, nested arrays,
--    and cross-type comparisons (string-vs-int, int-vs-decimal, etc.).
-- 2. Cover inner join (=, <=>, !=, <, >), left join, right join, full join,
--    EXISTS/NOT EXISTS subqueries, IN/NOT IN subqueries on array columns.
-- 3. Verify FE error handling for incompatible array type predicates.
-- Test Flow:
-- 1. Create array_test table with diverse array column types.
-- 2. Insert 6 rows with varied array data including NULLs and nested arrays.
-- 3. Run representative join queries across join types and operators.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.array_test;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.array_test (
pk bigint not null ,
i_0   Array<Int> not null,
s_1   Array<String>,
i_1   Array<BigInt>,
f_1   Array<Double>,
d_1   Array<DECIMAL(26, 2)>,
d_2   Array<DECIMAL64(4, 3)>,
d_3   Array<DECIMAL128(25, 19)>,
d_4   Array<DECIMAL32(8, 5)> ,
d_5   Array<DECIMAL(16, 3)>,
d_6   Array<DECIMAL128(18, 6)> ,
ai_1  Array<Array<BigInt>>,
as_1  Array<Array<String>>,
aas_1 Array<Array<Array<String>>>,
aad_1 Array<Array<Array<DECIMAL(26, 2)>>>
) ENGINE=OLAP
DUPLICATE KEY(`pk`)
DISTRIBUTED BY HASH(`pk`) BUCKETS 3
PROPERTIES (
"replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.array_test VALUES
(1,[null], ['a', 'b', 'c'], [1.0, 2.0, 3.0, 4.0, 10.0], [1.0, 2.0, 3.0, 4.0, 10.0, 1.1, 2.1, 3.2, 4.3, -1, -10, 100], [4.0, 10.0, 1.1, 2.1, 3.2, 4.3, -1, -10, 100, 1.0, 2.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 2.0, 3.0, 2.1, 3.2, 4.3, -1], [4.0, 2.1, 3.2, 10.0, 1.1, -10, 100, -1, 1.0, 2.0, 3.0, 4.3], [4.0, 2.1, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10, 100, 1.0, 4.3], [4.0, 2.1, 3.0, 1.1, 4.3, 3.2, -10, 100, 1.0, 10.0, -1, 2.0], [4.0, 2.1, 100, 1.0, 4.3, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10], [[1, 2, 3, 4], [5, 2, 6, 4], [100, -1, 92, 8], [66, 4, 32, -10]], [['1', '2', '3', '4'], ['-1', 'a', '-100', '100'], ['a', 'b', 'c']], [[['1'],['2'],['3']], [['6'],['5'],['4']], [['-1', '-2'],['-2', '10'],['100','23']]], [[[1],[2],[3]], [[6],[5],[4]], [[-1, -2],[-2, 10],[100,23]]]),
(2,[1], ['-1', '10', '1', '100', '2'], NULL, [10.0, 20.0, 30.0, 4.0, 100.0, 10.1, 2.1, 30.2, 40.3, -1, -10, 100], [40.0, 100.0, 01.1, 2.1, 30.2, 40.3, -1, -100, 1000, 1.0, 2.0, 3.0], [40.0, 100.0, 01.1, -10, 1000, 10.0, 2.0, 30.0, 20.1, 3.2, 4.3, -1], NULL, NULL, [40.0, 20.1, 30.0, 10.1, 40.30, 30.20, -100, 1000, 1.0, 100.0, -10, 2.0], [40.0, 20.1, 1000, 10.0, 40.30, 30.20, 100.0, 20.0, 3.0, 10.1, -10, -10], NULL, NULL, [[['10'],['20'],['30']], [['60'],['5'],['4']], [['-100', '-2'],['-20', '10'],['100','23']]], [[[10],[20],[30]], [[60],[50],[4]], [[-1, -2],[-2, 100],[100,23]]]),
(4,[1,2,3,4,10], ['a', NULL, 'c', 'e', 'd'], [1.0, 2.0, 3.0, 4.0, 10.0], [1.0, 2.0, 3.0, 4.0, 10.0, NULL, 1.1, 2.1, 3.2, NULL, 4.3, -1, -10, 100], [4.0, 10.0, 1.1, 2.1,NULL, 3.2, 4.3, -1, -10, 100, 1.0, 2.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 2.0, 3.0, 2.1, 3.2, 4.3, -1], [4.0, 2.1, 3.2, 10.0, 1.1, -10, 100, -1, 1.0, 2.0, 3.0, 4.3], [4.0, 2.1, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10, 100, 1.0, 4.3], [4.0, 2.1, 3.0, 1.1, 4.3, 3.2, -10, 100, 1.0, 10.0, -1, 2.0], [4.0, 2.1, 100, NULL, 1.0, 4.3, 3.2, 10.0, 2.0, 3.0, 1.1, -1, -10], [[1, 2, 3, NULL, 4], [5, 2, 6, 4], NULL, [100, -1, 92, 8], [66, 4, 32, -10]], [['1', '2', '3', '4'], ['-1', 'a', '-100', '100'], ['a', 'b', 'c']], [[['1'],['2'],['3']], [['6'],['5'],['4']], [['-1', '-2'],NULL,['-2', '10'],['100','23']]], [[[1],NULL,[2],[3]], [[6],[5],[4]], NULL, [[-1, -2],[-2, 10],[100,23]]]),
(3,[null,null,null,null,null], NULL, [1.0, 2.0, 3.0, 4.0, 10.0], NULL, [40.0, 10.0, 1.1, 2.1, 3.2, 4.3, -10, -10, 100, 10.0, 20.0, 3.0], [4.0, 10.0, 1.1, -10, 100, 1.0, 20.0, 3.0, 2.1, 3.2, 4.3, -1], [40.0, 20.1, 3.2, 10.0, 10.1, -10, 100, -1, 10.0, 2.0, 30.0, 4.3], [4.0, 2.1, 3.2, 10.0, 20.0, 3.0, 1.1, -10, -100, 100, 10.0, 4.3], NULL, NULL, [[1, 2, 30, 4], [50, 2, 6, 4], [100, -10, 92, 8], [66, 40, 32, -100]], [['1', '20', '3', '4'], ['-1', 'a00', '-100', '100'], ['a', 'b0', 'c']], NULL, NULL),
(5,[1,2],  ['1','2'], [1, 2], [1.0, 2.0], [1.00, 2.00], [1.00, 2.00], [1.00, 2.00], [1.00, 2.00], [1.00, 2.00], [1.00, 2.00], [[1, 2], [50, 2, 6, 4]], [['1', '2'], ['50', '2', '6', '4']], [[['1', '2'],['3']], [['50', '2', '6', '4']]], [[[1, 2],[3]], [[50, 2, 6, 4]]]),
(6,[1,null], ['1',null], [1, null], [1.0, null], [1.00, null], [1.00, null], [1.00, null], [1.00, null], [1.00, null], [1.00, null], [[1, null], [50, 2, 6, 4]], [['1', null], ['50', '2', '6', '4']], [[['1', null],['3'],null], [['50', '2', '6', '4']]], [[[1, null],[3],null], [[50, 2, 6, 4]]]);

-- ============================================================
-- INNER JOIN: Array<String> = Array<BigInt> (cross-type equality)
-- ============================================================
-- query 4
-- @order_sensitive=true
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.s_1 = t.i_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: Array<String> <=> Array<BigInt> (null-safe equality)
-- ============================================================
-- query 5
-- @order_sensitive=true
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.s_1 <=> t.i_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: Array<String> > Array<BigInt> (comparison)
-- ============================================================
-- query 6
-- @order_sensitive=true
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.s_1 > t.i_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: Array<String> < Array<BigInt>
-- ============================================================
-- query 7
-- @order_sensitive=true
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.s_1 < t.i_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: Array<String> = Array<DECIMAL> (all return empty - type mismatch)
-- ============================================================
-- query 8
-- @order_sensitive=true
SELECT s.s_1, t.d_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.s_1 = t.d_1 ORDER BY 1, 2;

-- ============================================================
-- FE error: nested array vs decimal array (incompatible types)
-- ============================================================
-- query 9
-- @expect_error=does not support binary predicate operation
SELECT s.ai_1, t.d_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.ai_1 = t.d_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: nested Array<Array<BigInt>> != Array<Array<String>> (cross-type !=)
-- ============================================================
-- query 10
-- @order_sensitive=true
SELECT s.ai_1, t.as_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.ai_1 != t.as_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: Array<Array<BigInt>> = / != (same type equality/inequality)
-- ============================================================
-- query 11
-- @order_sensitive=true
SELECT s.ai_1, t.ai_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.ai_1 = t.ai_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: 3-nested array equality: aad_1 = aad_1
-- ============================================================
-- query 12
-- @order_sensitive=true
SELECT s.aad_1, t.aad_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.aad_1 = t.aad_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: 3-nested array equality: aas_1 = aas_1
-- ============================================================
-- query 13
-- @order_sensitive=true
SELECT s.aas_1, t.aas_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.aas_1 = t.aas_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: 3-nested array null-safe: aas_1 <=> aas_1
-- ============================================================
-- query 14
-- @order_sensitive=true
SELECT s.aas_1, t.aas_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.aas_1 <=> t.aas_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: with additional array_length predicate
-- ============================================================
-- query 15
-- @order_sensitive=true
SELECT s.i_1, t.i_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.i_1 = t.i_1 AND array_length(s.i_1) > 3 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: count with cross-type <=> and =
-- ============================================================
-- query 16
SELECT count(*) FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.i_1 <=> t.s_1;

-- query 17
SELECT count(*) FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.i_1 = t.s_1;

-- ============================================================
-- INNER JOIN: Array<BigInt> = Array<DECIMAL> (various decimal precisions)
-- ============================================================
-- query 18
-- @order_sensitive=true
SELECT s.i_1, t.d_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.i_1 = t.d_1 ORDER BY 1, 2;

-- query 19
-- @order_sensitive=true
SELECT s.i_1, t.d_2 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.i_1 = t.d_2 ORDER BY 1, 2;

-- query 20
-- @order_sensitive=true
SELECT s.i_1, t.d_3 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.i_1 = t.d_3 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: decimal cross-precision equality
-- ============================================================
-- query 21
-- @order_sensitive=true
SELECT s.d_4, t.d_3 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.d_4 = t.d_3 ORDER BY 1, 2;

-- query 22
-- @order_sensitive=true
SELECT s.d_6, t.d_3 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.d_6 = t.d_3 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: decimal >= comparison
-- ============================================================
-- query 23
-- @order_sensitive=true
SELECT s.d_5, t.d_4 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON s.d_5 >= t.d_4 ORDER BY 1, 2;

-- ============================================================
-- EXISTS subquery on array columns
-- ============================================================
-- query 24
-- @order_sensitive=true
SELECT s_1 FROM ${case_db}.array_test s WHERE EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.s_1 = s.s_1) ORDER BY 1;

-- query 25
-- @order_sensitive=true
SELECT s_1 FROM ${case_db}.array_test s WHERE EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.s_1 = s.i_1) ORDER BY 1;

-- query 26
-- @order_sensitive=true
SELECT s_1 FROM ${case_db}.array_test s WHERE EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.s_1 = s.d_1) ORDER BY 1;

-- ============================================================
-- FE error: EXISTS with <=> on array (non-EQ predicate not supported)
-- ============================================================
-- query 27
-- @expect_error=Not support exists correlation subquery with Non-EQ predicate
SELECT i_0 FROM ${case_db}.array_test s WHERE EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.i_0 <=> s.d_4) ORDER BY 1;

-- ============================================================
-- FE error: EXISTS with incompatible nested array types
-- ============================================================
-- query 28
-- @expect_error=does not support binary predicate operation
SELECT i_0 FROM ${case_db}.array_test s WHERE EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.i_0 <=> s.ai_1) ORDER BY 1;

-- ============================================================
-- NOT EXISTS subquery on array columns
-- ============================================================
-- query 29
-- @order_sensitive=true
SELECT s_1 FROM ${case_db}.array_test s WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.s_1 = s.s_1) ORDER BY 1;

-- query 30
-- @order_sensitive=true
SELECT s_1 FROM ${case_db}.array_test s WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.s_1 = s.i_1) ORDER BY 1;

-- query 31
-- @order_sensitive=true
SELECT i_0 FROM ${case_db}.array_test s WHERE NOT EXISTS (SELECT 1 FROM ${case_db}.array_test t WHERE t.i_0 = s.d_4) ORDER BY 1;

-- ============================================================
-- IN subquery on array columns
-- ============================================================
-- query 32
-- @order_sensitive=true
SELECT d_1 FROM ${case_db}.array_test s WHERE d_1 IN (SELECT i_0 FROM ${case_db}.array_test t) ORDER BY 1;

-- query 33
-- @order_sensitive=true
SELECT d_1 FROM ${case_db}.array_test s WHERE d_1 NOT IN (SELECT i_0 FROM ${case_db}.array_test t) ORDER BY 1;

-- query 34
-- @order_sensitive=true
SELECT ai_1 FROM ${case_db}.array_test s WHERE ai_1 NOT IN (SELECT ai_1 FROM ${case_db}.array_test t) ORDER BY 1;

-- ============================================================
-- FE error: IN with incompatible array types
-- ============================================================
-- query 35
-- @expect_error=in predict are not compatible
SELECT pk FROM ${case_db}.array_test s WHERE pk NOT IN (SELECT i_0 FROM ${case_db}.array_test t) ORDER BY 1;

-- ============================================================
-- IN scalar expression on array columns
-- ============================================================
-- query 36
-- @order_sensitive=true
SELECT ai_1 IN (SELECT ai_1 FROM ${case_db}.array_test t) FROM ${case_db}.array_test s ORDER BY 1;

-- query 37
-- @order_sensitive=true
SELECT d_1 NOT IN (SELECT d_2 FROM ${case_db}.array_test t) FROM ${case_db}.array_test s ORDER BY 1;

-- query 38
-- @order_sensitive=true
SELECT ai_1 NOT IN (SELECT ai_1 FROM ${case_db}.array_test t) FROM ${case_db}.array_test s ORDER BY 1;

-- ============================================================
-- FE error: nested array left join incompatible types
-- ============================================================
-- query 39
-- @expect_error=does not support binary predicate operation
SELECT s.ai_1, t.d_4 FROM ${case_db}.array_test s LEFT JOIN ${case_db}.array_test t ON s.ai_1 = t.d_4 ORDER BY 1, 2;

-- ============================================================
-- LEFT JOIN: nested array cross-type (ai_1 = as_1)
-- ============================================================
-- query 40
-- @order_sensitive=true
SELECT s.ai_1, t.as_1 FROM ${case_db}.array_test s LEFT JOIN ${case_db}.array_test t ON s.ai_1 = t.as_1 ORDER BY 1, 2;

-- ============================================================
-- LEFT JOIN: same-type nested array (ai_1 = ai_1)
-- ============================================================
-- query 41
-- @order_sensitive=true
SELECT s.ai_1, t.ai_1 FROM ${case_db}.array_test s LEFT JOIN ${case_db}.array_test t ON s.ai_1 = t.ai_1 ORDER BY 1, 2;

-- ============================================================
-- LEFT JOIN: Array<Array<String>> equality
-- ============================================================
-- query 42
-- @order_sensitive=true
SELECT s.as_1, t.as_1 FROM ${case_db}.array_test s LEFT JOIN ${case_db}.array_test t ON s.as_1 = t.as_1 ORDER BY 1, 2;

-- ============================================================
-- LEFT JOIN: 3-nested array <=> null-safe
-- ============================================================
-- query 43
-- @order_sensitive=true
SELECT s.aas_1, t.aas_1 FROM ${case_db}.array_test s LEFT JOIN ${case_db}.array_test t ON s.aas_1 <=> t.aas_1 ORDER BY 1, 2;

-- ============================================================
-- RIGHT JOIN: Array<String> = Array<BigInt>
-- ============================================================
-- query 44
-- @order_sensitive=true
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s RIGHT JOIN ${case_db}.array_test t ON s.s_1 = t.i_1 ORDER BY 1, 2;

-- ============================================================
-- RIGHT JOIN: Array<String> <=> Array<BigInt>
-- ============================================================
-- query 45
-- @order_sensitive=true
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s RIGHT JOIN ${case_db}.array_test t ON s.s_1 <=> t.i_1 ORDER BY 1, 2;

-- ============================================================
-- RIGHT JOIN: nested array equality (ai_1 = ai_1)
-- ============================================================
-- query 46
-- @order_sensitive=true
SELECT s.ai_1, t.ai_1 FROM ${case_db}.array_test s RIGHT JOIN ${case_db}.array_test t ON s.ai_1 = t.ai_1 ORDER BY 1, 2;

-- ============================================================
-- RIGHT JOIN: 3-nested array <=> null-safe
-- ============================================================
-- query 47
-- @order_sensitive=true
SELECT s.aad_1, t.aad_1 FROM ${case_db}.array_test s RIGHT JOIN ${case_db}.array_test t ON s.aad_1 <=> t.aad_1 ORDER BY 1, 2;

-- ============================================================
-- FULL JOIN: Array<String> = Array<BigInt>
-- ============================================================
-- query 48
-- @order_sensitive=true
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s FULL JOIN ${case_db}.array_test t ON s.s_1 = t.i_1 ORDER BY 1, 2;

-- ============================================================
-- FE error: syntax error with => operator
-- ============================================================
-- query 49
-- @expect_error=syntax error
SELECT s.s_1, t.i_1 FROM ${case_db}.array_test s FULL JOIN ${case_db}.array_test t ON s.s_1 => t.i_1 ORDER BY 1, 2;

-- ============================================================
-- FULL JOIN: 3-nested array <=> null-safe
-- ============================================================
-- query 50
-- @order_sensitive=true
SELECT s.aas_1, t.aas_1 FROM ${case_db}.array_test s FULL JOIN ${case_db}.array_test t ON s.aas_1 <=> t.aas_1 ORDER BY 1, 2;

-- ============================================================
-- FULL JOIN: 3-nested array != (aad_1)
-- ============================================================
-- query 51
-- @order_sensitive=true
SELECT s.aad_1, t.aad_1 FROM ${case_db}.array_test s FULL JOIN ${case_db}.array_test t ON s.aad_1 != t.aad_1 ORDER BY 1, 2;

-- ============================================================
-- INNER JOIN: array_map expression (empty result)
-- ============================================================
-- query 52
-- @order_sensitive=true
SELECT s.i_1, t.d_1 FROM ${case_db}.array_test s JOIN ${case_db}.array_test t ON array_map(x -> x*3, s.i_1) = array_map(x-> x*3 + 1, t.d_1) ORDER BY 1, 2;
