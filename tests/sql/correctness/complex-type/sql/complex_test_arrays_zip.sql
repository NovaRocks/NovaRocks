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

-- Migrated from dev/test/sql/test_array_fn/R/test_arrays_zip
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_arrays_zip @mac
-- query 2
USE ${case_db};
SELECT arrays_zip([1, 2], ['1b', '2b']) AS result;

-- query 3
USE ${case_db};
SELECT arrays_zip([1, 2], ['1b', null, '3b']) AS result;

-- query 4
USE ${case_db};
SELECT arrays_zip([1, 2, 3], ['a', 'b', 'c'], [10, 20, 30]) AS result;

-- query 5
USE ${case_db};
SELECT arrays_zip([], []) AS result;

-- query 6
USE ${case_db};
SELECT arrays_zip([1, 2, 3]) AS result;

-- query 7
USE ${case_db};
SELECT arrays_zip(null, [1, 2]) AS result;

-- query 8
USE ${case_db};
SELECT arrays_zip([1, null, 3], ['a', 'b', null]) AS result;

-- query 9
USE ${case_db};
SELECT * FROM (SELECT 1 AS id) a WHERE a.id = 1 AND arrays_zip([1], ['a']) IS NOT NULL;

-- query 10
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE arrays_zip_test (
  pk bigint not null,
  int_array Array<BigInt>,
  str_array Array<VARCHAR>,
  double_array Array<Double>,
  null_array Array<VARCHAR>,
  mixed_null_array Array<BigInt>,
  empty_array Array<VARCHAR>,
  single_element_array Array<BigInt>,
  large_array Array<VARCHAR>,
  decimal_array Array<DECIMAL(10, 2)>
) WITH (format_version = 3);

-- query 11
-- @skip_result_check=true
USE ${case_db};
INSERT INTO arrays_zip_test VALUES ROW(1, ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'], ARRAY[DECIMAL '1.1', DECIMAL '2.2', DECIMAL '3.3'], NULL, ARRAY[1, 2, 3], ARRAY[], ARRAY[100], ARRAY['x1', 'x2', 'x3'], ARRAY[DECIMAL '10.50', DECIMAL '20.75', DECIMAL '30.25']), ROW(2, ARRAY[10, 20], ARRAY['hello', 'world'], ARRAY[DECIMAL '10.5', DECIMAL '20.5'], NULL, ARRAY[10, NULL, 30], ARRAY[], ARRAY[200], ARRAY['y1', 'y2'], ARRAY[DECIMAL '100.00', DECIMAL '200.00']), ROW(3, ARRAY[100], ARRAY['single'], ARRAY[DECIMAL '99.9'], NULL, ARRAY[NULL, NULL], ARRAY[], ARRAY[300], ARRAY['single_element'], ARRAY[DECIMAL '999.99']), ROW(4, ARRAY[], ARRAY[], ARRAY[], NULL, ARRAY[], ARRAY[], ARRAY[], ARRAY[], ARRAY[]), ROW(5, ARRAY[1, 2, 3, 4, 5], ARRAY['a', 'b', 'c', 'd', 'e'], ARRAY[DECIMAL '1.1', DECIMAL '2.2', DECIMAL '3.3', DECIMAL '4.4', DECIMAL '5.5'], NULL, ARRAY[1, 2, 3, 4, 5], ARRAY[], ARRAY[400], ARRAY['a', 'b', 'c', 'd', 'e', 'f'], ARRAY[DECIMAL '1.11', DECIMAL '2.22', DECIMAL '3.33', DECIMAL '4.44', DECIMAL '5.55']), ROW(6, ARRAY[NULL, NULL, NULL], ARRAY[NULL, NULL, NULL], ARRAY[NULL, NULL, NULL], NULL, ARRAY[NULL, NULL, NULL], ARRAY[], ARRAY[500], ARRAY[NULL, NULL, NULL], ARRAY[NULL, NULL, NULL]), ROW(7, ARRAY[1, NULL, 3, NULL, 5], ARRAY['x', NULL, 'z', NULL, 'w'], ARRAY[DECIMAL '1.1', NULL, DECIMAL '3.3', NULL, DECIMAL '5.5'], NULL, ARRAY[1, NULL, 3, NULL, 5], ARRAY[], ARRAY[600], ARRAY['p', NULL, 'r', NULL, 't'], ARRAY[DECIMAL '11.11', NULL, DECIMAL '33.33', NULL, DECIMAL '55.55']), ROW(8, ARRAY[999, 888, 777, 666, 555, 444, 333, 222, 111, 0], ARRAY['z', 'y', 'x', 'w', 'v', 'u', 't', 's', 'r', 'q'], ARRAY[DECIMAL '99.9', DECIMAL '88.8', DECIMAL '77.7', DECIMAL '66.6', DECIMAL '55.5', DECIMAL '44.4', DECIMAL '33.3', DECIMAL '22.2', DECIMAL '11.1', DECIMAL '0.0'], NULL, ARRAY[9, 8, 7, 6, 5, 4, 3, 2, 1, 0], ARRAY[], ARRAY[700], ARRAY['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j'], ARRAY[DECIMAL '9.90', DECIMAL '8.80', DECIMAL '7.70', DECIMAL '6.60', DECIMAL '5.50', DECIMAL '4.40', DECIMAL '3.30', DECIMAL '2.20', DECIMAL '1.10', DECIMAL '0.00']), ROW(9, ARRAY[1], ARRAY['one'], ARRAY[DECIMAL '1.0'], NULL, ARRAY[1], ARRAY[], ARRAY[800], ARRAY['one'], ARRAY[DECIMAL '1.00']), ROW(10, ARRAY[2, 4, 6, 8, 10, 12], ARRAY['two', 'four', 'six', 'eight', 'ten', 'twelve'], ARRAY[DECIMAL '2.0', DECIMAL '4.0', DECIMAL '6.0', DECIMAL '8.0', DECIMAL '10.0', DECIMAL '12.0'], NULL, ARRAY[2, 4, 6, 8, 10, 12], ARRAY[], ARRAY[900], ARRAY['even1', 'even2', 'even3', 'even4', 'even5', 'even6'], ARRAY[DECIMAL '2.00', DECIMAL '4.00', DECIMAL '6.00', DECIMAL '8.00', DECIMAL '10.00', DECIMAL '12.00']), ROW(11, ARRAY[3, 6, 9, 12, 15], ARRAY['three', 'six', 'nine', 'twelve', 'fifteen'], ARRAY[DECIMAL '3.0', DECIMAL '6.0', DECIMAL '9.0', DECIMAL '12.0', DECIMAL '15.0'], NULL, ARRAY[3, 6, 9, 12, 15], ARRAY[], ARRAY[1000], ARRAY['odd1', 'odd2', 'odd3', 'odd4', 'odd5'], ARRAY[DECIMAL '3.00', DECIMAL '6.00', DECIMAL '9.00', DECIMAL '12.00', DECIMAL '15.00']), ROW(12, ARRAY[NULL], ARRAY[NULL], ARRAY[NULL], NULL, ARRAY[NULL], ARRAY[], ARRAY[1100], ARRAY[NULL], ARRAY[NULL]), ROW(13, ARRAY[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200], ARRAY['a1', 'b1', 'c1', 'd1', 'e1', 'f1', 'g1', 'h1', 'i1', 'j1', 'k1', 'l1'], ARRAY[DECIMAL '100.1', DECIMAL '200.2', DECIMAL '300.3', DECIMAL '400.4', DECIMAL '500.5', DECIMAL '600.6', DECIMAL '700.7', DECIMAL '800.8', DECIMAL '900.9', DECIMAL '1000.1', DECIMAL '1100.2', DECIMAL '1200.3'], NULL, ARRAY[100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1100, 1200], ARRAY[], ARRAY[1200], ARRAY['long1', 'long2', 'long3', 'long4', 'long5', 'long6', 'long7', 'long8', 'long9', 'long10', 'long11', 'long12'], ARRAY[DECIMAL '100.10', DECIMAL '200.20', DECIMAL '300.30', DECIMAL '400.40', DECIMAL '500.50', DECIMAL '600.60', DECIMAL '700.70', DECIMAL '800.80', DECIMAL '900.90', DECIMAL '1000.10', DECIMAL '1100.20', DECIMAL '1200.30']), ROW(14, ARRAY[-1, -2, -3], ARRAY['neg_a', 'neg_b', 'neg_c'], ARRAY[(- (DECIMAL '1.5')), (- (DECIMAL '2.5')), (- (DECIMAL '3.5'))], NULL, ARRAY[-1, -2, -3], ARRAY[], ARRAY[1300], ARRAY['neg1', 'neg2', 'neg3'], ARRAY[(- (DECIMAL '1.50')), (- (DECIMAL '2.50')), (- (DECIMAL '3.50'))]), ROW(15, ARRAY[1, 2], ARRAY['short', 'array'], ARRAY[DECIMAL '1.0', DECIMAL '2.0'], NULL, ARRAY[1, 2], ARRAY[], ARRAY[1400], ARRAY['a', 'b'], ARRAY[DECIMAL '1.00', DECIMAL '2.00']), ROW(16, ARRAY[0, 0, 0], ARRAY['zero', 'zero', 'zero'], ARRAY[DECIMAL '0.0', DECIMAL '0.0', DECIMAL '0.0'], NULL, ARRAY[0, 0, 0], ARRAY[], ARRAY[1500], ARRAY['z1', 'z2', 'z3'], ARRAY[DECIMAL '0.00', DECIMAL '0.00', DECIMAL '0.00']), ROW(17, ARRAY[111, 222, 333, 444, 555], ARRAY['str_111', 'str_222', 'str_333', 'str_444', 'str_555'], ARRAY[DECIMAL '111.1', DECIMAL '222.2', DECIMAL '333.3', DECIMAL '444.4', DECIMAL '555.5'], NULL, ARRAY[111, 222, 333, 444, 555], ARRAY[], ARRAY[1600], ARRAY['s1', 's2', 's3', 's4', 's5'], ARRAY[DECIMAL '111.11', DECIMAL '222.22', DECIMAL '333.33', DECIMAL '444.44', DECIMAL '555.55']), ROW(18, ARRAY[1, 1, 1], ARRAY['dup', 'dup', 'dup'], ARRAY[DECIMAL '1.1', DECIMAL '1.1', DECIMAL '1.1'], NULL, ARRAY[1, 1, 1], ARRAY[], ARRAY[1700], ARRAY['d1', 'd2', 'd3'], ARRAY[DECIMAL '1.11', DECIMAL '1.11', DECIMAL '1.11']), ROW(19, ARRAY[7, 8, 9, 10, 11, 12, 13, 14, 15], ARRAY['seven', 'eight', 'nine', 'ten', 'eleven', 'twelve', 'thirteen', 'fourteen', 'fifteen'], ARRAY[DECIMAL '7.7', DECIMAL '8.8', DECIMAL '9.9', DECIMAL '10.1', DECIMAL '11.1', DECIMAL '12.1', DECIMAL '13.1', DECIMAL '14.1', DECIMAL '15.1'], NULL, ARRAY[7, 8, 9, 10, 11, 12, 13, 14, 15], ARRAY[], ARRAY[1800], ARRAY['n1', 'n2', 'n3', 'n4', 'n5', 'n6', 'n7', 'n8', 'n9'], ARRAY[DECIMAL '7.70', DECIMAL '8.80', DECIMAL '9.90', DECIMAL '10.10', DECIMAL '11.10', DECIMAL '12.10', DECIMAL '13.10', DECIMAL '14.10', DECIMAL '15.10']), ROW(20, ARRAY[42], ARRAY['answer'], ARRAY[DECIMAL '42.0'], NULL, ARRAY[42], ARRAY[], ARRAY[1900], ARRAY['fortytwo'], ARRAY[DECIMAL '42.00']);

-- query 12
USE ${case_db};
SELECT pk, arrays_zip(int_array, str_array) FROM arrays_zip_test ORDER BY pk;

-- query 13
USE ${case_db};
SELECT pk, arrays_zip(int_array, str_array, double_array) FROM arrays_zip_test ORDER BY pk;

-- query 14
USE ${case_db};
SELECT pk, arrays_zip(null_array, int_array) FROM arrays_zip_test ORDER BY pk;

-- query 15
USE ${case_db};
SELECT pk, arrays_zip(mixed_null_array, str_array) FROM arrays_zip_test ORDER BY pk;

-- query 16
USE ${case_db};
SELECT pk, arrays_zip(empty_array, int_array) FROM arrays_zip_test ORDER BY pk;

-- query 17
USE ${case_db};
SELECT pk, arrays_zip(single_element_array, str_array) FROM arrays_zip_test ORDER BY pk;

-- query 18
USE ${case_db};
SELECT pk, arrays_zip(large_array, double_array) FROM arrays_zip_test ORDER BY pk;

-- query 19
USE ${case_db};
SELECT pk, arrays_zip(int_array, str_array, double_array, decimal_array) FROM arrays_zip_test ORDER BY pk;

-- query 20
USE ${case_db};
SELECT pk, arrays_zip(int_array) FROM arrays_zip_test ORDER BY pk;

-- query 21
USE ${case_db};
SELECT pk FROM arrays_zip_test WHERE arrays_zip(int_array, str_array) IS NOT NULL ORDER BY pk;

-- query 22
USE ${case_db};
SELECT pk FROM arrays_zip_test WHERE arrays_zip(null_array, str_array) IS NULL ORDER BY pk;

-- query 23
USE ${case_db};
SELECT pk, arrays_zip(int_array, ['const1', 'const2', 'const3']) FROM arrays_zip_test ORDER BY pk;

-- query 24
USE ${case_db};
SELECT pk, arrays_zip(['const_a', 'const_b', 'const_c'], str_array) FROM arrays_zip_test ORDER BY pk;

-- query 25
USE ${case_db};
SELECT pk, arrays_zip(int_array, decimal_array) FROM arrays_zip_test ORDER BY pk;

-- query 26
USE ${case_db};
SELECT pk, arrays_zip(int_array, str_array) AS zipped 
FROM arrays_zip_test 
WHERE pk > 5 AND pk <= 15
ORDER BY pk;

-- query 27
USE ${case_db};
SELECT pk, arrays_zip(str_array, double_array, decimal_array) 
FROM arrays_zip_test 
WHERE int_array IS NOT NULL
ORDER BY pk;

-- query 28
USE ${case_db};
SELECT pk, int_array, str_array, arrays_zip(mixed_null_array, str_array, double_array) 
FROM arrays_zip_test 
WHERE pk IN (1, 6, 7, 12)
ORDER BY pk;

-- query 29
USE ${case_db};
SELECT pk, 
  CASE 
    WHEN pk <= 5 THEN arrays_zip(int_array, str_array)
    WHEN pk <= 10 THEN arrays_zip(str_array, double_array)
    ELSE arrays_zip(double_array, decimal_array)
  END AS conditional_zip
FROM arrays_zip_test
ORDER BY pk;

-- query 30
USE ${case_db};
SELECT pk, arrays_zip(int_array, CASE WHEN pk > 10 THEN str_array ELSE empty_array END) 
FROM arrays_zip_test 
ORDER BY pk;

-- query 31
USE ${case_db};
SELECT pk, arrays_zip(CAST(int_array AS Array<VARCHAR>), str_array)
FROM arrays_zip_test 
ORDER BY pk;

-- query 32
USE ${case_db};
SELECT pk, array_length(arrays_zip(int_array, str_array)) AS zip_length
FROM arrays_zip_test
ORDER BY pk;

-- query 33
USE ${case_db};
SELECT DISTINCT arrays_zip([1, 2], ['a', 'b']) FROM arrays_zip_test LIMIT 5;

-- query 34
USE ${case_db};
SELECT COUNT(*) AS cnt, arrays_zip([1], ['test'])
FROM arrays_zip_test 
GROUP BY arrays_zip([1], ['test']);

-- query 35
-- @skip_result_check=true
-- A catalog that cannot hold views cannot answer view enumeration, so
-- DROP DATABASE ... FORCE is refused here rather than silently assuming
-- the namespace holds none. Drop the tables explicitly instead.
USE ${case_db};
DROP TABLE IF EXISTS arrays_zip_test;
