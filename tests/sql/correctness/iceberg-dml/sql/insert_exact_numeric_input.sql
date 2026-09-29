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

-- @order_sensitive=true
-- @tags=iceberg_dml,decimal,insert
-- Test Objective:
-- 1. Preserve exact numeric tokens until conversion to the write target type.
-- 2. Apply target typing before VALUES, constant UNION ALL, and container type merging.
-- 3. Preserve explicit CAST, source controls, and Binary/VARIANT literal representations.
-- Expected decimals come from their original base-10 coefficients, not server output.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_scalar;
CREATE TABLE ${case_db}.t_insert_exact_scalar (id INT, d DECIMAL(18,9));
INSERT INTO ${case_db}.t_insert_exact_scalar VALUES (1, 123456789.123456789);

-- query 2
-- Keep this first assertion independent of arithmetic, containers, and explicit CAST.
SELECT id, d FROM ${case_db}.t_insert_exact_scalar ORDER BY id;

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_insert_exact_scalar VALUES
  (2, -123456789.123456789), (3, NULL);
INSERT INTO ${case_db}.t_insert_exact_scalar SELECT 4, 123456789.123456789;
INSERT INTO ${case_db}.t_insert_exact_scalar
  SELECT 5, 123456789.123456789 UNION ALL SELECT 6, 1e0;
INSERT INTO ${case_db}.t_insert_exact_scalar VALUES
  (7, CAST(1.2356 AS DECIMAL(10,2))),
  (8, CAST(2.5 AS DOUBLE)),
  (9, -2E0),
  (10, CAST(NULL AS DECIMAL(18,9))),
  (11, 123456789.123456789);

-- query 4
SELECT id, d FROM ${case_db}.t_insert_exact_scalar ORDER BY id;

-- query 5
-- The product coefficient is 123456789123456789 squared, at scale 18.
SELECT CAST(d * d AS DECIMAL(38,18)) AS squared
FROM ${case_db}.t_insert_exact_scalar WHERE id = 1;

-- query 6
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_wide;
CREATE TABLE ${case_db}.t_insert_exact_wide (
  id INT, d DECIMAL(38,10), n DECIMAL(38,0)
);
INSERT INTO ${case_db}.t_insert_exact_wide VALUES
  (1, 1234567890.1234567890, 9223372036854775808),
  (2, -1234567890.1234567890, -9223372036854775809),
  (3, 0.0000000001, NULL);

-- query 7
SELECT id, d, n FROM ${case_db}.t_insert_exact_wide ORDER BY id;

-- query 8
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_nested;
CREATE TABLE ${case_db}.t_insert_exact_nested (
  id INT,
  a ARRAY<DECIMAL(18,9)>,
  m MAP<DECIMAL(18,9),DECIMAL(18,9)>,
  s STRUCT<d DECIMAL(18,9), a ARRAY<DECIMAL(18,9)>>
);
INSERT INTO ${case_db}.t_insert_exact_nested VALUES
  (1, [123456789.123456789, -123456789.123456789, NULL],
      map{123456789.123456789:-123456789.123456789},
      row(123456789.123456789, [-123456789.123456789, NULL])),
  (2, [], map{}, NULL),
  (3, NULL, NULL, NULL),
  (4, [1.0, 2.50], map{1:2.50}, row(NULL, []));
INSERT INTO ${case_db}.t_insert_exact_nested
SELECT 5, [-123456789.123456789, NULL],
       map{123456789.123456789:NULL},
       named_struct('d', -123456789.123456789, 'a', [123456789.123456789]);
INSERT INTO ${case_db}.t_insert_exact_nested VALUES
  (6, [123456789.123456789, 1e0], NULL,
      row(123456789.123456789, [123456789.123456789, 1e0]));

-- query 9
SELECT id, a[1] AS first_value, a[2] AS second_value,
       a[3] AS third_value, array_length(a) AS element_count,
       a IS NULL AS whole_null
FROM ${case_db}.t_insert_exact_nested ORDER BY id;

-- query 10
-- Read keys and values without introducing a mixed-scale lookup comparison.
SELECT id, map_keys(m)[1] AS first_key, map_values(m)[1] AS first_value,
       map_size(m) AS entry_count, m IS NULL AS whole_null
FROM ${case_db}.t_insert_exact_nested ORDER BY id;

-- query 11
SELECT id, s.d AS decimal_field, s.a[1] AS nested_first,
       s.a[2] AS nested_second, array_length(s.a) AS nested_count,
       s IS NULL AS whole_null
FROM ${case_db}.t_insert_exact_nested ORDER BY id;

-- query 12
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_controls;
CREATE TABLE ${case_db}.t_insert_exact_controls (id INT, d DECIMAL(10,2));
INSERT INTO ${case_db}.t_insert_exact_controls SELECT 99, 9.99 WHERE FALSE;
INSERT INTO ${case_db}.t_insert_exact_controls SELECT 98, 9.98 LIMIT 0;
INSERT INTO ${case_db}.t_insert_exact_controls SELECT DISTINCT 1, 1.2344 WHERE TRUE;
INSERT INTO ${case_db}.t_insert_exact_controls SELECT 2, CAST(1.2356 AS DECIMAL(10,4)) LIMIT 1;
INSERT INTO ${case_db}.t_insert_exact_controls VALUES (3, CAST(2.5 AS DOUBLE));

-- query 13
SELECT id, d FROM ${case_db}.t_insert_exact_controls ORDER BY id;

-- query 14
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_bytes;
CREATE TABLE ${case_db}.t_insert_exact_bytes (id INT, b VARBINARY, v VARIANT)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.t_insert_exact_bytes VALUES
  (1, 'ÿ', parse_json('{"text":"values-direct"}')),
  (2, X'FF00', CAST(parse_json('{"text":"values-cast"}') AS VARIANT));
INSERT INTO ${case_db}.t_insert_exact_bytes
SELECT 3, 'ÿ', parse_json('{"text":"select-direct"}');
INSERT INTO ${case_db}.t_insert_exact_bytes
SELECT 4, X'FF00', CAST(parse_json('{"text":"select-cast"}') AS VARIANT);
INSERT INTO ${case_db}.t_insert_exact_bytes VALUES (5, NULL, NULL);
INSERT INTO ${case_db}.t_insert_exact_bytes
VALUES (6, X'FF', parse_json(CAST('{"text":"values-varchar-cast"}' AS VARCHAR)));
INSERT INTO ${case_db}.t_insert_exact_bytes
SELECT 7, X'FF00', parse_json(CAST('{"text":"select-varchar-cast"}' AS VARCHAR));

-- query 15
-- Logical VARIANT decoding checks that the stored bytes are canonical payloads.
-- LATIN1 U+00FF is one byte FF; a UTF8 rewrite would incorrectly write C3BF.
SELECT id, hex(b) AS bytes_hex, variant_typeof(v) AS variant_kind,
       get_json_string(v, '$.text') AS text_value
FROM ${case_db}.t_insert_exact_bytes ORDER BY id;

-- query 16
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_source_order;
CREATE TABLE ${case_db}.t_insert_exact_source_order (a INT, b INT);
-- ORDER BY 1 denotes source b, before mapping the source to target (a,b).
INSERT INTO ${case_db}.t_insert_exact_source_order (b,a)
VALUES (1,2), (2,1) ORDER BY 1 LIMIT 1;

-- query 17
SELECT a, b FROM ${case_db}.t_insert_exact_source_order ORDER BY a, b;

-- query 18
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_having;
CREATE TABLE ${case_db}.t_insert_exact_having (d DECIMAL(3,0));
-- HAVING must resolve x to the source 1.4, before the sink narrows it to 1.
INSERT INTO ${case_db}.t_insert_exact_having SELECT 1.4 AS x HAVING x = 1.4;

-- query 19
SELECT d FROM ${case_db}.t_insert_exact_having ORDER BY d;

-- query 20
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_insert_exact_union_distinct;
CREATE TABLE ${case_db}.t_insert_exact_union_distinct (d DECIMAL(5,2));
-- Explicit DECIMAL CAST retains the already supported general query admission path.
INSERT INTO ${case_db}.t_insert_exact_union_distinct
SELECT CAST(1 AS DECIMAL(5,2)) UNION SELECT CAST(1 AS DECIMAL(5,2));

-- query 21
SELECT d FROM ${case_db}.t_insert_exact_union_distinct ORDER BY d;

-- query 22
-- @skip_result_check=true
DROP TABLE ${case_db}.t_insert_exact_scalar;
DROP TABLE ${case_db}.t_insert_exact_wide;
DROP TABLE ${case_db}.t_insert_exact_nested;
DROP TABLE ${case_db}.t_insert_exact_controls;
DROP TABLE ${case_db}.t_insert_exact_bytes;
DROP TABLE ${case_db}.t_insert_exact_source_order;
DROP TABLE ${case_db}.t_insert_exact_having;
DROP TABLE ${case_db}.t_insert_exact_union_distinct;
