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
-- @tags=iceberg_dml,cast
-- Test Objective:
-- 1. Regression coverage for writing INT target columns through Iceberg sink.
-- 2. Validate INT INSERT casts with statement-local integer inputs.
-- query 1
DROP TABLE IF EXISTS ${case_db}.t_int_insert_regression;
-- query 2
CREATE TABLE ${case_db}.t_int_insert_regression (
  id INT,
  v INT
);
-- query 3
-- query 4
INSERT INTO ${case_db}.t_int_insert_regression VALUES ROW(1, 1);
-- query 5
INSERT INTO ${case_db}.t_int_insert_regression VALUES ROW(2, 2);
-- query 6
INSERT INTO ${case_db}.t_int_insert_regression VALUES ROW(3, 3);
-- query 7
SELECT id, v
FROM ${case_db}.t_int_insert_regression
ORDER BY id;

-- query 8
CREATE TABLE ${case_db}.t_bigint_source_cast (v BIGINT);

-- query 9
INSERT INTO ${case_db}.t_bigint_source_cast
VALUES (BIGINT '9007199254740993'), (DOUBLE '1.0');

-- query 10
INSERT INTO ${case_db}.t_bigint_source_cast
SELECT BIGINT '9007199254740995' UNION ALL SELECT DOUBLE '2.0';

-- query 11
SELECT v FROM ${case_db}.t_bigint_source_cast ORDER BY v;

-- query 12
SELECT count(*) AS snapshots FROM ${case_db}."t_bigint_source_cast$snapshots";

-- query 13
-- @expect_trino_error=TYPE_MISMATCH,USER_ERROR
INSERT INTO ${case_db}.t_bigint_source_cast VALUES ('12');

-- query 14
-- @expect_trino_error=NUMERIC_VALUE_OUT_OF_RANGE,USER_ERROR
INSERT INTO ${case_db}.t_bigint_source_cast VALUES (DECIMAL '9223372036854775808');

-- query 15
SELECT v FROM ${case_db}.t_bigint_source_cast ORDER BY v;

-- query 16
SELECT count(*) AS snapshots FROM ${case_db}."t_bigint_source_cast$snapshots";

-- query 17
CREATE TABLE ${case_db}.t_nullable_source_cast (id INTEGER, v BIGINT, items ARRAY(VARBINARY));

-- query 18
INSERT INTO ${case_db}.t_nullable_source_cast (id, v)
VALUES (1, BIGINT '9007199254740993'), (2, DOUBLE '1.0'), (3, NULL);

-- query 19
INSERT INTO ${case_db}.t_nullable_source_cast (id, items)
SELECT 4, s.x FROM (SELECT ARRAY[] AS x) AS s;

-- query 20
INSERT INTO ${case_db}.t_nullable_source_cast (id, items)
SELECT 5, CASE WHEN TRUE THEN ARRAY[] ELSE NULL END;

-- query 21
INSERT INTO ${case_db}.t_nullable_source_cast (id, items)
SELECT 6, CASE WHEN FALSE THEN ARRAY[] ELSE NULL END;

-- query 22
SELECT id, v, items
FROM ${case_db}.t_nullable_source_cast ORDER BY id;
