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
-- Validate top-level Iceberg schema evolution over the local Hadoop-style catalog.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
DROP TABLE IF EXISTS orders_local;
CREATE TABLE orders_local (
  id INT,
  amount FLOAT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO orders_local VALUES (1, 10.5), (2, 20.25);
ALTER TABLE orders_local ADD COLUMN note_text STRING DEFAULT NULL;

-- query 2
SELECT id, amount, note_text FROM orders_local ORDER BY id;

-- query 3
INSERT INTO orders_local (id, amount, note_text) VALUES (3, 30.75, 'new');

-- query 4
SELECT id, amount, note_text FROM orders_local ORDER BY id;

-- query 5
ALTER TABLE orders_local RENAME COLUMN amount TO total_amount;

-- query 6
SELECT id, total_amount, note_text FROM orders_local ORDER BY id;

-- query 7
ALTER TABLE orders_local MODIFY COLUMN id BIGINT;

-- query 8
SELECT id + 10000000000 AS widened_id, total_amount FROM orders_local ORDER BY id;

-- query 9
ALTER TABLE orders_local DROP COLUMN note_text;

-- query 10
SELECT * FROM orders_local ORDER BY id;

-- query 11
-- @expect_error=Column 'note_text' cannot be resolved
SELECT note_text FROM orders_local;

-- query 12
ALTER TABLE orders_local ADD COLUMN note_text STRING DEFAULT NULL;

-- query 13
SELECT id, total_amount, note_text FROM orders_local ORDER BY id;

-- query 14
INSERT INTO orders_local (id, total_amount, note_text) VALUES (4, 40.5, 'fresh');

-- query 15
SELECT id, total_amount, note_text FROM orders_local ORDER BY id;

-- query 16
-- @expect_error=Iceberg schema evolution cannot modify reserved column
ALTER TABLE orders_local DROP COLUMN _row_id;

-- query 17
-- @skip_result_check=true
ALTER TABLE orders_local ADD EQUALITY DELETE (id) VALUES (1);

-- query 18
-- @expect_error=DROP COLUMN `id` is blocked because an Iceberg equality-delete file references `id`
ALTER TABLE orders_local DROP COLUMN id;

-- query 19
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_local_${uuid0}.orders_local FORCE;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_local_${uuid0};
