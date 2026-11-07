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
-- Validate top-level Iceberg schema evolution against an S3-backed catalog.

-- query 1
CREATE DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
DROP TABLE IF EXISTS orders_s3;
CREATE TABLE orders_s3 (
  id INT,
  amount FLOAT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO orders_s3 VALUES (1, 10.5), (2, 20.25);
ALTER TABLE orders_s3 ADD COLUMN note_text STRING DEFAULT NULL;

-- query 2
SELECT id, amount, note_text FROM orders_s3 ORDER BY id;

-- query 3
INSERT INTO orders_s3 (id, amount, note_text) VALUES (3, 30.75, 's3-new');
ALTER TABLE orders_s3 RENAME COLUMN amount TO total_amount;

-- query 4
SELECT id, total_amount, note_text FROM orders_s3 ORDER BY id;

-- query 5
ALTER TABLE orders_s3 MODIFY COLUMN id BIGINT;
ALTER TABLE orders_s3 DROP COLUMN note_text;

-- query 6
SELECT id + 10000000000 AS widened_id, total_amount FROM orders_s3 ORDER BY id;

-- query 7
ALTER TABLE orders_s3 ADD COLUMN note_text STRING DEFAULT NULL;

-- query 8
SELECT id, total_amount, note_text FROM orders_s3 ORDER BY id;

-- query 9
INSERT INTO orders_s3 (id, total_amount, note_text) VALUES (4, 40.5, 's3-fresh');

-- query 10
SELECT id, total_amount, note_text FROM orders_s3 ORDER BY id;

-- query 11
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_s3_${uuid0}.orders_s3 FORCE;
DROP DATABASE iceberg_ddl_cat_${suite_uuid0}.schema_evolution_s3_${uuid0};
