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
-- Validate equality-delete read semantics across Iceberg schema evolution.

-- query 1
CREATE DATABASE iceberg_dml_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0};
USE iceberg_dml_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0};
DROP TABLE IF EXISTS orders_eq_evo;
CREATE TABLE orders_eq_evo (
  id INT,
  amount FLOAT
) TBLPROPERTIES (
  "format-version" = "2"
);
INSERT INTO orders_eq_evo VALUES
  (1, 10.5),
  (2, 20.25),
  (3, 30.75);

-- query 2
SELECT count(*) AS snapshot_count_before_equality_delete
  FROM iceberg_dml_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0}.orders_eq_evo$snapshots;

-- query 3
ALTER TABLE orders_eq_evo ADD EQUALITY DELETE (amount) VALUES (20.25);

-- query 4
SELECT count(*) AS snapshot_count_after_equality_delete
  FROM iceberg_dml_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0}.orders_eq_evo$snapshots;

-- query 5
ALTER TABLE orders_eq_evo RENAME COLUMN amount TO total_amount;
ALTER TABLE orders_eq_evo MODIFY COLUMN total_amount DOUBLE;

-- query 6
SELECT id, total_amount FROM orders_eq_evo ORDER BY id;

-- query 7
DROP TABLE iceberg_dml_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0}.orders_eq_evo FORCE;
DROP DATABASE iceberg_dml_cat_${suite_uuid0}.eq_delete_schema_evolution_${uuid0};
