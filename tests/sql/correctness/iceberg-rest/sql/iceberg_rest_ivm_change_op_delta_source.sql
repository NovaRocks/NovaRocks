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

-- @sequential=true
-- @order_sensitive=true
-- @tags=iceberg_rest,mv,ivm,change_op,projection,aggregate,overwrite
-- Test Point:
--   Validate Iceberg-backed MV refresh over an Iceberg REST v3 row-lineage table
--   uses tagged delta source semantics for both projection and signed aggregate
--   SUM overwrite changes, including full aggregate-group retraction.

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS iceberg_rest_${suite_uuid0}.ivm_${uuid0} FORCE;
CREATE DATABASE iceberg_rest_${suite_uuid0}.ivm_${uuid0};
CREATE TABLE iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0} (
  id INT NOT NULL,
  customer STRING,
  amount BIGINT
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0} VALUES
  (1, 'Alice', 100),
  (2, 'Bob', 40);
USE iceberg_rest_${suite_uuid0}.ivm_${uuid0};
CREATE MATERIALIZED VIEW orders_projection_mv_${uuid0}
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT id, amount
FROM iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0};
CREATE MATERIALIZED VIEW orders_sum_mv_${uuid0}
DISTRIBUTED BY HASH(customer) BUCKETS 1
PROPERTIES('storage_engine' = 'iceberg')
AS SELECT customer, SUM(amount) AS total_amount
FROM iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0}
GROUP BY customer;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_projection_mv_${uuid0};
REFRESH MATERIALIZED VIEW orders_sum_mv_${uuid0};

-- query 3
SELECT id, amount
FROM orders_projection_mv_${uuid0}
ORDER BY id;

-- query 4
SELECT customer, total_amount
FROM orders_sum_mv_${uuid0}
ORDER BY customer;

-- query 5
-- @skip_result_check=true
INSERT OVERWRITE iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0}
VALUES (1, 'Alice', 80), (2, 'Bob', 40);

-- query 6
SELECT id, customer, amount
FROM iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0}
ORDER BY id;

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_projection_mv_${uuid0};
REFRESH MATERIALIZED VIEW orders_sum_mv_${uuid0};

-- query 8
SELECT id, amount
FROM orders_projection_mv_${uuid0}
ORDER BY id;

-- query 9
SELECT customer, total_amount
FROM orders_sum_mv_${uuid0}
ORDER BY customer;

-- query 10
-- @skip_result_check=true
INSERT OVERWRITE iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0}
VALUES (2, 'Bob', 40);

-- query 11
SELECT id, customer, amount
FROM iceberg_rest_${suite_uuid0}.ivm_${uuid0}.orders_${uuid0}
ORDER BY id;

-- query 12
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_projection_mv_${uuid0};
REFRESH MATERIALIZED VIEW orders_sum_mv_${uuid0};

-- query 13
SELECT id, amount
FROM orders_projection_mv_${uuid0}
ORDER BY id;

-- query 14
SELECT customer, total_amount
FROM orders_sum_mv_${uuid0}
ORDER BY customer;

-- query 15
-- @cleanup=true
-- @skip_result_check=true
USE iceberg_rest_${suite_uuid0}.ivm_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ivm_${uuid0}.orders_sum_mv_${uuid0};
DROP MATERIALIZED VIEW IF EXISTS ivm_${uuid0}.orders_projection_mv_${uuid0};
DROP DATABASE IF EXISTS iceberg_rest_${suite_uuid0}.ivm_${uuid0} FORCE;
