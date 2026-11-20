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

-- Validate UK/FK-based table prune and aggregate elimination on Iceberg tables.
-- query 1
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.txn
-- @result_not_contains=TABLE: iceberg_pkfk_db_${uuid0}.payment
CREATE SCHEMA iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0};
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0};
CREATE TABLE payment (
  id INT,
  created_at DATE,
  status STRING
);
CREATE TABLE txn (
  id INT,
  created_at DATE,
  payment_id INT
);
INSERT INTO payment VALUES
  ROW(1, '2025-01-01', 'COMPLETED'),
  ROW(2, '2025-01-02', 'PENDING'),
  ROW(3, '2025-01-03', 'FAILED'),
  ROW(4, '2025-01-04', 'COMPLETED'),
  ROW(5, '2025-01-05', 'REFUNDED');
INSERT INTO txn VALUES
  ROW(101, '2025-01-01', 1),
  ROW(102, '2025-01-02', 2),
  ROW(103, '2025-01-03', 3),
  ROW(104, '2025-01-04', 4),
  ROW(105, '2025-01-05', 5),
  ROW(106, '2025-01-06', 1);
ALTER TABLE payment SET PROPERTIES extra_properties = MAP(ARRAY['unique_constraints'], ARRAY['id']);
ALTER TABLE txn SET PROPERTIES extra_properties = MAP(ARRAY['foreign_key_constraints'], ARRAY['(payment_id) REFERENCES payment(id)']);
SET SESSION enable_ukfk_opt = false;
SET SESSION enable_query_rewrite_table_prune = true;
SET SESSION enable_cbo_table_prune = true;
SET SESSION enable_table_prune_on_update = true;
EXPLAIN
SELECT txn.id, txn.created_at, txn.payment_id
FROM txn
LEFT JOIN payment
  ON payment.id = txn.payment_id;

-- query 2
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.payment
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.txn
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0};
EXPLAIN
SELECT txn.id, txn.created_at, txn.payment_id
FROM txn
INNER JOIN payment
  ON payment.id = txn.payment_id;

-- query 3
-- @result_contains=TABLE: iceberg_pkfk_db_${uuid0}.txn
-- @result_contains=payment_id IS NOT NULL
-- @result_not_contains=TABLE: iceberg_pkfk_db_${uuid0}.payment
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0};
SET SESSION enable_ukfk_opt = true;
SET SESSION enable_query_rewrite_table_prune = false;
SET SESSION enable_cbo_table_prune = false;
SET SESSION enable_table_prune_on_update = false;
EXPLAIN
SELECT txn.id, txn.created_at, txn.payment_id
FROM txn
INNER JOIN payment
  ON payment.id = txn.payment_id;

-- query 4
-- @result_contains=AGGREGATE
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0};
SET SESSION enable_eliminate_agg = false;
EXPLAIN
SELECT COUNT(1) AS cnt, payment.id
FROM payment
GROUP BY payment.id;

-- query 5
-- @result_not_contains=AGGREGATE
USE iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0};
SET SESSION enable_eliminate_agg = true;
EXPLAIN
SELECT COUNT(1) AS cnt, payment.id
FROM payment
GROUP BY payment.id;
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0}.txn ;
DROP TABLE iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0}.payment ;
DROP SCHEMA iceberg_ddl_cat_${suite_uuid0}.iceberg_pkfk_db_${uuid0};
