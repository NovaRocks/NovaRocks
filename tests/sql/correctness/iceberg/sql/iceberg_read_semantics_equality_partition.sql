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
-- Test Point: Iceberg equality delete visibility across partition evolution
-- Method: add an equality delete on an unpartitioned table, evolve to a partitioned spec, insert a new row with the deleted key, and verify sequence-aware visibility
-- Scope: ordinary Iceberg SELECT over current snapshot live rows

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.read_sem_eq_part ;
CREATE TABLE ${case_db}.read_sem_eq_part (
  id BIGINT NOT NULL,
  city VARCHAR,
  amount BIGINT
) WITH (format_version = 2);
INSERT INTO ${case_db}.read_sem_eq_part VALUES
  ROW(1, 'A', 10),
  ROW(2, 'B', 20);
ALTER TABLE ${case_db}.read_sem_eq_part EXECUTE add_equality_delete(columns => ARRAY['id'], rows => ARRAY[ROW(2)]);

-- query 2
SELECT id, city, amount
FROM ${case_db}.read_sem_eq_part
ORDER BY id;

-- query 3
-- @skip_result_check=true
ALTER TABLE ${case_db}.read_sem_eq_part SET PROPERTIES partitioning = ARRAY['city'];
INSERT INTO ${case_db}.read_sem_eq_part VALUES
  ROW(2, 'B', 25),
  ROW(3, 'A', 30);

-- query 4
SELECT id, city, amount
FROM ${case_db}.read_sem_eq_part
ORDER BY id, amount;

-- query 5
-- @skip_result_check=true
DROP TABLE ${case_db}.read_sem_eq_part ;
