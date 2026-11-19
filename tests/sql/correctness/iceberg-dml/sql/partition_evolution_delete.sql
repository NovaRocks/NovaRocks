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
-- Test Point: Iceberg DELETE works across historical partition specs
-- Method: create a bucket-partitioned table, insert old-spec rows, evolve to a new bucket spec, insert new-spec rows, delete rows from both specs, and verify remaining rows
-- Scope: standalone Iceberg table DDL, INSERT INTO, SELECT, DELETE FROM

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_delete FORCE;
CREATE TABLE ${case_db}.t_partition_evolution_delete (
  id BIGINT,
  user_id BIGINT,
  score INT
)
PARTITION BY bucket(user_id, 4)
TBLPROPERTIES ("format-version" = "2");
INSERT INTO ${case_db}.t_partition_evolution_delete VALUES
  (1, 10, 100),
  (2, 20, 200),
  (3, 30, 300);
ALTER TABLE ${case_db}.t_partition_evolution_delete DROP PARTITION COLUMN bucket(user_id, 4);
ALTER TABLE ${case_db}.t_partition_evolution_delete ADD PARTITION COLUMN bucket(user_id, 8);
INSERT INTO ${case_db}.t_partition_evolution_delete VALUES
  (4, 40, 400),
  (5, 50, 500),
  (6, 60, 600);
DELETE FROM ${case_db}.t_partition_evolution_delete WHERE id IN (2, 5);

-- query 2
SELECT COUNT() AS cnt
FROM ${case_db}.t_partition_evolution_delete;

-- query 3
SELECT id, user_id, score
FROM ${case_db}.t_partition_evolution_delete
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_partition_evolution_delete FORCE;
