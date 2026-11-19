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
-- Test Point: Iceberg v3 deletion-vector DELETE works across historical partition specs
-- Method: create a v3 row-lineage bucket-partitioned table, insert old-spec rows, evolve to a new bucket spec, insert new-spec rows, delete rows from both specs, and verify remaining rows
-- Scope: standalone Iceberg table DDL, INSERT INTO, SELECT, DELETE FROM, v3 row-lineage deletion vectors

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_v3_delete ;
CREATE TABLE ${case_db}.t_partition_evolution_v3_delete (
  id BIGINT,
  user_id BIGINT,
  score INT
) WITH (partitioning = ARRAY['bucket(user_id, 4)'], format_version = 3);
INSERT INTO ${case_db}.t_partition_evolution_v3_delete VALUES
  ROW(1, 10, 100),
  ROW(2, 20, 200),
  ROW(3, 30, 300);
ALTER TABLE ${case_db}.t_partition_evolution_v3_delete SET PROPERTIES partitioning = ARRAY[];
ALTER TABLE ${case_db}.t_partition_evolution_v3_delete SET PROPERTIES partitioning = ARRAY['bucket(user_id, 8)'];
INSERT INTO ${case_db}.t_partition_evolution_v3_delete VALUES
  ROW(4, 40, 400),
  ROW(5, 50, 500),
  ROW(6, 60, 600);
DELETE FROM ${case_db}.t_partition_evolution_v3_delete WHERE id IN (2, 5);

-- query 2
SELECT COUNT(*) AS cnt
FROM ${case_db}.t_partition_evolution_v3_delete;

-- query 3
SELECT id, user_id, score
FROM ${case_db}.t_partition_evolution_v3_delete
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.t_partition_evolution_v3_delete ;
