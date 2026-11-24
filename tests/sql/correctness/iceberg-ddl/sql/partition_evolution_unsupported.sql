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

-- ADD EQUALITY DELETE refuses a table with an evolved partition spec.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_partition_evolution_unsupported ;
CREATE TABLE ${case_db}.t_partition_evolution_unsupported (
  id BIGINT,
  user_id BIGINT,
  score INT
) WITH (partitioning = ARRAY['bucket(user_id, 4)'], format_version = 2);
INSERT INTO ${case_db}.t_partition_evolution_unsupported VALUES ROW(1, 10, 100);
ALTER TABLE ${case_db}.t_partition_evolution_unsupported SET PROPERTIES partitioning = ARRAY[];
ALTER TABLE ${case_db}.t_partition_evolution_unsupported SET PROPERTIES partitioning = ARRAY['bucket(user_id, 8)'];

-- query 2
-- @expect_error=ADD EQUALITY DELETE on an evolved Iceberg table is not supported yet
ALTER TABLE ${case_db}.t_partition_evolution_unsupported EXECUTE add_equality_delete(columns => ARRAY['id'], rows => ARRAY[ROW(1)]);

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.t_partition_evolution_unsupported ;
