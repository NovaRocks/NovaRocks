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
-- Validate static Iceberg scan pruning preserves results for identity partitions and file stats.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_scan_pruning;
CREATE TABLE ${case_db}.t_scan_pruning (
  id INT,
  part_id INT,
  payload STRING
)
PARTITION BY (part_id);
INSERT INTO ${case_db}.t_scan_pruning VALUES
  (1, 1, 'cold-a'),
  (2, 1, 'cold-b'),
  (12, 12, 'target'),
  (13, 12, 'neighbor');

-- query 2
-- @skip_result_check=true
SET enable_connector_static_predicate_pushdown = false;

-- query 3
SELECT id, part_id, payload
FROM ${case_db}.t_scan_pruning
WHERE part_id = 12 AND id = 12
ORDER BY id;

-- query 4
-- @skip_result_check=true
SET enable_connector_static_predicate_pushdown = true;

-- query 5
SELECT id, part_id, payload
FROM ${case_db}.t_scan_pruning
WHERE part_id = 12 AND id = 12
ORDER BY id;

-- query 6
SELECT COUNT() AS cnt
FROM ${case_db}.t_scan_pruning
WHERE part_id = 99 AND id = 99;

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.t_scan_pruning FORCE;
