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
-- SQLX-2 owner coverage: a MOR UPDATE ... FROM rejects duplicate source
-- matches with the keyed row-lineage assertion before staging a write.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.sqlx2_mor_update_target FORCE;
DROP TABLE IF EXISTS ${case_db}.sqlx2_mor_update_source FORCE;
CREATE TABLE ${case_db}.sqlx2_mor_update_target (
  id BIGINT,
  v STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true",
  "novarocks.update.mode" = "merge-on-read"
);
CREATE TABLE ${case_db}.sqlx2_mor_update_source (
  id BIGINT,
  new_v STRING
) TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.sqlx2_mor_update_target VALUES (1, 'a');
INSERT INTO ${case_db}.sqlx2_mor_update_source VALUES (1, 'x'), (1, 'y');

-- query 2
-- @expect_error=[query.execution.invalid_data] Data failure (reason 7:317)
UPDATE ${case_db}.sqlx2_mor_update_target AS t
SET v = s.new_v
FROM ${case_db}.sqlx2_mor_update_source AS s
WHERE t.id = s.id;

-- query 3
SELECT id, v
FROM ${case_db}.sqlx2_mor_update_target
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.sqlx2_mor_update_target FORCE;
DROP TABLE ${case_db}.sqlx2_mor_update_source FORCE;
