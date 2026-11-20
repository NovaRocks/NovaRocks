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
-- matches with the keyed physical-position assertion before staging a write.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.sqlx2_mor_update_target ;
DROP TABLE IF EXISTS ${case_db}.sqlx2_mor_update_source ;
CREATE TABLE ${case_db}.sqlx2_mor_update_target (
  id BIGINT,
  v STRING
) WITH (format_version = 3, extra_properties = MAP(ARRAY['novarocks.update.mode'], ARRAY['merge-on-read']));
CREATE TABLE ${case_db}.sqlx2_mor_update_source (
  id BIGINT,
  new_v STRING
) WITH (format_version = 3);
INSERT INTO ${case_db}.sqlx2_mor_update_target VALUES ROW(1, 'a');
INSERT INTO ${case_db}.sqlx2_mor_update_source VALUES ROW(1, 'x'), ROW(1, 'y');

-- query 2
-- @expect_error_tier=drift
-- @expect_error=TrinoError { name=MERGE_TARGET_ROW_MULTIPLE_MATCHES type=USER_ERROR code=119 }: One MERGE target table row matched more than one source row
MERGE INTO ${case_db}.sqlx2_mor_update_target AS t
USING ${case_db}.sqlx2_mor_update_source AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET v = s.new_v;

-- query 3
SELECT id, v
FROM ${case_db}.sqlx2_mor_update_target
ORDER BY id;

-- query 4
-- @skip_result_check=true
DROP TABLE ${case_db}.sqlx2_mor_update_target ;
DROP TABLE ${case_db}.sqlx2_mor_update_source ;
