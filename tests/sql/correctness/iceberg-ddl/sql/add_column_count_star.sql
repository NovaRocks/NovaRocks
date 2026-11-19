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
-- @tags=iceberg_ddl
-- Test Objective:
-- 1. Validate ALTER TABLE ADD COLUMN on an Iceberg table preserves COUNT(*).
-- 2. Verify the new column is visible with NULL backfill after schema change.
-- Iceberg ALTER is synchronous at metadata commit, so no sleep/retry is needed
-- between the ALTER and the follow-up reads.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t0;
CREATE TABLE ${case_db}.t0 (
  k1 INT,
  c1 INT
);
INSERT INTO ${case_db}.t0 VALUES ROW(1, 1);

-- query 2
-- @order_sensitive=true
SELECT count(*) AS row_count FROM ${case_db}.t0;

-- query 3
-- @order_sensitive=true
SELECT k1, c1 FROM ${case_db}.t0 ORDER BY k1;

-- query 4
-- @skip_result_check=true
ALTER TABLE ${case_db}.t0 ADD COLUMN b1 BOOLEAN;

-- query 5
-- @order_sensitive=true
SELECT count(*) AS row_count FROM ${case_db}.t0;

-- query 6
-- @order_sensitive=true
SELECT k1, c1, b1 FROM ${case_db}.t0 ORDER BY k1;
