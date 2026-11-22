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
-- @tags=iceberg_dml,datetime
-- Test Objective:
-- 1. Validate DATE/DATETIME values are persisted correctly through INSERT-SELECT into Iceberg sink.
-- 2. Cover leap-day and epoch-style values together with NULL temporal fields.
DROP TABLE IF EXISTS ${case_db}.t_temporal_insert_src;
DROP TABLE IF EXISTS ${case_db}.t_temporal_insert_sink;
CREATE TABLE ${case_db}.t_temporal_insert_src (
  id BIGINT,
  d DATE,
  dt TIMESTAMP(6)
);
CREATE TABLE ${case_db}.t_temporal_insert_sink (
  id BIGINT,
  d DATE,
  dt TIMESTAMP(6)
);
INSERT INTO ${case_db}.t_temporal_insert_src VALUES
  ROW(1, DATE '1970-01-01', TIMESTAMP '1970-01-01 00:00:00'),
  ROW(2, DATE '2024-02-29', TIMESTAMP '2024-02-29 23:59:59'),
  ROW(3, NULL, NULL);
INSERT INTO ${case_db}.t_temporal_insert_sink
SELECT id, d, dt
FROM ${case_db}.t_temporal_insert_src;
SELECT id, d, dt
FROM ${case_db}.t_temporal_insert_sink
ORDER BY id;
