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
-- @tags=iceberg_dml,datetime,invalid_literal
-- Test Objective:
-- 1. Validate invalid temporal literal rows can be sanitized to NULL before DATETIME sink writes.
-- 2. Validate valid temporal literals are still persisted correctly in the same batch.
DROP TABLE IF EXISTS ${case_db}.t_datetime_literal_src;
DROP TABLE IF EXISTS ${case_db}.t_datetime_literal_sink;
CREATE TABLE ${case_db}.t_datetime_literal_src (
  id BIGINT,
  raw_dt STRING
);
CREATE TABLE ${case_db}.t_datetime_literal_sink (
  id BIGINT,
  dt DATETIME
);
INSERT INTO ${case_db}.t_datetime_literal_src VALUES
  ROW(1, '2024-02-29 12:34:56'),
  ROW(2, '2024-02-30 00:00:00'),
  ROW(3, 'not-a-datetime'),
  ROW(4, NULL);
INSERT INTO ${case_db}.t_datetime_literal_sink
SELECT id, CASE WHEN (raw_dt IS NULL) THEN NULL WHEN (raw_dt = '2024-02-29 12:34:56') THEN CAST('2024-02-29 12:34:56' AS TIMESTAMP) ELSE NULL END AS dt FROM ${case_db}.t_datetime_literal_src;
SELECT
  id,
  dt,
  YEAR(dt) AS y
FROM ${case_db}.t_datetime_literal_sink
ORDER BY id;
