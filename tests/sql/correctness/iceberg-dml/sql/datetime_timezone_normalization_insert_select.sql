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
-- @tags=iceberg_dml,datetime,timezone
-- Test Objective:
-- 1. Validate timezone-tagged temporal rows can be normalized to deterministic DATETIME values before sink writes.
-- 2. Validate normalized DATETIME values remain stable after persistence.
DROP TABLE IF EXISTS ${case_db}.t_datetime_tz_src;
DROP TABLE IF EXISTS ${case_db}.t_datetime_tz_sink;
CREATE TABLE ${case_db}.t_datetime_tz_src (
  id BIGINT,
  local_dt VARCHAR,
  tz VARCHAR
);
CREATE TABLE ${case_db}.t_datetime_tz_sink (
  id BIGINT,
  local_dt VARCHAR,
  tz VARCHAR,
  normalized_dt TIMESTAMP(6)
);
INSERT INTO ${case_db}.t_datetime_tz_src VALUES
  ROW(1, '2024-01-01 08:00:00', '+08:00'),
  ROW(2, '2023-12-31 19:00:00', '-05:00'),
  ROW(3, '2024-01-01 00:00:00', '+00:00'),
  ROW(4, '2024-01-01 08:00:00', '+08:00');
INSERT INTO ${case_db}.t_datetime_tz_sink
SELECT id, local_dt, tz, CASE WHEN (local_dt IS NULL) THEN NULL WHEN (tz = '+08:00') THEN CAST('2024-01-01 00:00:00' AS TIMESTAMP) WHEN (tz = '-05:00') THEN CAST('2024-01-01 00:00:00' AS TIMESTAMP) WHEN (tz = '+00:00') THEN CAST(local_dt AS TIMESTAMP) ELSE NULL END AS normalized_dt FROM ${case_db}.t_datetime_tz_src;
SELECT
  id,
  tz,
  normalized_dt,
  YEAR(normalized_dt) AS y
FROM ${case_db}.t_datetime_tz_sink
ORDER BY id;
