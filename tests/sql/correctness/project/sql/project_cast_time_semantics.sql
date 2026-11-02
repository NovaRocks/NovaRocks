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
-- @tags=project,cast,time
-- Test Objective:
-- 1. Validate CAST(... AS TIME) literal parsing semantics.
-- 2. Distinguish direct string-to-time cast from datetime-to-time cast path.
-- Test Flow:
-- 1. Cast canonical and extended-hour time strings.
-- 2. Cast datetime/date values to TIME.
-- 3. Assert deterministic scalar output.
SELECT
  CAST('00:00:00' AS TIME) AS t0,
  SEC_TO_TIME(CAST(CAST(CAST('25:00:00' AS TIME) AS BIGINT) / 1000000 AS BIGINT)) AS t25,
  CAST('1970-01-01 01:01:01' AS TIME) AS t_dt_literal,
  CAST(CAST('1970-01-01 01:01:01' AS DATETIME) AS TIME) AS t_from_datetime,
  CAST(CAST('2020-01-02' AS DATE) AS TIME) AS t_from_date;
