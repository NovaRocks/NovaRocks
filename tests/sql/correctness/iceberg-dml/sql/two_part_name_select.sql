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
-- @tags=iceberg_dml,resolve
-- Test Objective:
-- 1. Validate `INSERT INTO db.t1 SELECT FROM db.t2` resolves both 2-part refs
--    under the active catalog (no USE / no current database).
-- 2. Validate standalone `SELECT FROM db.t` resolves the 2-part ref too.
-- Regression guard for: 2-part `db.table` refs failing with "unknown database"
-- in the SELECT-FROM path before the resolver fix.
DROP TABLE IF EXISTS ${case_db}.t_two_part_src;
DROP TABLE IF EXISTS ${case_db}.t_two_part_sink;
CREATE TABLE ${case_db}.t_two_part_src (
  id BIGINT,
  v VARCHAR
);
CREATE TABLE ${case_db}.t_two_part_sink (
  id BIGINT,
  v VARCHAR
);
INSERT INTO ${case_db}.t_two_part_src VALUES
  ROW(1, 'alpha'),
  ROW(2, 'beta'),
  ROW(3, 'gamma');

-- Path 1: INSERT INTO db.t1 SELECT FROM db.t2 (both sides 2-part).
INSERT INTO ${case_db}.t_two_part_sink
SELECT id, v FROM ${case_db}.t_two_part_src;

-- Path 2: standalone SELECT FROM db.t.
SELECT id, v FROM ${case_db}.t_two_part_sink ORDER BY id;
