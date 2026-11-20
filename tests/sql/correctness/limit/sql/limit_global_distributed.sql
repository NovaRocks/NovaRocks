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

-- @tags=limit,distributed
-- Test Objective:
-- Validate that LIMIT without ORDER BY is still a global LIMIT in distributed
-- execution. A LIMIT 1 marker must produce one row for the query, not one row
-- per BE fragment instance.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_limit_global;

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_limit_global (
  k1 INT NOT NULL
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.t_limit_global VALUES
  ROW(1), ROW(2), ROW(3);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.t_limit_global VALUES
  ROW(4), ROW(5), ROW(6);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.t_limit_global VALUES
  ROW(7), ROW(8), ROW(9);

-- query 6
SELECT COUNT(*) FROM (
  SELECT 1 AS marker FROM ${case_db}.t_limit_global LIMIT 1
) x;

-- query 7
SELECT COUNT(*) FROM (
  SELECT 42 AS host_key
) h
LEFT JOIN (
  SELECT 1 AS marker FROM ${case_db}.t_limit_global LIMIT 1
) m ON true;
