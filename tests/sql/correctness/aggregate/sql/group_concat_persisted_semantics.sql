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

-- Test Objective: reject uncaptured VIEW/MV semantic settings before replay.
-- Session views use the explicit default catalog; the shared REST fixture has no VIEW capability.
-- @order_sensitive=true

-- query 1
-- @skip_result_check=true
CREATE VIEW default_catalog.${case_db}.v_persisted_mode AS SELECT 7 AS value;
SET sql_mode='GROUP_CONCAT_LEGACY';

-- query 2
-- @expect_error=sql.admit.persisted_definition_semantics_unsupported
CREATE VIEW default_catalog.${case_db}.v_forbidden_mode AS SELECT 1;

-- query 3
-- @expect_error=GROUP_CONCAT_LEGACY is not captured
SELECT * FROM default_catalog.${case_db}.v_persisted_mode;

-- query 4
SELECT /*+ SET_VAR(sql_mode=32) */ value FROM default_catalog.${case_db}.v_persisted_mode;

-- query 5
-- @expect_error=GROUP_CONCAT_LEGACY is not captured
EXPLAIN SELECT * FROM default_catalog.${case_db}.v_persisted_mode;

-- query 6
-- @expect_error=sql.admit.persisted_definition_semantics_unsupported
CREATE MATERIALIZED VIEW ${case_db}.mv_forbidden_mode DISTRIBUTED BY HASH(k) BUCKETS 1 AS SELECT k FROM definitely_missing_table;

-- query 7
-- @expect_error=sql.admit.persisted_definition_semantics_unsupported
REFRESH MATERIALIZED VIEW ${case_db}.definitely_missing_mv;

-- query 8
SELECT group_concat('a','b') AS value;

-- query 9
-- @skip_result_check=true
SET sql_mode=32;

-- query 10
SELECT value FROM default_catalog.${case_db}.v_persisted_mode;

-- query 11
-- @expect_error=GROUP_CONCAT_LEGACY is not captured
SELECT * FROM (SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ * FROM default_catalog.${case_db}.v_persisted_mode) d;

-- query 12
-- @expect_error=sql.admit.persisted_definition_semantics_unsupported
CREATE OR REPLACE VIEW default_catalog.${case_db}.v_persisted_mode AS SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ 9 AS value;

-- query 13
SELECT value FROM default_catalog.${case_db}.v_persisted_mode;

-- query 14
-- @skip_result_check=true
DROP VIEW default_catalog.${case_db}.v_persisted_mode;
