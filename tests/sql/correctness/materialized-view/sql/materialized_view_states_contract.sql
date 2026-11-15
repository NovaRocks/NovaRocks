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

-- @sequential=true
-- @order_sensitive=true
-- Correctness owner: MV state facts, spec5.6. Expected values come from the
-- explicit policy, pause controls, independent seed rows and publication state.
-- Full base identity uses the row's exact catalog/schema components. The case
-- owns its MVs and base; the private suite fixture owns shutdown and retention.

-- query 1
-- @skip_result_check=true
CREATE TABLE state_base (k BIGINT NOT NULL, v BIGINT)
TBLPROPERTIES ('format-version' = '3', 'write.row-lineage' = 'true');
INSERT INTO state_base VALUES (1, 10), (2, 20);
CREATE MATERIALIZED VIEW state_manual DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH DEFERRED MANUAL PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM state_base;
CREATE MATERIALIZED VIEW state_interval DISTRIBUTED BY HASH(k) BUCKETS 1
REFRESH ASYNC EVERY INTERVAL 5 MINUTE PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, v FROM state_base;

-- query 2
SELECT name, refresh_policy, refresh_interval_ms, refresh_paused,
       last_refresh_time, last_refresh_rows, manageability, manageability_reason,
       base_tables[1] = concat(catalog_name, '.', schema_name, '.state_base') AS base_identity_matches,
       upstream_materialized_views
FROM system.runtime.materialized_view_states
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}' AND schema_name = '${case_db}'
ORDER BY name;

-- query 3
-- @skip_result_check=true
ALTER MATERIALIZED VIEW state_interval PAUSE REFRESH;

-- query 4
SELECT name, refresh_policy, refresh_interval_ms, refresh_paused
FROM system.runtime.materialized_view_states
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}' AND schema_name = '${case_db}' AND name = 'state_interval';

-- query 5
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW state_manual;

-- query 6
SELECT name, last_refresh_time IS NOT NULL AS has_publication_prepared_time, last_refresh_rows
FROM system.runtime.materialized_view_states
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}' AND schema_name = '${case_db}' AND name = 'state_manual';

-- query 7
SELECT table_name FROM system.information_schema.tables
WHERE table_schema = 'runtime' AND table_name = 'materialized_view_states';

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS state_manual;
DROP MATERIALIZED VIEW IF EXISTS state_interval;
DROP TABLE IF EXISTS state_base FORCE;
