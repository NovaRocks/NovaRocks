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
-- @tags=mv,iceberg,definition-effects

-- query 1
-- @skip_result_check=true
CREATE TABLE b8a_base (id BIGINT NOT NULL) WITH (format_version = 3);
INSERT INTO b8a_base VALUES (1);
CREATE VIEW b8a_effect_view AS SELECT id, IF(false, random(), 0e0) AS v FROM b8a_base;

-- query 2
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW b8a_rejected_direct WITH (refresh_policy = 'MANUAL') AS
SELECT id, IF(false, random(), 0e0) AS v FROM b8a_base;

-- query 3
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW b8a_rejected_view WITH (refresh_policy = 'MANUAL') AS
SELECT id, v FROM b8a_effect_view;

-- query 4
SELECT count(*) AS rejected_in_system
FROM system.metadata.materialized_views
WHERE catalog_name = 'materialized_view_cat_${suite_uuid0}'
  AND schema_name = '${case_db}'
  AND name IN ('b8a_rejected_direct', 'b8a_rejected_view');

-- query 5
SELECT count(*) AS rejected_in_catalog
FROM materialized_view_cat_${suite_uuid0}.information_schema.tables
WHERE table_schema = '${case_db}'
  AND table_name IN ('b8a_rejected_direct', 'b8a_rejected_view');

-- query 6
SELECT id, v FROM b8a_effect_view ORDER BY id;

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW b8a_accepted WITH (refresh_policy = 'MANUAL') AS
SELECT id, IF(false, 42e0, 0e0) AS v FROM b8a_base;
REFRESH MATERIALIZED VIEW b8a_accepted;

-- query 8
SELECT id, v FROM b8a_accepted ORDER BY id;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS b8a_accepted;
DROP MATERIALIZED VIEW IF EXISTS b8a_rejected_view;
DROP MATERIALIZED VIEW IF EXISTS b8a_rejected_direct;
DROP VIEW IF EXISTS b8a_effect_view;
DROP TABLE IF EXISTS b8a_base;
