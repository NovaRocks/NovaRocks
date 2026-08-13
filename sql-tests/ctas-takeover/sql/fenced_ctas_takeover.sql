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
-- This acceptance-only case must run through the runner-owned 1FE+3BE
-- topology with the SQLite-backed fenced REST proxy enabled. The first CTAS
-- establishes the advertised positive path. The second loses the publish
-- response after downstream commit; an FE restart then proves current-
-- generation recovery observes the durable catalog terminal rather than
-- replaying source execution or guessing an operation id.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG ctas_takeover_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "aws.s3.access_key" = "${oss_ak}",
  "aws.s3.secret_key" = "${oss_sk}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "us-east-1",
  "aws.s3.enable_path_style_access" = "true"
);
DROP DATABASE IF EXISTS ctas_takeover_${uuid0}.ns_${uuid0} FORCE;
CREATE DATABASE ctas_takeover_${uuid0}.ns_${uuid0};
CREATE TABLE ctas_takeover_${uuid0}.ns_${uuid0}.source_rows (id INT, value VARCHAR(16))
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO ctas_takeover_${uuid0}.ns_${uuid0}.source_rows VALUES
  (1, 'alpha'), (2, 'beta'), (3, 'gamma');

-- query 2
-- @skip_result_check=true
-- @be_log_contains=NOVAROCKS_CONNECTOR_WRITER_OPENED
CREATE TABLE ctas_takeover_${uuid0}.ns_${uuid0}.published_rows AS
  SELECT id, value FROM ctas_takeover_${uuid0}.ns_${uuid0}.source_rows;

-- query 3
SELECT id, value FROM ctas_takeover_${uuid0}.ns_${uuid0}.published_rows ORDER BY id;

-- query 4
-- @fenced_catalog_fault=publish,after-downstream-before-response
-- @expect_error=CTAS catalog outcome is unresolved
-- @restart_fe_after_step=true
CREATE TABLE ctas_takeover_${uuid0}.ns_${uuid0}.recovered_rows AS
  SELECT id, value FROM ctas_takeover_${uuid0}.ns_${uuid0}.source_rows;

-- query 5
-- The prior frontend lost only the catalog response. Recovery must inspect the
-- durable published disposition after restart; no client retry may re-run the
-- source query.
-- @retry_count=30
-- @retry_interval_ms=1000
SELECT id, value FROM ctas_takeover_${uuid0}.ns_${uuid0}.recovered_rows ORDER BY id;

-- query 6
-- @fenced_catalog_fault=stage,before-accept
-- @expect_error=CTAS catalog outcome is unresolved
-- @restart_fe_after_step=true
CREATE TABLE ctas_takeover_${uuid0}.ns_${uuid0}.rejected_before_stage AS
  SELECT id, value FROM ctas_takeover_${uuid0}.ns_${uuid0}.source_rows;

-- query 7
-- The stage rejection must not alter a visible source or published target.
SELECT COUNT(*) AS n FROM ctas_takeover_${uuid0}.ns_${uuid0}.published_rows;

-- query 8
-- @skip_result_check=true
DROP DATABASE ctas_takeover_${uuid0}.ns_${uuid0} FORCE;
DROP CATALOG ctas_takeover_${uuid0};
