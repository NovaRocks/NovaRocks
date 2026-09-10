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
-- @tags=write_path,mv,iceberg,ivm,storage_engine_iceberg,aggregate,avg,min,max
-- Test Objective:
-- 1. AVG over Int and Decimal inputs (output type follows analyzer).
-- 2. MIN/MAX over numeric, string, and timestamp inputs.
-- 3. NULL handling for AVG / MIN / MAX (whole group of NULLs).
-- 4. Incremental INSERT correctly updates AVG / MIN / MAX state.
-- 5. DDL rejections: AVG(*), AVG(string), MIN(*).
-- MV is Iceberg-target (PROPERTIES('storage_engine'='iceberg')).
-- This is the native FE/BE owner case for first refresh plus incremental
-- aggregate-state persistence; Core's in-process test harness intentionally
-- does not emulate the runtime-filter deployment/session lifecycle.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mv_agg2_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "hadoop",
  "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}/iceberg_agg2_${uuid0}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "static",
  "credential.object-store-metadata.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "static",
  "credential.object-store-data.name" = "${iceberg_object_store_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.enable_path_style_access" = "true"
);
CREATE DATABASE mv_agg2_${uuid0}.ns_${uuid0};
CREATE TABLE mv_agg2_${uuid0}.ns_${uuid0}.measurements (
  k INT,
  v BIGINT,
  d DECIMAL(20, 4),
  s STRING,
  ts DATETIME
)
TBLPROPERTIES ("format-version" = "3",
  "write.row-lineage" = "true");
INSERT INTO mv_agg2_${uuid0}.ns_${uuid0}.measurements VALUES
  (1, 10,   100.5000, 'apple',  '2024-01-01 00:00:00'),
  (1, 20,   200.0000, 'banana', '2024-02-01 00:00:00'),
  (1, NULL, NULL,     NULL,     NULL),
  (2, 5,    50.2500,  'cherry', '2024-03-15 12:00:00');
SET CATALOG mv_agg2_${uuid0};
USE ns_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE MATERIALIZED VIEW measurements_mv
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT
  k,
  COUNT(*)  AS c_all,
  SUM(v)    AS s_v,
  AVG(v)    AS a_v,
  AVG(d)    AS a_d,
  MIN(v)    AS mn_v,
  MAX(v)    AS mx_v,
  MIN(s)    AS mn_s,
  MAX(s)    AS mx_s,
  MIN(ts)   AS mn_ts,
  MAX(ts)   AS mx_ts
FROM measurements
GROUP BY k;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW measurements_mv;

-- query 4
SELECT k, c_all, s_v, a_v, a_d, mn_v, mx_v, mn_s, mx_s, mn_ts, mx_ts
FROM measurements_mv
ORDER BY k;

-- query 5
-- @skip_result_check=true
INSERT INTO mv_agg2_${uuid0}.ns_${uuid0}.measurements VALUES
  (1, 30,   300.7500, 'date', '2024-06-01 09:00:00'),
  (3, 7,    70.0000,  'fig',  '2024-07-01 18:30:00');

-- query 6
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW measurements_mv;

-- query 7
-- @imv_equivalence_check=measurements_mv
SELECT k, c_all, s_v, a_v, a_d, mn_v, mx_v, mn_s, mx_s, mn_ts, mx_ts
FROM measurements_mv
ORDER BY k;

-- query 8
-- @expect_error=Iceberg IMV refresh contract requires exactly one argument for aggregate function `avg`
CREATE MATERIALIZED VIEW bad_avg_star
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, AVG(*) FROM measurements GROUP BY k;

-- query 9
-- @expect_error=unsupported identifier `avg(s)`
CREATE MATERIALIZED VIEW bad_avg_string
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, AVG(s) FROM measurements GROUP BY k;

-- query 10
-- @expect_error=Iceberg IMV refresh contract requires exactly one argument for aggregate function `min`
CREATE MATERIALIZED VIEW bad_min_star
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, MIN(*) FROM measurements GROUP BY k;

-- query 11
-- @skip_result_check=true
DROP MATERIALIZED VIEW measurements_mv;
DROP TABLE mv_agg2_${uuid0}.ns_${uuid0}.measurements FORCE;
DROP DATABASE mv_agg2_${uuid0}.ns_${uuid0};
DROP CATALOG mv_agg2_${uuid0};
