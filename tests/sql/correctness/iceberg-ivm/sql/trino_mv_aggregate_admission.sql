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
-- @order_sensitive=false
-- @tags=column-naming,mv,iceberg,aggregate-admission
-- Test Objective: Validate aggregate arity and unnamed MV output admission independently of refresh capability.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc_l7_measurements (
  k INT,
  v BIGINT,
  d DECIMAL(20, 4),
  s STRING,
  ts DATETIME
)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 2
-- @expect_error=Iceberg IMV refresh contract requires exactly one argument for aggregate function `avg`
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_bad_avg_star
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, AVG(*) AS invalid_avg FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 3
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_bad_avg_string
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, AVG(s) FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 4
-- @expect_error=Iceberg IMV refresh contract requires exactly one argument for aggregate function `min`
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_bad_min_star
DISTRIBUTED BY HASH(k) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k, MIN(*) AS invalid_min FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 5
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_bad_avg_star;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_bad_avg_string;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_bad_min_star;
DROP TABLE ${case_db}.trc_l7_measurements FORCE;
