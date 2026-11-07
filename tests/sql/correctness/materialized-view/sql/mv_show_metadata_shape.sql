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
-- @tags=mv,iceberg,lake_native,c1
-- Test Objective:
-- Freeze the user-facing `SHOW MATERIALIZED VIEWS` projection.
--
-- The first SHOW runs against a database with no materialized views: the row
-- set is empty, so the recorded golden is exactly the column projection — every
-- column name, its order, and by construction the column count. That keeps the
-- shape assertion free of the per-run catalog/database/name identifiers that
-- would otherwise make a populated row unstable.
--
-- The second SHOW then covers the per-column values that are stable across
-- runs. The volatile cells (names, base-table paths, timestamps) are
-- deliberately not asserted here; `mv_lifecycle_edges` and the ivm suite own
-- the behaviour those cells describe.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS ${case_db};

-- query 2
SHOW MATERIALIZED VIEWS FROM ${case_db};

-- query 3
-- @skip_result_check=true
CREATE TABLE ${case_db}.orders_shape (
  k1 INT NOT NULL,
  v2 BIGINT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.orders_shape VALUES (1, 10);
CREATE MATERIALIZED VIEW ${case_db}.orders_shape_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM ${case_db}.orders_shape;

-- query 4
-- @result_contains=orders_shape_mv
-- @result_contains=iceberg
-- @result_contains=DEFERRED_MANUAL
-- @result_contains=MANUAL
-- @result_contains=false
SHOW MATERIALIZED VIEWS FROM ${case_db};

-- query 5
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.orders_shape_mv;
DROP TABLE IF EXISTS ${case_db}.orders_shape FORCE;
DROP DATABASE IF EXISTS ${case_db};
