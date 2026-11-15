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
-- Freeze the two lake-native MV lifecycle edges that the basic smoke does not
-- reach:
--   1. refreshing again over an unchanged base snapshot is a no-op — it must
--      neither duplicate nor drop rows;
--   2. after DROP MATERIALIZED VIEW the name stops resolving.

-- query 1
-- @skip_result_check=true
CREATE TABLE orders_edges_${uuid0} (
  k1 INT NOT NULL,
  v2 BIGINT
)
TBLPROPERTIES (
  "format-version" = "3"
);
INSERT INTO orders_edges_${uuid0} VALUES
  (1, 10),
  (2, 20);
CREATE MATERIALIZED VIEW orders_edges_mv_${uuid0}
DISTRIBUTED BY HASH(k1) BUCKETS 1
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v2 FROM orders_edges_${uuid0} WHERE v2 >= 10;

-- query 2
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_edges_mv_${uuid0};

-- query 3
SELECT k1, v2 FROM orders_edges_mv_${uuid0} ORDER BY k1;

-- query 4
-- Second refresh with no intervening base write: the base snapshot is
-- unchanged, so this must publish nothing rather than re-appending.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_edges_mv_${uuid0};

-- query 5
SELECT k1, v2 FROM orders_edges_mv_${uuid0} ORDER BY k1;

-- query 6
-- A third refresh, still with no base write, stays a no-op.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_edges_mv_${uuid0};

-- query 7
SELECT k1, v2 FROM orders_edges_mv_${uuid0} ORDER BY k1;

-- query 8
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_edges_mv_${uuid0};

-- query 9
-- @expect_error=unknown table
SELECT k1, v2 FROM orders_edges_mv_${uuid0} ORDER BY k1;

-- query 10
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS orders_edges_mv_${uuid0};
DROP TABLE IF EXISTS orders_edges_${uuid0} FORCE;
