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
-- Validate that the active materialized-view suite runs on its suite-level
-- REST Iceberg catalog and covers the lake-native MV create/refresh/read/drop
-- path required by NIDL-C1. The StarRocks-compatible MV cases from the legacy
-- FE suite are parked under materialized-view/legacy/.

-- query 1
-- @skip_result_check=true
CREATE TABLE orders_base_${uuid0} (
  k1 INT NOT NULL,
  v2 BIGINT
) WITH (format_version = 3);
INSERT INTO orders_base_${uuid0} VALUES
  ROW(1, 10),
  ROW(2, 20);
CREATE MATERIALIZED VIEW orders_mv_${uuid0} AS SELECT k1, v2 FROM orders_base_${uuid0};

-- query 2
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 3
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 4
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 5
-- @skip_result_check=true
INSERT INTO orders_base_${uuid0} VALUES ROW(3, 30);

-- query 6
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 7
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_mv_${uuid0};

-- query 8
SELECT k1, v2 FROM orders_mv_${uuid0} ORDER BY k1;

-- query 9
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS orders_mv_${uuid0};
DROP TABLE IF EXISTS orders_base_${uuid0} ;
