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
-- @tags=mv,iceberg,rest,minio,lnp-3a,native-rebuild
-- This case is intentionally executed only as cross-process 1FE+3BE. It
-- proves that a published, paused, non-default async MV survives the
-- test-only accelerator wipe and later resumes with the same lake authority.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG lnp3a_ice_${uuid0}
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
CREATE DATABASE lnp3a_ice_${uuid0}.ns_${uuid0};
CREATE TABLE lnp3a_ice_${uuid0}.ns_${uuid0}.orders (
  k1 INT NOT NULL,
  v1 BIGINT
)
TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");
INSERT INTO lnp3a_ice_${uuid0}.ns_${uuid0}.orders VALUES (1, 10), (2, 20);
SET CATALOG lnp3a_ice_${uuid0};
USE ns_${uuid0};
CREATE MATERIALIZED VIEW orders_mv
DISTRIBUTED BY HASH(k1) BUCKETS 1
PRIMARY KEY (k1)
REFRESH ASYNC EVERY INTERVAL 1 SECOND
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT k1, v1 FROM orders;
REFRESH MATERIALIZED VIEW orders_mv;

-- query 2
-- @retry_count=30
-- @retry_interval_ms=500
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 3
-- @skip_result_check=true
ALTER MATERIALIZED VIEW orders_mv PAUSE REFRESH;

-- query 4
-- @imv_stateless_rebuild=orders_mv,catalog=lnp3a_ice_${uuid0},level=full
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 5
-- @skip_result_check=true
INSERT INTO lnp3a_ice_${uuid0}.ns_${uuid0}.orders VALUES (3, 30);

-- query 6
-- @skip_result_check=true
shell: sleep 2

-- query 7
SELECT k1, v1 FROM orders_mv ORDER BY k1;

-- query 8
-- @skip_result_check=true
ALTER MATERIALIZED VIEW orders_mv RESUME REFRESH;

-- query 9
-- @skip_result_check=true
DROP MATERIALIZED VIEW orders_mv;
DROP TABLE lnp3a_ice_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE lnp3a_ice_${uuid0}.ns_${uuid0};
DROP CATALOG lnp3a_ice_${uuid0};
