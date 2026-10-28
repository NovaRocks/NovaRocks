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
-- @tags=mv,iceberg,rest,minio,storage-contract,cold-restart
-- Test Objective:
-- The whole MV storage contract in one chain, against the product topology.
-- Every link is a claim the lake alone must carry:
--   1. CREATE writes the view's own documents, and SHOW reads those fields back.
--   2. A first refresh publishes, and the result is both directly readable and
--      cheap enough to rewrite a base query onto.
--   3. Wiping this fixture's Accelerator and restarting the frontend leaves the
--      lake as the only source: the view is still readable afterwards.
--   4. Management is closed after that restart, because an unanswered dispatch
--      from the previous process leaves no trace in the documents. Refresh is
--      refused while it is closed.
--   5. An operator declaration retires that barrier, and only then does a
--      second, explicit full refresh publish again.
-- Incremental refresh is deliberately out of scope here; this gate proves the
-- documents, the recovery and the continuation, not the delta path.

-- query 1
-- @skip_result_check=true
CREATE EXTERNAL CATALOG mvsc_${uuid0}
PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "rest",
  "uri" = "${iceberg_rest_uri}",
  "warehouse" = "${iceberg_rest_warehouse}",
  "credential.object-store-metadata.consumer-role" = "frontend",
  "credential.object-store-metadata.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-metadata.name" = "${iceberg_object_store_metadata_credential_name}",
  "credential.object-store-metadata.generation" = "${iceberg_object_store_credential_generation}",
  "credential.object-store-data.consumer-role" = "backend",
  "credential.object-store-data.mode" = "${iceberg_object_store_credential_mode}",
  "credential.object-store-data.name" = "${iceberg_object_store_data_credential_name}",
  "credential.object-store-data.generation" = "${iceberg_object_store_credential_generation}",
  "aws.s3.endpoint" = "${oss_endpoint}",
  "aws.s3.region" = "${oss_region}",
  "aws.s3.enable_path_style_access" = "${oss_path_style}"
);

-- query 2
-- @skip_result_check=true
CREATE DATABASE mvsc_${uuid0}.ns_${uuid0};

-- query 3
-- @skip_result_check=true
CREATE TABLE mvsc_${uuid0}.ns_${uuid0}.orders (
  region STRING,
  channel STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- Four groups over a few thousand rows, so the aggregate view is a decisive
-- cost win and the rewrite proof below does not turn on file layout.
-- @skip_result_check=true
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.orders
SELECT
  CASE WHEN n % 2 = 0 THEN 'east' ELSE 'west' END,
  CASE WHEN n % 3 = 0 THEN 'online' ELSE 'store' END,
  CAST(n % 100 AS BIGINT)
FROM TABLE(generate_series(1, 4000)) t(n);

-- query 5
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- @skip_result_check=true
CREATE MATERIALIZED VIEW orders_rollup
DISTRIBUTED BY HASH(region) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT region, channel, SUM(amount) AS total, COUNT(*) AS rows_in
FROM orders
GROUP BY region, channel;

-- query 8
-- CREATE alone writes the view's documents. SHOW reads the fields back out of
-- them: the recorded source, the frozen SELECT and the declared refresh mode.
--
-- The view is DEFERRED MANUAL on purpose. A scheduled view would refresh
-- itself between these steps, and every refresh below is meant to be the one
-- this case asked for.
-- @skip_result_check=true
-- @result_contains=orders_rollup
-- @result_contains=mvsc_${uuid0}.ns_${uuid0}.orders
-- @result_contains=SUM(amount)
-- @result_contains=DEFERRED_MANUAL
-- @result_contains=MANAGEABLE
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 9
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW orders_rollup WITH SYNC MODE;

-- query 10
SELECT region, channel, total, rows_in FROM orders_rollup ORDER BY region, channel;

-- query 11
-- The published result is cheap enough that a base query is rewritten onto it.
-- @skip_result_check=true
-- @explain_contains=rewritten with mv: orders_rollup
SELECT region, channel, SUM(amount) FROM orders GROUP BY region, channel;

-- query 12
-- Clear this fixture's Accelerator and restart the frontend. Everything below
-- can only come from REST Catalog and MinIO.
-- @imv_accelerator_wipe_restart=orders_rollup,catalog=mvsc_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 13
-- @retry_count=40
-- @retry_interval_ms=250
SELECT region, channel, total, rows_in
FROM mvsc_${uuid0}.ns_${uuid0}.orders_rollup
ORDER BY region, channel;

-- query 14
-- The restart replaced the session with a new connection, so the statements
-- below name their catalog again rather than relying on a session that no
-- longer exists.
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};

-- query 15
-- The restart left a barrier: the previous process could have had a dispatch
-- in flight, and the documents cannot say otherwise.
-- Startup rediscovery installs the barrier behind catalog admission rather
-- than inside it, so this waits for it rather than assuming it finished
-- before the connection did.
-- @retry_count=40
-- @retry_interval_ms=250
-- @skip_result_check=true
-- @result_contains=AWAITING_EFFECT_SETTLEMENT
-- @result_contains=UnsettledEffects
CALL novarocks_mv_management_status('mvsc_${uuid0}', 'ns_${uuid0}', 'orders_rollup');

-- query 16
-- Reading is unaffected; writing is not this process's to do yet.
-- @retry_count=40
-- @retry_interval_ms=250
-- @skip_result_check=true
-- @result_contains=orders_rollup
-- @result_contains=READ_ONLY
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 17
-- @skip_result_check=true
-- @expect_error_tier=drift
-- @expect_error=MV target requires a successful fresh Current observation
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
REFRESH MATERIALIZED VIEW orders_rollup WITH SYNC MODE;

-- query 18
-- The operator declares the previous process isolated. The directive reads the
-- challenge and the incarnation out of the status above and then runs the real
-- command, so nothing here is a shortcut around it.
-- @mv_resume_management=orders_rollup,catalog=mvsc_${uuid0},database=ns_${uuid0}
-- @skip_result_check=true
SELECT 1;

-- query 19
-- @skip_result_check=true
-- @result_contains=MANAGEABLE
-- @mv_rest_document_graph=ns_${uuid0}.orders_rollup,publications=1,slot-changes=1,table-commits=3
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 20
-- @skip_result_check=true
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.orders VALUES ('east', 'online', 1000), ('west', 'store', 2000);

-- query 21
-- @skip_result_check=true
-- @mv_rest_document_graph=ns_${uuid0}.orders_rollup,publications=2,slot-changes=1,table-commits=4,full-overwrite-last=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
REFRESH MATERIALIZED VIEW orders_rollup FULL WITH SYNC MODE;

-- query 22
SELECT region, channel, total, rows_in FROM orders_rollup ORDER BY region, channel;

-- query 23
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW orders_rollup;
DROP TABLE mvsc_${uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE mvsc_${uuid0}.ns_${uuid0};
-- Dropping the attachment matters: two attachments onto one warehouse make the
-- same physical MV discoverable under two catalog names, and its management
-- binds to whichever attachment discovered it first -- so the next case's
-- status query, which names its own catalog, would find nothing.
DROP CATALOG mvsc_${uuid0};
