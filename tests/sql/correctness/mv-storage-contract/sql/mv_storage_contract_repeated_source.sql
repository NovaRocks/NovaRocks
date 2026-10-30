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
-- @tags=mv,iceberg,rest,minio,storage-contract
-- Test Objective:
-- One table, referenced twice, is two sources.
--
-- The canonical definition records two relation occurrences of the same
-- object, and every source fact belongs to its own occurrence rather than to
-- the table name they share. So does everything downstream now: base
-- relations, snapshot pins, predecessor facts, the publication provenance and
-- its hashed watermark, and the join's own two sides -- which resolve by the
-- qualifier each was bound under rather than by the table they share.
--
-- What must NOT happen is the two collapsing into one. Keyed by table name the
-- second mention overwrote the first, which is why this shape used to be
-- refused outright rather than published with one mention's pin standing for
-- both.

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
CREATE TABLE mvsc_${uuid0}.ns_${uuid0}.moves (
  account STRING,
  peer STRING,
  amount BIGINT
) TBLPROPERTIES ("format-version" = "3", "write.row-lineage" = "true");

-- query 4
-- @skip_result_check=true
-- A single cycle, so each account has exactly one outgoing move and exactly
-- one incoming one. The join then pairs each account's two mentions one to
-- one, which keeps the numbers below about the pins rather than about join
-- multiplicity.
INSERT INTO mvsc_${uuid0}.ns_${uuid0}.moves VALUES
  ('a', 'b', 10), ('b', 'c', 4), ('c', 'a', 6);

-- query 5
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};

-- query 6
-- @skip_result_check=true
USE ns_${uuid0};

-- query 7
-- Two occurrences of `moves`: outgoing and the incoming it is matched against.
-- @skip_result_check=true
CREATE MATERIALIZED VIEW move_balance
DISTRIBUTED BY HASH(account) BUCKETS 1
REFRESH DEFERRED MANUAL
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT out.account AS account, SUM(out.amount) AS sent, SUM(inb.amount) AS received
FROM moves out JOIN moves inb ON out.account = inb.peer
GROUP BY out.account;

-- query 8
-- The view is real: both mentions of `moves` are recorded and it is listed.
-- @skip_result_check=true
-- @result_contains=move_balance
-- @result_contains=MANAGEABLE
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
SHOW MATERIALIZED VIEWS FROM ns_${uuid0};

-- query 9
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
REFRESH MATERIALIZED VIEW move_balance WITH SYNC MODE;

-- query 10
-- `a` sent 10 and received 6, `b` sent 4 and received 10, `c` sent 6 and
-- received 4. Every row reads both mentions, so a collapsed pin would put one
-- mention's numbers on both columns.
SELECT account, sent, received
FROM mvsc_${uuid0}.ns_${uuid0}.move_balance
ORDER BY account;

-- query 11
-- @skip_result_check=true
SET CATALOG mvsc_${uuid0};
USE ns_${uuid0};
DROP MATERIALIZED VIEW move_balance;
DROP TABLE mvsc_${uuid0}.ns_${uuid0}.moves FORCE;
DROP DATABASE mvsc_${uuid0}.ns_${uuid0};
-- Dropping the attachment matters: two attachments onto one warehouse make the
-- same physical MV discoverable under two catalog names, and its management
-- binds to whichever attachment discovered it first -- so the next case's
-- status query, which names its own catalog, would find nothing.
DROP CATALOG mvsc_${uuid0};
