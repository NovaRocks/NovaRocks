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
-- B8. The replacement snapshot keeps its statistics after their source expires.

-- query 1
-- @skip_result_check=true
CREATE SCHEMA iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0};
CREATE TABLE iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows (id BIGINT, value BIGINT)
WITH (format_version = 2, statistics_collect_on_write = false);
INSERT INTO iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows VALUES (1, 10), (2, 20);
INSERT INTO iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows VALUES (3, 30), (4, 40);
ANALYZE iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows;

-- query 2
SHOW STATS FOR iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows;

-- query 3
ALTER TABLE iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows EXECUTE optimize;

-- query 4
-- @skip_result_check=true
SET SESSION iceberg_cat_${suite_uuid0}.expire_snapshots_min_retention = '0s';
ALTER TABLE iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows EXECUTE expire_snapshots(retention_threshold => '0s', retain_last => 1);
RESET SESSION iceberg_cat_${suite_uuid0}.expire_snapshots_min_retention;

-- query 5
SELECT operation FROM iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}."stats_rows$snapshots" ORDER BY committed_at;

-- query 6
SHOW STATS FOR iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows;

-- query 7
SELECT id, value FROM iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0}.stats_rows ORDER BY id;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP SCHEMA IF EXISTS iceberg_cat_${suite_uuid0}.stats_rewrite_${uuid0} CASCADE;
