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

-- @order_sensitive=true
-- @tags=runtime_filter,ordered_domain,timestamp_timezone
-- Test Objective:
-- 1. The ordered filter on the Iceberg metadata scan uses its declared
--    zoned timestamp key.
-- 2. Returning only the group count keeps the oracle independent of timestamps.
-- One INSERT commits exactly one append snapshot. Both queries therefore return
-- one, and neither exposes generated snapshot IDs or wall-clock timestamps.
DROP TABLE IF EXISTS ${case_db}.rf_ordered_timestamp_zone;
CREATE TABLE ${case_db}.rf_ordered_timestamp_zone (k INT) WITH (format_version = 3);
INSERT INTO ${case_db}.rf_ordered_timestamp_zone VALUES ROW(1), ROW(2);

SET SESSION disable_optimizer_rules = 'PushDownTopNToPreAgg';
SET SESSION enable_global_runtime_filter = true;
-- @explain_contains=LOCAL TOP-N (limit=1, offset=0)
-- @explain_contains=HASH AGGREGATE (GLOBAL, group by: [rf_ordered_timestamp_zone$snapshots.committed_at])
-- @explain_contains=domain = OrderedBound
SELECT COUNT(*) AS snapshots_at_first_commit
FROM ${case_db}."rf_ordered_timestamp_zone$snapshots"
GROUP BY committed_at ORDER BY committed_at LIMIT 1;

SET SESSION enable_global_runtime_filter = false;
SELECT COUNT(*) AS snapshots_at_first_commit
FROM ${case_db}."rf_ordered_timestamp_zone$snapshots"
GROUP BY committed_at ORDER BY committed_at LIMIT 1;

SET SESSION enable_global_runtime_filter = true;
SET SESSION disable_optimizer_rules = '';
DROP TABLE ${case_db}.rf_ordered_timestamp_zone;
