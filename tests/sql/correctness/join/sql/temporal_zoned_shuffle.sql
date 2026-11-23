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

CREATE TABLE ${case_db}.temporal_probe (k TIMESTAMP(9) WITH TIME ZONE, id INT) WITH (format_version = 3);
CREATE TABLE ${case_db}.temporal_build (k TIMESTAMP(9) WITH TIME ZONE, id INT) WITH (format_version = 3);
INSERT INTO ${case_db}.temporal_probe VALUES (TIMESTAMP '1969-12-31 23:59:59.999999999 UTC', 1);
INSERT INTO ${case_db}.temporal_probe VALUES (TIMESTAMP '2024-01-01 00:00:00.000000001 UTC', 2);
INSERT INTO ${case_db}.temporal_probe VALUES (TIMESTAMP '2024-01-01 00:00:00.000000002 UTC', 3);
INSERT INTO ${case_db}.temporal_build VALUES (TIMESTAMP '1969-12-31 23:59:59.999999999 UTC', 101);
INSERT INTO ${case_db}.temporal_build VALUES (TIMESTAMP '2024-01-01 00:00:00.000000001 UTC', 102);
INSERT INTO ${case_db}.temporal_build VALUES (TIMESTAMP '2024-01-01 00:00:00.000000003 UTC', 103);

SET SESSION cbo_broadcast_node_mem_budget_bytes = 0;
SET SESSION global_runtime_filter_build_max_size = 10737418240;
SET SESSION global_runtime_filter_probe_min_selectivity = 0.0;
SET SESSION disable_optimizer_rules = 'FoldConstant';

-- @explain_contains=HASH JOIN (PARTITIONED, INNER
-- @explain_contains=PARTITION: HASH_PARTITIONED
-- @be_log_be_count_at_least=NOVAROCKS_EXCHANGE_GRANT_ONLY_PROBE event=deliver_started,2
WITH p AS (
    SELECT at_timezone(k, 'Asia/Shanghai') AS k, id, from_unixtime(CAST(id AS DOUBLE)) AS epoch
    FROM ${case_db}.temporal_probe
),
b AS (
    SELECT k, id, from_unixtime(CAST(id - 100 AS DOUBLE)) AS epoch
    FROM ${case_db}.temporal_build
)
SELECT p.id AS probe_id, b.id AS build_id, CAST(p.k AS VARCHAR) AS probe_zone,
       CAST(b.k AS VARCHAR) AS build_zone, to_unixtime(p.epoch) AS probe_epoch,
       now() = CURRENT_TIMESTAMP AS same_statement_clock
FROM p JOIN b ON p.k = b.k AND p.epoch = b.epoch
ORDER BY p.id;
