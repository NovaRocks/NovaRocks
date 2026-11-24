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

-- Twelve source files, with exactly 768 rows for each k in [0, 16].
CREATE TABLE ${case_db}.multicast_source (
    id BIGINT NOT NULL,
    k BIGINT NOT NULL,
    p BIGINT NOT NULL
) WITH (partitioning = ARRAY['p'], format_version = 3);

INSERT INTO ${case_db}.multicast_source
SELECT sequential_number, sequential_number % 17, sequential_number % 12
FROM TABLE(sequence(1, 13056));

SET SESSION pipeline_dop = 4;
SET SESSION cbo_broadcast_node_mem_budget_bytes = 0;

-- @skip_result_check=true
-- @result_contains=SINK multicast
EXPLAIN WITH SESSION explain_detail = 'CONTRACT'
WITH shared AS (
    SELECT k FROM ${case_db}.multicast_source
)
SELECT k, k AS k2, count(*) AS n
FROM (
    SELECT k FROM shared
    WHERE k IN (SELECT filter_key FROM (VALUES (BIGINT '0'), (BIGINT '2')) AS keys(filter_key))
    UNION ALL
    SELECT k FROM shared
) AS consumers
GROUP BY k
ORDER BY k;

-- @be_log_contains=NOVAROCKS_TASK_PREPARED_DOP
-- @be_log_contains=NOVAROCKS_EXCHANGE_SNAPSHOT event=source_finished
-- @be_log_be_count_at_least=NOVAROCKS_EXCHANGE_MULTICAST_DRIVER_ENQUEUED,2
WITH shared AS (
    SELECT k FROM ${case_db}.multicast_source
)
SELECT k, k AS k2, count(*) AS n
FROM (
    SELECT k FROM shared
    WHERE k IN (SELECT filter_key FROM (VALUES (BIGINT '0'), (BIGINT '2')) AS keys(filter_key))
    UNION ALL
    SELECT k FROM shared
) AS consumers
GROUP BY k
ORDER BY k;
