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
--
-- A backend process that disappears mid-query must not leave the query
-- hanging, and the backends that survive must release the attempt.
--
-- Migrated onto the task protocol. Both retired parts are replaced:
--
--   * the trigger. The retired directive waited for SHOW BACKENDS to report a
--     fresh ScheduledFragments count, and that column is only fed by the
--     retired stage loop, so on the task path it never moves and the kill was
--     never delivered. The kill is now released by the backend's own
--     admission marker.
--   * the fragment backend limit. It existed so the killed backend was
--     certainly a fragment executor. Every backend the task protocol gives a
--     query context to hosts a task, so the limit has nothing left to select
--     -- and its evidence branch reads retired ControlReady fields.
--
-- The logical read uses the frozen semantic description to replace an attempt
-- that loses one backend before any result becomes visible. The old attempt
-- remains fenced and converges independently of the replacement result.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.resilience_series (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.resilience_series
VALUES (1, 1);
INSERT INTO ${case_db}.resilience_series
VALUES (2, 1);
INSERT INTO ${case_db}.resilience_series
VALUES (3, 1);

-- query 2
-- @kill_be_after_be_log_contains=1,NOVAROCKS_TASK_CREATE_APPLIED
-- The aggregate does not expose a result before every input completes. The
-- killed attempt is therefore replaceable on the two surviving backends from
-- the same frozen semantic description.
-- @result_contains=3
-- A context exists only where a task is placed, so this is deliberately an
-- execution-participation assertion rather than a cluster-size assertion.
-- The completed result after one participant disappears proves the successor
-- was activated without requiring the unreachable Worker to report a stop.
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,2
SELECT COUNT(*)
FROM ${case_db}.resilience_series
WHERE sleep(delay_s);

-- query 3
-- @heartbeat_delay_ms=3000
-- @result_contains=1000000
SELECT COUNT(*) FROM TABLE(generate_series(1, 1000000));

-- query 4
-- @kill_be_index=1
-- @restart_be_delay_ms=0
-- @heartbeat_delay_ms=3000
-- @skip_result_check=true
SELECT 1;
