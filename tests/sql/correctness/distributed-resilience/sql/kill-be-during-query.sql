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
--   * the trigger. `@kill_be_after_fragment_start` waits for SHOW BACKENDS to
--     report a fresh ScheduledFragments count, and that column is only fed by
--     the retired stage loop, so on the task path it never moves and the kill
--     was never delivered. The kill is now released by the backend's own
--     admission marker.
--   * the fragment backend limit. It existed so the killed backend was
--     certainly a fragment executor. Every backend the task protocol gives a
--     query context to hosts a task, so the limit has nothing left to select
--     -- and its evidence branch reads retired ControlReady fields.
--
-- The expected error is DERIVED, not verified: the task protocol has no
-- attributed backend-process-loss detector. A lost operation is retried
-- inside the frontend queue-residence bound and then dropped with no
-- consumer, and a dropped status subscription exhausts a budget nothing
-- reads, so the loss surfaces only when an exchange peer of the dead process
-- fails -- which depends on where the aggregation task was placed. The
-- survivor assertions below hold whichever way that lands.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.resilience_series (
  id BIGINT
)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO ${case_db}.resilience_series
SELECT generate_series FROM TABLE(generate_series(1, 333333));
INSERT INTO ${case_db}.resilience_series
SELECT generate_series FROM TABLE(generate_series(333334, 666666));
INSERT INTO ${case_db}.resilience_series
SELECT generate_series FROM TABLE(generate_series(666667, 1000000));

-- query 2
-- @kill_be_after_be_log_contains=1,NOVAROCKS_TASK_CREATE_APPLIED
-- The attempt is decided by the killed backend's status subscription settling
-- where resubscribing cannot repair it, and the error names that backend. It
-- deliberately does not assert the root result fetch failing instead: that
-- happens only when the killed process was the one holding the root task, so
-- asserting it would make this case pass or fail on where the scheduler put
-- the root.
-- @expect_error=is no longer observable
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED,2
SELECT COUNT(*) FROM ${case_db}.resilience_series;

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
