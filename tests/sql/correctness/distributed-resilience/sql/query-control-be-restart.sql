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
-- A backend replaced after it admitted this attempt must come back as a new
-- process that has restored nothing, and the query must still be answered.
--
-- Migrated onto the task protocol. `EstablishQueryContext` is the task
-- protocol's first per-backend admission point, so it is where the retired
-- applied-Init rendezvous moves to: the backend installs the context,
-- publishes a token-scoped marker, and waits for the harness to replace that
-- exact process. The frontend's establish therefore never answers, the
-- attempt fails while its pre-ready retry window is still open, and the
-- replan runs against the topology the replacement joined -- which is why
-- this step still returns a result instead of an error.
--
-- The runner's own proof is that the replacement carries a different
-- BackendProcessId and that its fresh log contains neither an establish nor a
-- create for the old execution. The assertions here are the case-visible
-- half: the rendezvous fired at all, and every backend of the answering
-- attempt established its context.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.be_restart (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.be_restart VALUES (1, 3);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.be_restart VALUES (2, 3);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.be_restart VALUES (3, 3);

-- query 5
-- @restart_be_after_establish_context_index=1
-- @be_log_count_at_least=NOVAROCKS_TASK_ESTABLISH_CONTEXT_OBSERVED,1
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED,3
SELECT COUNT(*)
FROM ${case_db}.be_restart
WHERE sleep(delay_s);

-- query 6
-- @retry_count=5
-- @retry_interval_ms=1000
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.be_restart;
