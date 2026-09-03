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

-- KILL QUERY is client-facing, and after the cutover it had no coverage at
-- all: both of this case's triggers waited on retired evidence, so the kill
-- was never even sent and the query completed normally. A case that fails
-- because its trigger never fires cannot tell "cancellation works" from
-- "cancellation is broken", which is the worst of the three states to be in.
--
-- Retargeted onto task-protocol evidence. The trigger is a task actually
-- being created on a backend, and the assertion is that every context was
-- told to abort -- delivery of the abort, not merely the client seeing an
-- error, because a client error is also what a timeout looks like.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.kill_query (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.kill_query VALUES (1, 10);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.kill_query VALUES (2, 10);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.kill_query VALUES (3, 10);

-- query 5
-- @kill_query_after_be_log_contains=NOVAROCKS_TASK_CREATE_APPLIED
-- @expect_error=Query execution was interrupted
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED,3
SELECT COUNT(*)
FROM ${case_db}.kill_query
WHERE sleep(delay_s);

-- query 6
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.kill_query;

-- query 7
-- SPI-5B: metadata aliases use the generic ConnectorReadSource path.  Abort
-- after its metadata reader opens and require that reader to close before the
-- runner accepts cross-process resource convergence.
-- @kill_query_after_be_log_contains=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN
-- @expect_error=Query execution was interrupted
-- @be_log_count_at_least=NOVAROCKS_CONNECTOR_UNIT_READER_OPEN,1
-- @be_log_count_at_least=NOVAROCKS_CONNECTOR_UNIT_READER_CLOSE,1
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED,3
SELECT COUNT(*)
FROM ${case_db}.kill_query$files AS metadata
JOIN ${case_db}.kill_query AS data ON TRUE
WHERE sleep(data.delay_s);

-- query 8
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.kill_query;
