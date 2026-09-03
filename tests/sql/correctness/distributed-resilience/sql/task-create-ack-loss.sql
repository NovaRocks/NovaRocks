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

-- One dropped CreateTask acknowledgement must cost nothing but a resend. The
-- task is admitted on the backend and the frontend never learns of it, so the
-- resend must be recognised as an identical replay -- not admitted a second
-- time, which would double the task, and not refused as a conflict, which
-- would fail a query that had already succeeded.
--
-- CreateTask is the protocol's single admission point, so this one fault
-- covers what the retired protocol needed two for: its Stage and Start phases
-- each had an acknowledgement that could be lost independently.
--
-- Both halves are asserted because either alone passes for the wrong reason.
-- Counting only APPLIED across three backends would also pass if the fault
-- never fired, and counting only IDEMPOTENT would pass if a backend that owed
-- a task never created one.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.create_ack_loss (
  id BIGINT,
  payload BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.create_ack_loss VALUES (1, 10);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.create_ack_loss VALUES (2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.create_ack_loss VALUES (3, 30);

-- query 5
-- @query_lifecycle_fault=create-task-ack-drop,1
-- @result_contains=1	10
-- @result_contains=2	20
-- @result_contains=3	30
-- @be_log_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_count_at_least=NOVAROCKS_TASK_CREATE_IDEMPOTENT,1
SELECT id, payload
FROM ${case_db}.create_ack_loss
ORDER BY id;

-- query 6
-- @query_lifecycle_fault=create-task-ack-drop,2
-- @result_contains=60
-- @be_log_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_count_at_least=NOVAROCKS_TASK_CREATE_IDEMPOTENT,1
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.create_ack_loss left_side
JOIN ${case_db}.create_ack_loss right_side
  ON left_side.id = right_side.id;
