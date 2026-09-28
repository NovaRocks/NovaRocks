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

-- A dropped CreateTask acknowledgement leaves the RPC outcome unknown. The
-- Worker independently publishes Installed/terminal facts through the covered
-- status stream. Either a positive fact or an identical replay can prove
-- ownership; the frontend must stop replaying once ownership is known.
--
-- Require the real ACK-drop fault to fire, each participating backend to
-- apply its tasks, and both query results to remain correct. Exact-request
-- idempotence is tested by native-creation/frozen-replay-and-membership, which
-- deliberately sends the replay instead of racing it against Installed.

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
-- @be_log_count_at_least=NOVAROCKS_TASK_CREATE_ACK_DROPPED,1
SELECT id, payload
FROM ${case_db}.create_ack_loss
ORDER BY id;

-- query 6
-- @query_lifecycle_fault=create-task-ack-drop,2
-- @result_contains=60
-- @be_log_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_count_at_least=NOVAROCKS_TASK_CREATE_ACK_DROPPED,1
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.create_ack_loss left_side
JOIN ${case_db}.create_ack_loss right_side
  ON left_side.id = right_side.id;
