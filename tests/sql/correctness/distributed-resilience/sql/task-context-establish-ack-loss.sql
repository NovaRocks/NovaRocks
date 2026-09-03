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

-- One dropped EstablishQueryContext acknowledgement must cost nothing but a
-- resend. The operation applies on the backend and the frontend never learns
-- of it, which is exactly the unknown outcome the protocol's per-domain
-- progression exists to survive: the resend is recognised as an identical
-- replay rather than admitted a second time or refused as a conflict.
--
-- Both halves are asserted because either alone passes for the wrong reason.
-- Counting only APPLIED across three backends would also pass if the fault
-- never fired, and counting only IDEMPOTENT would pass if a backend that
-- should have established never did. The result assertions are what make the
-- whole thing mean "and the query was still answered correctly".

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.ack_loss (
  id BIGINT,
  payload BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.ack_loss VALUES (1, 10);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.ack_loss VALUES (2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.ack_loss VALUES (3, 30);

-- query 5
-- @query_lifecycle_fault=establish-context-ack-drop,1
-- @result_contains=1	10
-- @result_contains=2	20
-- @result_contains=3	30
-- @be_log_count_at_least=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED,3
-- @be_log_count_at_least=NOVAROCKS_TASK_CONTEXT_ESTABLISH_IDEMPOTENT,1
SELECT id, payload
FROM ${case_db}.ack_loss
ORDER BY id;

-- query 6
-- @query_lifecycle_fault=establish-context-ack-drop,2
-- @result_contains=60
-- @be_log_count_at_least=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ESTABLISH_APPLIED,3
-- @be_log_count_at_least=NOVAROCKS_TASK_CONTEXT_ESTABLISH_IDEMPOTENT,1
SELECT SUM(left_side.payload) AS total
FROM ${case_db}.ack_loss left_side
JOIN ${case_db}.ack_loss right_side
  ON left_side.id = right_side.id;
