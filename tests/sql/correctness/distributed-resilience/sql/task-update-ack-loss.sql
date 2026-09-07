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

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.task_update_ack_loss (id BIGINT)
TBLPROPERTIES ("format-version" = "3");

-- Independent writes intentionally leave several Iceberg data files. The
-- cross-process scanner therefore sends nonempty terminal assignments to all
-- three admitted read tasks rather than exercising an empty terminal marker.
-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (1);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (2);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (3);

-- query 5
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (4);

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (5);

-- query 7
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (6);

-- query 8
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (7);

-- query 9
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (8);

-- query 10
-- @skip_result_check=true
INSERT INTO ${case_db}.task_update_ack_loss VALUES (9);

-- query 11
-- Baseline: each backend receives a real remote split assignment before the
-- acknowledgement-loss scenario asks one backend to replay it.
-- @result_contains=45
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED,3
SELECT SUM(id) AS total FROM ${case_db}.task_update_ack_loss;

-- query 12
-- The selected BE accepts its nonempty terminal assignment, loses only the
-- reply, then observes the exact immutable retransmission as duplicates. The
-- result must remain complete and the retry must not re-enqueue a split.
-- @query_lifecycle_fault=task-update-terminal-ack-drop,0
-- @result_contains=45
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,3
-- @be_log_contains=NOVAROCKS_TASK_UPDATE_TERMINAL_ACK_DROPPED
-- @be_log_contains=NOVAROCKS_TASK_SPLIT_ASSIGNMENT_ACCEPTED
-- @be_log_contains=NOVAROCKS_TASK_SPLIT_ASSIGNMENT_DUPLICATE
SELECT SUM(id) AS total FROM ${case_db}.task_update_ack_loss;

-- query 13
-- The one-shot fault must not poison later TaskUpdate delivery.
-- @result_contains=9
SELECT COUNT(*) FROM ${case_db}.task_update_ack_loss;
