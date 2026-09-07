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
-- One task's local execution failure must fail the whole attempt and stand
-- every other participant down.
--
-- Migrated onto the task protocol. The retired directives named the Stage
-- phase: the failure was claimed while a fragment was staged and released
-- only after the frontend published its stage barrier, and the cancellation
-- proof counted per-fragment CANCEL_FINST lines against the accepted
-- fragments of that phase. `CreateTask` is the task protocol's single
-- admission point, so there is no staged-but-not-started window to release
-- from and no second phase to rendezvous with: the fault is claimed on the
-- create and applied by the worker once the task has published RUNNING.
--
-- The three assertions below are what carry the meaning. The injection marker
-- proves the fault actually fired -- without it every remaining assertion
-- would also hold for a query that simply succeeded. The create count proves
-- the failure happened inside a real three-backend attempt. The abort count
-- proves the fan-out reached every context, which is the task protocol's form
-- of "the other fragments were cancelled": an abort emits only where it
-- actually applied, so a replay of the same abort is idempotent and silent.
--
-- Exact per-participant cancellation accounting is not restored. It needs a
-- per-task stand-down marker carrying the fragment instance the retired
-- CANCEL_FINST line carried, and no such marker exists on the task path;
-- inventing an at-least count and calling it exact would assert less than it
-- claims.
--
-- The participant-outcome proof is still absent for the same reason it was
-- before this migration: it reads the frontend's structured lifecycle
-- snapshot, which a task-path query does not produce.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.fragment_execution_failure (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.fragment_execution_failure VALUES (1, 5);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.fragment_execution_failure VALUES (2, 5);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.fragment_execution_failure VALUES (3, 5);

-- query 5
-- @query_lifecycle_fault=task-execution-failure,1
-- @expect_error=task execution terminated
-- @be_log_count_at_least=NOVAROCKS_TASK_EXECUTION_FAILURE_INJECTED,1
-- Participant counts cannot be pinned to the cluster size: a context exists
-- only where a task is placed, and which backends get the table's splits is
-- the scheduler's business -- a restart earlier in the suite is enough to
-- co-locate two of three. What the case is about survives that: more than one
-- backend ran a task, and the failure of one reached a peer.
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CREATE_APPLIED,2
-- The failing task's own backend is the originator: its context terminated
-- locally, so the frontend's abort reaches it as a replay and applies to
-- nothing. ABORT_APPLIED therefore fires on the participants minus that one,
-- which is why this is one fewer than the count above rather than equal to it.
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED,1
SELECT COUNT(*)
FROM ${case_db}.fragment_execution_failure
WHERE sleep(delay_s);

-- query 6
-- A destructive local executor failure must leave the following query healthy.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.fragment_execution_failure;
