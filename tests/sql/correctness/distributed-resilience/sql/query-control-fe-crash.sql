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
-- A coordinator that dies mid-query must not leave its backends holding the
-- attempt.
--
-- Migrated onto the task protocol. The retired `staged` phase was a frontend
-- rendezvous inside the Stage barrier, and the evidence was the backend
-- noticing its query-control stream had gone. The task protocol has no
-- long-lived control stream, so a frontend that dies is indistinguishable
-- from a slow one until the query execution lease it stopped renewing runs
-- out. That expiry is the whole liveness mechanism, and it is what each
-- backend must act on alone.
--
-- The kill is released by the first applied lease renewal rather than by the
-- first admitted task, and that is not a cosmetic choice. The lease a
-- frontend installs at establish is the protocol's 30-second initial lease,
-- and a frontend killed before it renews leaves backends waiting out that
-- whole lease -- longer than any step's evidence budget. One applied renewal
-- means the attempt is in steady state on its five-second renewal lease,
-- which is both the closest thing this protocol has to the retired `staged`
-- phase and what makes the expiry that follows observable.
--
-- Two assertions, because either alone passes for the wrong reason. The
-- expiry is the backend deciding on its own that the coordinator is gone --
-- the successor of the retired coordinator-lost marker. The completed
-- termination is the successor of the retired terminal and cleanup pair: a
-- context reaches it only once every task it knew is a terminal record and
-- its shared facts are released, so it is the assertion that makes this about
-- released resources rather than about noticing.
--
-- The row delay is sized to straddle both: long enough that the query is
-- still running when the first renewal lands, and short enough that the
-- sleeping scalar -- which no stand-down can interrupt -- has finished before
-- the step's evidence budget runs out.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.fe_crash (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.fe_crash VALUES (1, 15);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.fe_crash VALUES (2, 15);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.fe_crash VALUES (3, 15);

-- query 5
-- @kill_fe_after_be_log_contains=NOVAROCKS_TASK_LEASE_RENEWED
-- @expect_error=server disconnected
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_LEASE_EXPIRED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_TERMINATION_COMPLETED,3
SELECT COUNT(*)
FROM ${case_db}.fe_crash
WHERE sleep(delay_s);

-- query 6
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.fe_crash;
