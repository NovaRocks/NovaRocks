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
-- A terminal record frozen before the coordinator died must still be
-- reclaimed by the backend alone.
--
-- Migrated onto the task protocol. The retired `terminal-retained` phase was
-- a frontend rendezvous inside the terminal-outcome store, released after the
-- frontend had durably kept a participant record but before it acknowledged
-- it. The task protocol keeps that record on the backend instead: retiring a
-- task is the moment it stops owning execution resources -- its inbound
-- capability and receiver are removed -- while its terminal answer stays
-- readable for the request horizon. So the record this case is about is a
-- backend-local one, and no frontend barrier is involved in producing it.
--
-- The query shape is load-bearing. The build side finishes as soon as it has
-- sent its rows and is retired within one maintenance tick, while the probe
-- side is still inside its sleeping predicate for another fifteen seconds --
-- which is what puts a frozen terminal record and a running attempt in the
-- same instant, and by a margin no scheduling jitter closes. A query whose
-- tasks all finished together would let the answer arrive before the
-- coordinator could be killed, and the step would fail on its barrier rather
-- than on the property.
--
-- The kill itself is released by the first applied lease renewal, for the
-- reason the plain frontend-crash case documents: a coordinator killed before
-- it renews leaves its backends on the 30-second initial lease, and the
-- expiry that follows would fall outside any step's evidence budget.
--
-- Full reclamation to `Gone` is deliberately not asserted. It is bounded by
-- the protocol's frozen 120-second request horizon, which no configuration
-- shortens, so a step budgeted in seconds cannot observe it. What is asserted
-- is the bounded retention the crash must not prevent: the record exists, the
-- lease runs out on every backend, and every context completes its
-- termination without a coordinator.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.fe_terminal_retained (
  id BIGINT,
  payload BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.fe_terminal_retained VALUES (1, 10, 15);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.fe_terminal_retained VALUES (2, 20, 15);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.fe_terminal_retained VALUES (3, 30, 15);

-- query 5
-- @kill_fe_after_be_log_contains=NOVAROCKS_TASK_LEASE_RENEWED
-- @expect_error=server disconnected
-- @be_log_count_at_least=NOVAROCKS_TASK_TERMINAL_RETAINED,1
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_LEASE_EXPIRED,3
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_TERMINATION_COMPLETED,3
SELECT SUM(probe.payload) AS total
FROM ${case_db}.fe_terminal_retained probe
JOIN ${case_db}.fe_terminal_retained build
  ON probe.id = build.id
WHERE sleep(probe.delay_s);

-- query 6
-- Health query after FE restart and BE retained-record reclamation.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.fe_terminal_retained;
