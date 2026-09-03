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
-- The statement is ANALYZE, not a SELECT. The terminal snapshot this case
-- perturbs belongs to the retired lifecycle, which after the task-protocol
-- cutover serves exactly one intent: statistics collection. A SELECT no
-- longer reports a lifecycle terminal at all, so the fault had nothing to
-- perturb and the query simply succeeded -- the case failed for expecting an
-- error rather than for the contract being broken.
--
-- The frontend fragment-backend limit is dropped with it: that directive
-- rests on the participant/service-only shape, which ADR-0134 removed.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.terminal_conflict (
  id BIGINT,
  payload BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.terminal_conflict VALUES (1, 10);
INSERT INTO ${case_db}.terminal_conflict VALUES (2, 20);
INSERT INTO ${case_db}.terminal_conflict VALUES (3, 30);

-- query 3
-- Inject a second valid terminal payload with the same execution and backend
-- identity but a different digest before FE ACK. The query must fail closed,
-- never publish a successful terminal set, and clean up normally.
-- @terminal_snapshot_conflict_be_index=0
-- @expect_error=query terminal outcome conflicts with an already stored participant outcome
ANALYZE TABLE ${case_db}.terminal_conflict (payload);

-- query 4
-- Health query after the rejected terminal identity conflict.
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.terminal_conflict;
