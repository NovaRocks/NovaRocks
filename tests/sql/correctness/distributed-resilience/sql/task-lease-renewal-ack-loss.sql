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

-- A lost lease-renewal acknowledgement must not cost the query. The renewal
-- applies on the backend and the frontend never learns of it, so the frontend
-- resends from its own send time rather than assuming the lease is gone, and
-- the backend accepts the resend at the sequence it already holds.
--
-- The query has to outlive one renewal for this to test anything at all. The
-- initial lease is thirty seconds and the first renewal comes due around a
-- third of it, so a query that answers in under a second -- which every other
-- case here does -- would assert a marker that could never appear and pass or
-- fail for reasons having nothing to do with the lease. Hence the sleep.
--
-- Fifteen seconds, not more: the server's own query timeout in this fixture
-- is twenty-seven seconds, so a longer sleep kills the query before it can
-- assert anything. Fifteen clears the first renewal with margin on both sides.
--
-- The fault targets backend 0 because the renewing backends are the ones
-- hosting the long scan. A backend that finishes its own work early releases
-- its context before any renewal comes due, so arming it would drop an
-- acknowledgement that is never sent and the case would pass having tested
-- nothing.
--
-- What this does NOT cover, deliberately: sustained loss of renewals, where
-- the lease genuinely expires and the backend must stand its tasks down. That
-- needs a fault that stops renewing rather than one that drops a single
-- acknowledgement, and no such fault exists yet. The retired protocol's
-- heartbeat-loss case asserted that failure path; it has no port here, and
-- saying so is more useful than a case that looks like one.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.lease_renewal (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.lease_renewal VALUES (1, 15);

-- query 3
-- @query_lifecycle_fault=lease-renewal-ack-drop,0
-- @skip_result_check=true
-- @be_log_count_at_least=NOVAROCKS_TASK_LEASE_RENEWED,1
SELECT COUNT(*)
FROM ${case_db}.lease_renewal
WHERE sleep(delay_s);

-- query 4
-- @result_contains=1
SELECT COUNT(*) FROM ${case_db}.lease_renewal;
