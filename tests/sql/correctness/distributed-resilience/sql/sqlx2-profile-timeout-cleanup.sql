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
-- The retired protocol's terminal markers are gone. What must still be true
-- is the resource fact they stood for: a query that gave up releases its
-- backends rather than leaving them to a lease expiry. On the task protocol
-- that is the context abort, which the statement-deadline path sends to every
-- context before it returns.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.sqlx2_profile_timeout_cleanup (
  id BIGINT,
  delay_s BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.sqlx2_profile_timeout_cleanup VALUES (1, 3);
INSERT INTO ${case_db}.sqlx2_profile_timeout_cleanup VALUES (2, 3);
INSERT INTO ${case_db}.sqlx2_profile_timeout_cleanup VALUES (3, 3);

-- query 3
-- SQLX-2 projects execution metrics into SQL-owned EXPLAIN facts and observes
-- the admission-frozen deadline without exposing execution profile objects to
-- the compiler. The timeout must abort every participant and leave no state
-- that contaminates the next EXPLAIN ANALYZE request.
-- @expect_error=query timed out after 1000 ms
-- @be_log_be_count_at_least=NOVAROCKS_TASK_CONTEXT_ABORT_APPLIED,3
SET query_timeout = 1;
EXPLAIN ANALYZE
SELECT COUNT(*)
FROM ${case_db}.sqlx2_profile_timeout_cleanup
WHERE sleep(delay_s);

-- query 4
-- @skip_result_check=true
SET query_timeout = 0;

-- query 5
-- @skip_result_check=true
-- @result_contains=Planning:
-- @result_contains=Profile: fragments=
EXPLAIN ANALYZE SELECT COUNT(*) FROM ${case_db}.sqlx2_profile_timeout_cleanup;

-- query 6
-- @result_contains=3
SELECT COUNT(*) FROM ${case_db}.sqlx2_profile_timeout_cleanup;
