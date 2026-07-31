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

-- The canonical STAT-1 acceptance uses the runner-owned native 1FE+3BE
-- topology. Three independent writes produce multiple Iceberg scan tasks;
-- the BE marker below proves all three collection participants emitted a
-- non-empty internal partial. The FE is restarted after the durable JobRecord
-- is committed, so the recovered worker (not the client session) owns the
-- eventual collection and publication.
-- @sequential=true

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.statistics_recovery (
  id BIGINT,
  value BIGINT
)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.statistics_recovery VALUES (1, 10);

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.statistics_recovery VALUES (2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.statistics_recovery VALUES (3, 30);

-- query 5
-- @restart_fe_after_step=true
-- @skip_result_check=true
ANALYZE TABLE ${case_db}.statistics_recovery;

-- query 6
-- The submitted job must survive the FE restart and finish under a new worker
-- lease. Retrying only observes the durable state; it never re-submits ANALYZE.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @be_log_be_count_at_least=NOVAROCKS_STATISTICS_FRAGMENT_COLLECTED,3
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 7
-- @retry_count=20
-- @retry_interval_ms=500
-- @result_contains=row_count
-- @result_contains=3
-- @result_contains=AVAILABLE
-- @skip_result_check=true
SHOW TABLE STATS ${case_db}.statistics_recovery;

-- A new data version must require a fresh ANALYZE result rather than exposing
-- the prior snapshot as current evidence.
-- query 8
-- @skip_result_check=true
DELETE FROM ${case_db}.statistics_recovery WHERE id = 3;

-- query 9
-- @skip_result_check=true
ANALYZE TABLE ${case_db}.statistics_recovery;

-- query 10
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 11
-- @retry_count=20
-- @retry_interval_ms=500
-- @result_contains=row_count
-- @result_contains=2
-- @result_contains=AVAILABLE
-- @skip_result_check=true
SHOW TABLE STATS ${case_db}.statistics_recovery;
