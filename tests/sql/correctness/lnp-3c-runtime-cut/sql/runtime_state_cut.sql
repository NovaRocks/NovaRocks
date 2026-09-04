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
-- This is the authoritative LNP-3C product acceptance: the runner owns one
-- FE and three independent BEs, publishes a statistics artifact, restarts FE,
-- then proves lake truth survives while the old process-local job is absent.

-- query 1
-- @skip_result_check=true
CREATE DATABASE lnp_3c_${suite_uuid0}.ns_${uuid0};
CREATE TABLE lnp_3c_${suite_uuid0}.ns_${uuid0}.orders (id BIGINT, value BIGINT)
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- Three distinct commits produce independent Iceberg scan tasks.  The native
-- marker below consequently proves all three BEs contributed a partial.
-- @skip_result_check=true
INSERT INTO lnp_3c_${suite_uuid0}.ns_${uuid0}.orders VALUES (1, 10);

-- query 3
-- @skip_result_check=true
INSERT INTO lnp_3c_${suite_uuid0}.ns_${uuid0}.orders VALUES (2, 20);

-- query 4
-- @skip_result_check=true
INSERT INTO lnp_3c_${suite_uuid0}.ns_${uuid0}.orders VALUES (3, 30);

-- query 5
-- @restart_fe_after_step=true
-- @be_log_be_count_at_least=NOVAROCKS_QUERY_FRAGMENT_ACCEPTED,3
-- @skip_result_check=true
ANALYZE TABLE lnp_3c_${suite_uuid0}.ns_${uuid0}.orders;

-- query 6
-- The previous FE incarnation cannot recover a terminal statistics job.
-- @result_not_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 7
-- The provider-owned published artifact and table data remain readable after
-- the FE restart; this observation never consults old job state.
-- @result_contains=row_count
-- @result_contains=3
-- @result_contains=AVAILABLE
-- @skip_result_check=true
SHOW TABLE STATS lnp_3c_${suite_uuid0}.ns_${uuid0}.orders;

-- query 8
-- A new intent belongs to the new process and may publish a fresh artifact.
-- @skip_result_check=true
ANALYZE TABLE lnp_3c_${suite_uuid0}.ns_${uuid0}.orders;

-- query 9
-- A completed maintenance command must leave provider-owned lake truth that
-- survives a process restart.  It must not recover an old FE job record.
-- @restart_fe_after_step=true
-- @skip_result_check=true
ALTER TABLE lnp_3c_${suite_uuid0}.ns_${uuid0}.orders REWRITE MANIFESTS;

-- query 10
-- @result_contains=3
-- @skip_result_check=true
SELECT COUNT(*) AS n FROM lnp_3c_${suite_uuid0}.ns_${uuid0}.orders;

-- query 11
-- @skip_result_check=true
DROP TABLE lnp_3c_${suite_uuid0}.ns_${uuid0}.orders FORCE;
DROP DATABASE lnp_3c_${suite_uuid0}.ns_${uuid0};
