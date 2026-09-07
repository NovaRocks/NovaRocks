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
-- Statistics are published against the snapshot they measured, and the table
-- keeps moving. Previously that made them unreadable the moment a write landed.
-- This case pins the whole shape: the distinct count survives the advance and
-- says which snapshot it came from, while the row count tracks the snapshot
-- actually being queried. Collect-on-write is disabled explicitly so the
-- append advances the table without publishing a replacement current-snapshot
-- sketch; collect-on-write current-snapshot union is covered separately.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0} (
    id BIGINT,
    k BIGINT
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 3
-- @skip_result_check=true
INSERT INTO statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0} VALUES
    (1, 10), (2, 20), (3, 30), (4, 40);

-- query 4
-- Measure snapshot S.
-- @skip_result_check=true
ANALYZE TABLE statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0};

-- query 5
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=SUCCEEDED
-- @skip_result_check=true
SHOW ANALYZE JOBS;

-- query 6
-- On S itself the sketch is published against the queried snapshot, so its
-- basis is that snapshot and its rows are the queried rows.
-- @retry_count=20
-- @retry_interval_ms=500
-- @result_contains=theta_ndv:k
-- @result_contains=PROVIDER_ARTIFACT
-- @result_contains=APPROXIMATE
-- @result_contains=SAME
-- @result_contains=IDENTICAL
-- @skip_result_check=true
SHOW TABLE STATS statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0};

-- query 7
-- Advance the table to S'. Nothing re-analyzes it.
-- @skip_result_check=true
INSERT INTO statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0} VALUES
    (5, 50), (6, 60);

-- query 8
-- The distinct count is still readable, and now says plainly that it was
-- measured somewhere else: a basis digest instead of SAME, and a basis holding
-- a subset of the rows now being queried. Before the ancestor walk this metric
-- simply vanished once the table moved.
-- @retry_count=20
-- @retry_interval_ms=500
-- @result_contains=theta_ndv:k
-- @result_contains=PROVIDER_ARTIFACT
-- @result_contains=BASIS_IS_SUBSET
-- @result_contains=sha256:
-- @skip_result_check=true
SHOW TABLE STATS statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0};

-- query 9
-- The row count is not carried along with it. It comes from the queried
-- snapshot's own manifest, so it reports six rows rather than the four the
-- sketch was measured over.
-- @result_contains=6
SELECT COUNT(*) FROM statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0};

-- query 10
-- @skip_result_check=true
DROP TABLE statistics_hadoop_${suite_uuid0}.nr_ancestor_${suite_uuid0}.advance_${uuid0};
