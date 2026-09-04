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
-- This case proves the stable SQL-visible half of same-name replacement: a
-- newly created physical table must not expose the predecessor's artifact.
-- ANALYZE is synchronous, so one runner session cannot deterministically
-- interleave DROP/CREATE between capture and worker rebind. Do not assert
-- STALE/TARGET_REPLACED here; that typed race is covered by frontend tests.

-- query 1
-- @skip_result_check=true
CREATE DATABASE IF NOT EXISTS statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} (
    k BIGINT NOT NULL
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 3
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} VALUES
    (37);

-- query 4
-- @skip_result_check=true
ANALYZE TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} (k);

-- query 5
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} FORCE;

-- query 6
-- @skip_result_check=true
CREATE TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} (
    k BIGINT NOT NULL
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 7
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} VALUES
    (99);

-- query 8
-- The current table has a new physical identity. It must not inherit the
-- predecessor's published provider artifact or its row count of 37.
-- @result_contains=row_count
-- @result_not_contains=PROVIDER_ARTIFACT
-- @result_not_contains=37
-- @skip_result_check=true
SHOW TABLE STATS statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0};

-- query 9
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0};
