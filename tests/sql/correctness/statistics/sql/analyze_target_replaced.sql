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
-- The REST ANALYZE has published before the next statement can read NDV.
-- @result_rows_where=column_name=k
-- @result_rows_count=1
-- @result_rows_expect=distinct_values_count=1.0
-- @result_rows_expect=low_value=37
-- @result_rows_expect=high_value=37
-- @skip_result_check=true
SHOW STATS FOR statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0};

-- query 6
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} FORCE;

-- query 7
-- @skip_result_check=true
CREATE TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} (
    k BIGINT NOT NULL
) TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 8
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0} VALUES
    (99);

-- query 9
-- The current table has a new physical identity. It must not inherit the
-- predecessor's NDV or bounds for the former value 37.
SHOW STATS FOR statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0};

-- query 10
-- @skip_result_check=true
DROP TABLE statistics_cat_${suite_uuid0}.nr_replaced_${suite_uuid0}.same_name_${uuid0};
