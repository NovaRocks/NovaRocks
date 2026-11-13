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
-- @order_sensitive=true
-- Correctness owner: SHOW STATS FOR. Trino 484 E03/E04/X04/X06/X10/F09
-- and plan1.4 independently define these deterministic fixed-width inputs.
-- No candidate output supplies the expected; variable-width byte estimates
-- and renderer edge cases are protected by their P02 owner unit tests.
-- The case drops its tables; the suite owns the private REST fixture lifecycle.

-- query 1
-- @skip_result_check=true
CREATE DATABASE statistics_cat_${suite_uuid0}.trc_stats_${uuid0};
CREATE TABLE statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.empty_rows (k BIGINT)
TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');
CREATE TABLE statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows (k BIGINT, only_null BIGINT)
TBLPROPERTIES ('novarocks.statistics.collect-on-write' = 'false');

-- query 2
SHOW STATS FOR statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.empty_rows;

-- query 3
-- @skip_result_check=true
-- Command completion shape is checked by the raw command-completion System scenario.
ANALYZE TABLE statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.empty_rows;

-- query 4
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows VALUES
(1, NULL), (2, NULL), (NULL, NULL);

-- query 5
-- Manifest facts are visible even before any NDV file exists.
-- Even an all-NULL column keeps unknown NDV until that column is analyzed.
SHOW STATS FOR statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows;

-- query 6
-- Partial ANALYZE publishes before its rows=NULL completion.
-- @skip_result_check=true
-- Command completion shape is checked by the raw command-completion System scenario.
ANALYZE TABLE statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows (k);

-- query 7
SHOW STATS FOR statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows;

-- query 8
-- @skip_result_check=true
INSERT INTO statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows VALUES (3, NULL);

-- query 9
-- The ancestor NDV stays two; current rows, null fraction and upper bound advance.
SHOW STATS FOR statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows;

-- query 10
-- @expect_trino_error=TABLE_NOT_FOUND,USER_ERROR
SHOW STATS FOR statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.no_such_table;

-- query 11
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW STATS FOR system.metadata.materialized_views;

-- query 12
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SHOW STATS FOR (SELECT 1);

-- query 13
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.empty_rows FORCE;
DROP TABLE IF EXISTS statistics_cat_${suite_uuid0}.trc_stats_${uuid0}.sample_rows FORCE;
DROP DATABASE IF EXISTS statistics_cat_${suite_uuid0}.trc_stats_${uuid0};
