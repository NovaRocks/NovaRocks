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
-- @order_sensitive=false
-- @tags=column-naming,mv,iceberg,aggregate-admission
-- Test Objective: Validate aggregate arity and unnamed MV output admission independently of refresh capability.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc_l7_measurements (
  k INT,
  v BIGINT,
  d DECIMAL(20, 4),
  s VARCHAR,
  ts TIMESTAMP(6)
) WITH (format_version = 3);

-- query 2
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_bad_avg_star AS SELECT k, AVG(*) AS invalid_avg FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 3
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_bad_avg_string AS SELECT k, AVG(s) FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 4
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_bad_min_star AS SELECT k, MIN(*) AS invalid_min FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 5
-- @expect_trino_error=FUNCTION_NOT_FOUND,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_retired_call AS
SELECT md5sum(s) AS v FROM ${case_db}.trc_l7_measurements;

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.trc_l7_measurements (k, v) VALUES ROW(1, 10);
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_count_control AS
SELECT k, count(*) AS n FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 7
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_count_filtered AS
SELECT k, count(*) FILTER (WHERE v > 0) AS n
FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 8
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_unnamed_avg AS
SELECT k, AVG(v) FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 9
-- @expect_error=Iceberg IMV requires an installed persistent state author for overload builtin.aggregate/approx_distinct/default-hll-v1
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_approx_distinct AS
SELECT k, approx_distinct(v) AS n FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 10
-- @expect_error=Iceberg IMV requires an installed persistent state author for overload builtin.aggregate/approx_distinct/explicit-error-hll-v1
CREATE MATERIALIZED VIEW ${case_db}.trc_l7_approx_distinct_explicit AS
SELECT k, approx_distinct(v, DOUBLE '0.023') AS n
FROM ${case_db}.trc_l7_measurements GROUP BY k;

-- query 11
SELECT table_name
FROM iceberg_ivm_cat_${suite_uuid0}.information_schema.tables
WHERE table_schema = '${case_db}'
  AND table_name IN ('trc_l7_count_control', 'trc_l7_count_filtered', 'trc_l7_approx_distinct', 'trc_l7_approx_distinct_explicit');

-- query 12
SELECT name
FROM system.runtime.materialized_view_states
WHERE catalog_name = 'iceberg_ivm_cat_${suite_uuid0}'
  AND schema_name = '${case_db}'
  AND name IN ('trc_l7_count_control', 'trc_l7_count_filtered', 'trc_l7_approx_distinct', 'trc_l7_approx_distinct_explicit');

-- query 13
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_bad_avg_star;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_bad_avg_string;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_bad_min_star;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_retired_call;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_count_filtered;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_count_control;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_unnamed_avg;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_approx_distinct;
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_l7_approx_distinct_explicit;
DROP TABLE IF EXISTS ${case_db}.trc_l7_measurements;
