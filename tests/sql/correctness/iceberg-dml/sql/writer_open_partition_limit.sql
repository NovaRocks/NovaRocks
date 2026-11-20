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

-- @order_sensitive=true
-- @sequential=true

-- query 1
-- @skip_result_check=true
SET SESSION pipeline_dop = 1;
CREATE TABLE ${case_db}.rwm13_open_limit (p BIGINT) WITH (partitioning = ARRAY['p']);

-- query 2
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
SET SESSION iceberg_dml_cat_${suite_uuid0}.max_partitions_per_writer = 101;

-- query 3
-- @skip_result_check=true
SET SESSION iceberg_dml_cat_${suite_uuid0}.max_partitions_per_writer = 1;

-- query 4
-- @expect_trino_error=ICEBERG_TOO_MANY_OPEN_PARTITIONS,USER_ERROR
INSERT INTO ${case_db}.rwm13_open_limit
SELECT generate_series FROM TABLE(generate_series(1, 17));

-- query 5
SELECT count(*) AS n FROM ${case_db}.rwm13_open_limit;

-- query 6
-- @skip_result_check=true
INSERT INTO ${case_db}.rwm13_open_limit VALUES ROW(42);

-- query 7
SELECT p FROM ${case_db}.rwm13_open_limit ORDER BY p;

-- query 8
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm13_open_limit ;
