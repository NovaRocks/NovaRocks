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

-- A p7 input distinguishes both keys. At the p6 storage target they collide.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.trc6c_map_input (payload ARRAY(MAP(TIMESTAMP(9), BIGINT)))
WITH (format_version = 3);
CREATE TABLE ${case_db}.trc6c_map_output (payload ARRAY(MAP(TIMESTAMP(6), BIGINT)))
WITH (format_version = 2);
INSERT INTO ${case_db}.trc6c_map_output
SELECT ARRAY[MAP_AGG(instant, measure)] FROM (
    VALUES ROW(TIMESTAMP '1970-01-01 00:00:00.000000', CAST(10 AS BIGINT))
) AS seed(instant, measure);
INSERT INTO ${case_db}.trc6c_map_input
SELECT ARRAY[MAP_AGG(instant, measure)] FROM (
    VALUES ROW(TIMESTAMP '1970-01-01 00:00:00.0000001', CAST(1 AS BIGINT)),
           ROW(TIMESTAMP '1970-01-01 00:00:00.0000002', CAST(2 AS BIGINT))
) AS seed(instant, measure);

-- query 2
SELECT count(*) AS snapshots FROM ${case_db}."trc6c_map_output$snapshots";

-- query 3
-- @expect_error=Data failure (reason 4:1)
INSERT INTO ${case_db}.trc6c_map_output SELECT payload FROM ${case_db}.trc6c_map_input;

-- query 4
SELECT count(*) AS snapshots FROM ${case_db}."trc6c_map_output$snapshots";

-- query 5
SELECT count(*) AS persisted_rows FROM ${case_db}.trc6c_map_output;

-- query 6
-- @skip_result_check=true
DROP TABLE ${case_db}.trc6c_map_input;
DROP TABLE ${case_db}.trc6c_map_output;
