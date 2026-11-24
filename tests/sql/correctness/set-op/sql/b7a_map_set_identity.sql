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
-- Opposite entry orders originate in separate scanned relations.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7a_map_left (m MAP(INTEGER, INTEGER));

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.b7a_map_right (m MAP(INTEGER, INTEGER));

-- query 3
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_map_left
VALUES ROW(MAP(ARRAY[1, 2], ARRAY[11, CAST(NULL AS INTEGER)]));

-- query 4
-- @skip_result_check=true
INSERT INTO ${case_db}.b7a_map_right
VALUES ROW(MAP(ARRAY[2, 1], ARRAY[CAST(NULL AS INTEGER), 11]));

-- query 5
-- @explain_contains=HASH EXCHANGE
SELECT count(*) AS row_count FROM (
    SELECT m FROM ${case_db}.b7a_map_left
    UNION DISTINCT
    SELECT m FROM ${case_db}.b7a_map_right
);

-- query 6
-- @explain_contains=HASH EXCHANGE
SELECT count(*) AS row_count FROM (
    SELECT ARRAY[m] FROM ${case_db}.b7a_map_left
    INTERSECT
    SELECT ARRAY[m] FROM ${case_db}.b7a_map_right
);
