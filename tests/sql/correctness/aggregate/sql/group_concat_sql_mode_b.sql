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
-- Exercise one connection; run both suffixes concurrently.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_concat_mode (id INT, v VARCHAR, sep VARCHAR);
CREATE TABLE ${case_db}.t_concat_output (v VARCHAR);
INSERT INTO ${case_db}.t_concat_mode VALUES ROW(1,'a','|'),ROW(2,'b','|');
SET SESSION sql_mode = '32';

-- query 2
SELECT group_concat(v ORDER BY id) FROM ${case_db}.t_concat_mode;

-- query 3
SELECT group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 4
WITH SESSION sql_mode = 'GROUP_CONCAT_LEGACY' SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 5
SELECT group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 6
WITH SESSION sql_mode = '32' SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ /*+ SET_VAR('sql_mode'='32') */  group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 7
SELECT group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 8
SELECT string_agg(v,'-'
,
 '|'
ORDER BY id
) AS value FROM ${case_db}.t_concat_mode;

-- query 9
WITH SESSION sql_mode = 'GROUP_CONCAT_LEGACY' SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v,sep ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 10
-- @expect_error=ORDER BY position 2 is not in group_concat output list.
WITH SESSION sql_mode = 'GROUP_CONCAT_LEGACY' SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ group_concat(v,'-' ORDER BY 2) FROM ${case_db}.t_concat_mode;

-- query 11
WITH SESSION sql_mode = '32' SELECT /*+ SET_VAR(sql_mode='32') */ group_concat(v,'-' ORDER BY 2,id) AS value FROM ${case_db}.t_concat_mode;

-- query 12
SELECT string_agg(v,sep ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 13
-- @skip_result_check=true
SET SESSION sql_mode = 'ERROR_IF_OVERFLOW,STRUCT_CAST_BY_NAME';

-- query 14
SELECT group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 15
-- @skip_result_check=true
SET SESSION sql_mode = 'GROUP_CONCAT_LEGACY';
INSERT INTO ${case_db}.t_concat_output SELECT group_concat(v,'-' ORDER BY id) FROM ${case_db}.t_concat_mode;

-- query 16
SELECT v AS value FROM ${case_db}.t_concat_output;

-- query 17
-- @skip_result_check=true
EXPLAIN WITH SESSION sql_mode = '32'  SELECT /*+ SET_VAR(sql_mode='32') */ group_concat(v,'-' ORDER BY id) FROM ${case_db}.t_concat_mode;

-- query 18
SELECT group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 19
-- @skip_result_check=true
SET SESSION sql_mode = '32';

-- query 20
-- One root policy reaches the CTE; the next step checks connection isolation.
WITH SESSION sql_mode = 'GROUP_CONCAT_LEGACY' WITH c AS (SELECT group_concat(v ORDER BY id) AS x FROM ${case_db}.t_concat_mode) SELECT /*+ SET_VAR(sql_mode='GROUP_CONCAT_LEGACY') */ x AS value FROM c;

-- query 21
SELECT group_concat(v,'-' ORDER BY id) AS value FROM ${case_db}.t_concat_mode;

-- query 22
-- @skip_result_check=true
DROP TABLE ${case_db}.t_concat_output ;
DROP TABLE ${case_db}.t_concat_mode ;
