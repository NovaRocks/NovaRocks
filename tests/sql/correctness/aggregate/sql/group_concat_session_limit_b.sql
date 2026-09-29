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


-- Explicit order makes byte-prefix expectations independent of task arrival.
-- @order_sensitive=true
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_concat_limit (id INT, v VARCHAR, u VARCHAR);
INSERT INTO ${case_db}.t_concat_limit VALUES (1, 'abcdef', '李四');
INSERT INTO ${case_db}.t_concat_limit VALUES (2, 'ghijkl', '王五');
INSERT INTO ${case_db}.t_concat_limit VALUES (3, NULL, NULL);

-- query 2
SELECT group_concat(v ORDER BY id SEPARATOR '|') AS value,
       count(*) AS rows_seen FROM ${case_db}.t_concat_limit;

-- query 3
-- @skip_result_check=true
SET group_concat_max_len = 8;

-- query 4
SELECT group_concat(v ORDER BY id SEPARATOR '|') AS value,
       count(*) AS rows_seen FROM ${case_db}.t_concat_limit;

-- query 5
-- @skip_result_check=true
SET group_concat_max_len = 121;

-- query 6
SELECT group_concat(v ORDER BY id SEPARATOR '|') AS value,
       count(*) AS rows_seen FROM ${case_db}.t_concat_limit;

-- query 7
-- @skip_result_check=true
SET group_concat_max_len = 4;

-- query 8
SELECT group_concat(v ORDER BY id SEPARATOR '|') AS value,
       count(*) AS rows_seen FROM ${case_db}.t_concat_limit;

-- query 9
-- @skip_result_check=true
SET group_concat_max_len = 1024;

-- query 10
SELECT group_concat(v ORDER BY id SEPARATOR '|') AS value,
       count(*) AS rows_seen FROM ${case_db}.t_concat_limit;

-- query 11
-- @skip_result_check=true
SET group_concat_max_len = 4;

-- query 12
SELECT group_concat(u ORDER BY id SEPARATOR '|') AS value
FROM ${case_db}.t_concat_limit;

-- query 13
-- @skip_result_check=true
SET group_concat_max_len = 5;

-- query 14
SELECT group_concat(u ORDER BY id SEPARATOR '|') AS value
FROM ${case_db}.t_concat_limit;

-- query 15
-- @skip_result_check=true
SET group_concat_max_len = 8;

-- query 16
SELECT group_concat(u ORDER BY id SEPARATOR '|') AS value
FROM ${case_db}.t_concat_limit;

-- query 17
-- @skip_result_check=true
SET group_concat_max_len = 1024;

-- query 18
SELECT group_concat(u ORDER BY id SEPARATOR '|') AS value
FROM ${case_db}.t_concat_limit;

-- query 19
-- @skip_result_check=true
DROP TABLE ${case_db}.t_concat_limit FORCE;
