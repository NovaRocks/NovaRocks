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

-- Test Objective: ordinary nested equality and null-safe equality have distinct null contracts.
-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE nested_null_comparison (id INT, a ARRAY<VARCHAR(10)>)
TBLPROPERTIES ("format-version" = "3");
INSERT INTO nested_null_comparison VALUES (1, ['a','b']), (2, ['aa',NULL]), (3,NULL);

-- query 2
USE ${case_db};
SELECT id, a = a AS eq, a != a AS ne, a <=> a AS safe
FROM nested_null_comparison ORDER BY id;

-- query 3
USE ${case_db};
SELECT id FROM nested_null_comparison WHERE a = CAST(a AS ARRAY<CHAR(10)>) ORDER BY id;

-- query 4
USE ${case_db};
SELECT [1,CAST(NULL AS BIGINT)] = [2,CAST(NULL AS BIGINT)] AS early_mismatch,
       [1,CAST(NULL AS BIGINT),3] = [1,CAST(NULL AS BIGINT),4] AS late_mismatch,
       [1,CAST(NULL AS BIGINT)] = [1,CAST(NULL AS BIGINT),3] AS length_mismatch,
       CAST([] AS ARRAY<BIGINT>) = CAST([] AS ARRAY<BIGINT>) AS empty_eq;

-- query 5
USE ${case_db};
SELECT [[1,CAST(NULL AS BIGINT)]] = [[1,CAST(NULL AS BIGINT)]] AS nested_eq,
       [[1,CAST(NULL AS BIGINT)]] != [[1,CAST(NULL AS BIGINT)]] AS nested_ne,
       [[1,CAST(NULL AS BIGINT)]] <=> [[1,CAST(NULL AS BIGINT)]] AS nested_safe,
       MAP(1,CAST(NULL AS BIGINT)) = MAP(1,CAST(NULL AS BIGINT)) AS map_eq,
       MAP(1,CAST(NULL AS BIGINT)) <=> MAP(1,CAST(NULL AS BIGINT)) AS map_safe;

-- query 6
-- @skip_result_check=true
USE ${case_db};
DROP TABLE nested_null_comparison;
