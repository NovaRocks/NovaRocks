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
-- @tags=self_contained
-- Test Objective: Bind original parameter positions and preserve prepared metadata.

-- query 1
-- @skip_result_check=true
PREPARE prepared_rows FROM SELECT CAST(? AS BIGINT) AS n;

-- query 2
EXECUTE prepared_rows USING 41;

-- query 3
EXECUTE prepared_rows USING CAST(NULL AS BIGINT);

-- query 4
DESCRIBE INPUT prepared_rows;

-- query 5
DESCRIBE OUTPUT prepared_rows;

-- query 6
-- @skip_result_check=true
DEALLOCATE PREPARE prepared_rows;

-- query 7
EXECUTE IMMEDIATE 'WITH t(x) AS (VALUES ?) SELECT x, ? FROM t' USING 11, 22;

-- query 8
EXECUTE IMMEDIATE 'SELECT IF(false, ?, 42)' USING CAST(1 / 0 AS INTEGER);

-- query 9
EXECUTE IMMEDIATE 'SELECT x FROM (VALUES 1, 2, 3) AS t(x) ORDER BY x LIMIT ?' USING 1 + 1;

-- query 10
EXECUTE IMMEDIATE 'WITH SESSION pipeline_dop = ? SELECT 42 AS value' USING 1;

-- query 11
DESCRIBE OUTPUT (SELECT CAST(NULL AS CHAR(4)) AS "é😀", CAST(NULL AS TIMESTAMP(3) WITH TIME ZONE), CAST(NULL AS VARCHAR(4)));

-- query 12
EXECUTE IMMEDIATE 'SELECT x, 10 - x AS y FROM (VALUES 1, 2, 3) AS t(x) ORDER BY ?, x' USING 2;

-- query 13
-- @skip_result_check=true
PREPARE no_parameters FROM SELECT 1;

-- query 14
DESCRIBE INPUT no_parameters;

-- query 15
-- @skip_result_check=true
DEALLOCATE PREPARE no_parameters;

-- query 16
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR
EXECUTE IMMEDIATE 'WITH SESSION pipeline_dop = ? SELECT 42 AS value' USING 'not-an-integer';
