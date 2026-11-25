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

-- The Iceberg column stores original JSON text as VARCHAR. Explicit parsing
-- produces the JSON value consumed by CAST for each row.
-- Preserve VARCHAR and INTEGER keys, JSON vs SQL null, and nested values.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_cast_json_to_map;
CREATE TABLE ${case_db}.t_cast_json_to_map (
    c1 INT,
    c2 VARCHAR
) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.t_cast_json_to_map VALUES
ROW(1, '[1,2,3]'),
ROW(2, '"abc"'),
ROW(3, 'null'),
ROW(4, 'true'),
ROW(5, '1'),
ROW(6, '{"1":1, "2":true, "3":null, "4":[5,6,7], "5":{"k51":"v51","k52":"v52"}}');

-- query 3
-- @order_sensitive=true
-- A nonobject shape fails; a JSON null input produces SQL NULL.
SELECT c1, TRY_CAST(PARSE_JSON(c2) AS MAP(VARCHAR, JSON)) AS m
FROM ${case_db}.t_cast_json_to_map ORDER BY c1;

-- query 4
-- @order_sensitive=true
SELECT c1, TRY_CAST(PARSE_JSON(c2) AS MAP(INTEGER, JSON)) AS m
FROM ${case_db}.t_cast_json_to_map ORDER BY c1;

-- query 5
-- @order_sensitive=true
-- Conversion to VARCHAR fails for JSON containers, while scalar JSON null becomes SQL NULL.
SELECT c1, TRY_CAST(PARSE_JSON(c2) AS MAP(VARCHAR, VARCHAR)) AS m
FROM ${case_db}.t_cast_json_to_map
UNION ALL
SELECT 7, CAST(JSON '{"1":1,"2":true,"3":null}' AS MAP(VARCHAR, VARCHAR))
ORDER BY c1;

-- query 6
-- @order_sensitive=true
SELECT c1, TRY_CAST(PARSE_JSON(c2) AS MAP(INTEGER, VARCHAR)) AS m
FROM ${case_db}.t_cast_json_to_map
UNION ALL
SELECT 7, CAST(JSON '{"1":1,"2":true,"3":null}' AS MAP(INTEGER, VARCHAR))
ORDER BY c1;

-- query 7
-- @order_sensitive=true
-- A bad child fails the entire MAP. Preserve the valid array and SQL NULL values independently.
SELECT c1, TRY_CAST(PARSE_JSON(c2) AS MAP(VARCHAR, ARRAY(INTEGER))) AS m
FROM ${case_db}.t_cast_json_to_map
UNION ALL
SELECT 7, CAST(JSON '{"1":null,"2":null,"3":null,"4":[5,6,7],"5":null}' AS MAP(VARCHAR, ARRAY(INTEGER)))
ORDER BY c1;

-- query 8
-- @order_sensitive=true
-- ROW accepts an array of exact arity or an object with matching field names.
SELECT c1, TRY_CAST(PARSE_JSON(c2) AS MAP(VARCHAR, ROW(k51 VARCHAR, k52 VARCHAR))) AS m
FROM ${case_db}.t_cast_json_to_map
UNION ALL
SELECT 7, CAST(JSON '{"1":null,"2":null,"3":null,"4":[5,6],"5":{"k51":"v51","k52":"v52"}}'
              AS MAP(VARCHAR, ROW(k51 VARCHAR, k52 VARCHAR)))
ORDER BY c1;

-- query 9
-- @order_sensitive=true
SELECT c1, TRY_CAST(PARSE_JSON(c2) AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR))) AS m
FROM ${case_db}.t_cast_json_to_map
UNION ALL
SELECT 7, CAST(JSON '{"1":null,"2":null,"3":null,"4":null,"5":{"k51":"v51","k52":"v52"}}'
              AS MAP(VARCHAR, MAP(VARCHAR, VARCHAR)))
ORDER BY c1;

-- query 10
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(PARSE_JSON(c2) AS MAP(VARCHAR, JSON))
FROM ${case_db}.t_cast_json_to_map WHERE c1 = 1;

-- query 11
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(PARSE_JSON(c2) AS MAP(VARCHAR, ARRAY(INTEGER)))
FROM ${case_db}.t_cast_json_to_map WHERE c1 = 6;

-- query 12
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_cast_json_to_map;
