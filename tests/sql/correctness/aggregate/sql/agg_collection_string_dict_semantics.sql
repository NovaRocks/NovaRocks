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
-- @tags=aggregate,array_agg,string_agg
-- Check grouped array and string aggregation with nullable inputs.
-- query 1
CREATE TABLE ${case_db}.t_agg_collection_string_dict (
    g INT,
    id INT,
    name VARCHAR,
    score INT,
    score_items ARRAY<INT>,
    tags ARRAY<VARCHAR>,
    city VARCHAR,
    city_null VARCHAR,
    city_array ARRAY<VARCHAR>,
    city_array_null ARRAY<VARCHAR>
) WITH (format_version = 3);

INSERT INTO ${case_db}.t_agg_collection_string_dict VALUES
    ROW(1, 1, 'Tom', 90, [90, 90], ['a', 'b'], 'beijing', 'beijing', ['beijing', 'shanghai'], NULL),
    ROW(1, 2, 'Tom', 80, [80], ['a', 'c'], 'beijing', NULL, ['shenzhen', 'shanghai'], ['shenzhen', 'shanghai']),
    ROW(1, 3, 'May', NULL, [80], ['z'], 'shanghai', 'shanghai', ['shenzhen', NULL], ['shenzhen', NULL]),
    ROW(2, 4, 'Ti', 98, [98], NULL, 'shanghai', NULL, ['beijing', NULL, 'shanghai'], NULL),
    ROW(2, 5, 'Ti', 99, [99], ['x'], 'hangzhou', 'hangzhou', ['hangzhou'], ['hangzhou']),
    ROW(2, 6, NULL, NULL, [], [], 'hangzhou', NULL, ['hangzhou', 'shanghai'], ['hangzhou', 'shanghai']),
    ROW(3, 7, 'Zoe', 70, [70], ['n'], 'nanjing', 'nanjing', ['nanjing'], ['nanjing']);
SELECT
    g,
    ARRAY_AGG(name ORDER BY id) AS names_all,
    ARRAY_AGG(DISTINCT name ORDER BY name) AS names_distinct
FROM ${case_db}.t_agg_collection_string_dict
WHERE name IS NOT NULL
GROUP BY g
ORDER BY g;

-- query 2
SELECT
    g,
    STRING_AGG(name, '|' ORDER BY id) AS names_concat,
    STRING_AGG(DISTINCT CAST(score AS VARCHAR), ',' ORDER BY 1) AS distinct_scores
FROM ${case_db}.t_agg_collection_string_dict
WHERE name IS NOT NULL
GROUP BY g
ORDER BY g;
