-- @order_sensitive=true
-- @tags=aggregate,array_agg,array_unique_agg,string_agg,dict_merge
-- Test Objective:
-- 1. Validate array/string collection aggregates on grouped nullable inputs.
-- 2. Cover dict_merge payload generation for string and array<string> columns.
-- Test Flow:
-- 1. Create/reset a mixed-type source table.
-- 2. Insert deterministic grouped rows with duplicates and NULLs.
-- 3. Snapshot grouped collection outputs and dict_merge payloads.
-- query 1
DROP TABLE IF EXISTS ${case_db}.t_agg_collection_string_dict;
CREATE TABLE ${case_db}.t_agg_collection_string_dict (
    g INT,
    id INT,
    name STRING,
    score INT,
    score_items ARRAY<INT>,
    tags ARRAY<STRING>,
    city STRING,
    city_null STRING,
    city_array ARRAY<STRING>,
    city_array_null ARRAY<STRING>
);

INSERT INTO ${case_db}.t_agg_collection_string_dict VALUES
    (1, 1, 'Tom', 90, [90, 90], ['a', 'b'], 'beijing', 'beijing', ['beijing', 'shanghai'], NULL),
    (1, 2, 'Tom', 80, [80], ['a', 'c'], 'beijing', NULL, ['shenzhen', 'shanghai'], ['shenzhen', 'shanghai']),
    (1, 3, 'May', NULL, [80], ['z'], 'shanghai', 'shanghai', ['shenzhen', NULL], ['shenzhen', NULL]),
    (2, 4, 'Ti', 98, [98], NULL, 'shanghai', NULL, ['beijing', NULL, 'shanghai'], NULL),
    (2, 5, 'Ti', 99, [99], ['x'], 'hangzhou', 'hangzhou', ['hangzhou'], ['hangzhou']),
    (2, 6, NULL, NULL, [], [], 'hangzhou', NULL, ['hangzhou', 'shanghai'], ['hangzhou', 'shanghai']),
    (3, 7, 'Zoe', 70, [70], ['n'], 'nanjing', 'nanjing', ['nanjing'], ['nanjing']);
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
    ARRAY_MIN(ARRAY_UNIQUE_AGG(score_items)) AS min_unique_score,
    ARRAY_MAX(ARRAY_UNIQUE_AGG(score_items)) AS max_unique_score,
    CARDINALITY(ARRAY_UNIQUE_AGG(score_items)) AS unique_score_count
FROM ${case_db}.t_agg_collection_string_dict
GROUP BY g
ORDER BY g;

-- query 3
SELECT
    g,
    STRING_AGG(name, '|' ORDER BY id) AS names_concat,
    STRING_AGG(DISTINCT CAST(score AS STRING), ',' ORDER BY 1) AS distinct_scores
FROM ${case_db}.t_agg_collection_string_dict
WHERE name IS NOT NULL
GROUP BY g
ORDER BY g;

-- query 4
SELECT
    DICT_MERGE(city, 255) AS city_dict,
    DICT_MERGE(city_null, 255) AS city_null_dict,
    DICT_MERGE(city_array, 255) AS city_array_dict,
    DICT_MERGE(city_array_null, 255) AS city_array_null_dict
FROM ${case_db}.t_agg_collection_string_dict;
