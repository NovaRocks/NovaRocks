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

-- @tags=join,large_in_predicate
-- Test Objective:
-- 1. Validate large IN predicate rewrite to join optimization.
-- 2. Cover integer, string, date types in IN lists; expressions inside IN;
--    subqueries with IN; NOT IN; nested subqueries; aggregation; window functions;
--    multi-table joins; CTE; special characters; empty results; query cache interaction.
-- Test Flow:
-- 1. Create five tables: t_int, t_string, t_mixed, t_large, t_dimension.
-- 2. Insert test data covering positive, negative, NULL, empty string, special chars.
-- 3. Run diverse IN/NOT IN queries across types, with subqueries, joins, CTEs,
--    window functions, aggregations, and edge cases.

-- query 1
-- @skip_result_check=true

-- query 2
-- @skip_result_check=true

-- query 3
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_int;

-- query 4
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_string;

-- query 5
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_mixed;

-- query 6
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_large;

-- query 7
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_dimension;

-- query 8
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_int (
    id int NOT NULL,
    tinyint_col tinyint,
    smallint_col smallint,
    int_col int,
    bigint_col bigint,
    value varchar(50)
) WITH (format_version = 3);

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_string (
    id int NOT NULL,
    varchar_col varchar(50),
    char_col char(10),
    text_col VARCHAR,
    category varchar(20)
) WITH (format_version = 3);

-- query 10
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_mixed (
    id int NOT NULL,
    int_val int,
    str_val varchar(50),
    float_val REAL,
    double_val double,
    decimal_val decimal(10,2),
    date_val date,
    datetime_val TIMESTAMP(6),
    bool_val boolean
) WITH (format_version = 3);

-- query 11
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_large (
    id bigint NOT NULL,
    category_id int,
    status varchar(20),
    score double,
    created_date date
) WITH (format_version = 3);

-- query 12
-- @skip_result_check=true
CREATE TABLE ${case_db}.t_dimension (
    dim_id int NOT NULL,
    dim_name varchar(50),
    dim_type varchar(20)
) WITH (format_version = 3);

-- query 13
-- @skip_result_check=true
INSERT INTO ${case_db}.t_int VALUES
ROW(1, 10, 100, 1000, 10000, 'value1'),
ROW(2, 20, 200, 2000, 20000, 'value2'),
ROW(3, 30, 300, 3000, 30000, 'value3'),
ROW(4, 40, 400, 4000, 40000, 'value4'),
ROW(5, 50, 500, 5000, 50000, 'value5'),
ROW(6, 60, 600, 6000, 60000, 'value6'),
ROW(7, 70, 700, 7000, 70000, 'value7'),
ROW(8, 80, 800, 8000, 80000, 'value8'),
ROW(9, 90, 900, 9000, 90000, 'value9'),
ROW(10, 100, 1000, 10000, 100000, 'value10'),
ROW(11, -10, -100, -1000, -10000, 'negative1'),
ROW(12, -20, -200, -2000, -20000, 'negative2'),
ROW(13, 0, 0, 0, 0, 'zero'),
ROW(14, null, null, null, null, 'null_values'),
ROW(15, 15, 150, 1500, 15000, 'extra1');

-- query 14
-- @skip_result_check=true
INSERT INTO ${case_db}.t_string VALUES
ROW(1, 'apple', 'fruit', 'This is apple text', 'food'),
ROW(2, 'banana', 'fruit', 'This is banana text', 'food'),
ROW(3, 'carrot', 'vegetable', 'This is carrot text', 'food'),
ROW(4, 'dog', 'animal', 'This is dog text', 'pet'),
ROW(5, 'elephant', 'animal', 'This is elephant text', 'wild'),
ROW(6, 'fish', 'animal', 'This is fish text', 'aquatic'),
ROW(7, 'grape', 'fruit', 'This is grape text', 'food'),
ROW(8, 'house', 'building', 'This is house text', 'shelter'),
ROW(9, 'ice', 'water', 'This is ice text', 'cold'),
ROW(10, 'jungle', 'nature', 'This is jungle text', 'wild'),
ROW(11, 'a''b', 'special', 'Text with quote', 'test'),
ROW(12, 'c"d', 'special', 'Text with double quote', 'test'),
ROW(13, 'e\f', 'special', 'Text with backslash', 'test'),
ROW(14, '', 'empty', 'Empty string test', 'test'),
ROW(15, null, null, null, null);

-- query 15
-- @skip_result_check=true
INSERT INTO ${case_db}.t_mixed VALUES
ROW(1, 100, 'str100', 1.1, 1.11, 100.50, DATE '2024-01-01', TIMESTAMP '2024-01-01 10:00:00', true),
ROW(2, 200, 'str200', 2.2, 2.22, 200.75, DATE '2024-01-02', TIMESTAMP '2024-01-02 11:00:00', false),
ROW(3, 300, 'str300', 3.3, 3.33, 300.25, DATE '2024-01-03', TIMESTAMP '2024-01-03 12:00:00', true),
ROW(4, 400, 'str400', 4.4, 4.44, 400.00, DATE '2024-01-04', TIMESTAMP '2024-01-04 13:00:00', false),
ROW(5, 500, 'str500', 5.5, 5.55, 500.99, DATE '2024-01-05', TIMESTAMP '2024-01-05 14:00:00', true),
ROW(6, 600, 'str600', 6.6, 6.66, 600.10, DATE '2024-01-06', TIMESTAMP '2024-01-06 15:00:00', false),
ROW(7, 700, 'str700', 7.7, 7.77, 700.20, DATE '2024-01-07', TIMESTAMP '2024-01-07 16:00:00', true),
ROW(8, 800, 'str800', 8.8, 8.88, 800.30, DATE '2024-01-08', TIMESTAMP '2024-01-08 17:00:00', false),
ROW(9, 900, 'str900', 9.9, 9.99, 900.40, DATE '2024-01-09', TIMESTAMP '2024-01-09 18:00:00', true),
ROW(10, 1000, 'str1000', 10.0, 10.10, 1000.50, DATE '2024-01-10', TIMESTAMP '2024-01-10 19:00:00', false);

-- query 16
-- @skip_result_check=true
INSERT INTO ${case_db}.t_large VALUES
ROW(1, 1, 'active', 85.5, DATE '2024-01-01'),
ROW(2, 1, 'inactive', 72.3, DATE '2024-01-02'),
ROW(3, 2, 'active', 91.2, DATE '2024-01-03'),
ROW(4, 2, 'pending', 68.7, DATE '2024-01-04'),
ROW(5, 3, 'active', 94.1, DATE '2024-01-05'),
ROW(6, 3, 'inactive', 55.9, DATE '2024-01-06'),
ROW(7, 4, 'active', 88.8, DATE '2024-01-07'),
ROW(8, 4, 'pending', 77.4, DATE '2024-01-08'),
ROW(9, 5, 'active', 92.6, DATE '2024-01-09'),
ROW(10, 5, 'inactive', 63.2, DATE '2024-01-10'),
ROW(11, 1, 'active', 89.3, DATE '2024-01-11'),
ROW(12, 2, 'active', 95.7, DATE '2024-01-12'),
ROW(13, 3, 'pending', 71.8, DATE '2024-01-13'),
ROW(14, 4, 'active', 84.4, DATE '2024-01-14'),
ROW(15, 5, 'inactive', 59.6, DATE '2024-01-15'),
ROW(16, 1, 'pending', 78.9, DATE '2024-01-16'),
ROW(17, 2, 'active', 93.2, DATE '2024-01-17'),
ROW(18, 3, 'active', 87.1, DATE '2024-01-18'),
ROW(19, 4, 'inactive', 66.5, DATE '2024-01-19'),
ROW(20, 5, 'active', 90.8, DATE '2024-01-20');

-- query 17
-- @skip_result_check=true
INSERT INTO ${case_db}.t_dimension VALUES
ROW(1, 'Category A', 'primary'),
ROW(2, 'Category B', 'secondary'),
ROW(3, 'Category C', 'primary'),
ROW(4, 'Category D', 'tertiary'),
ROW(5, 'Category E', 'secondary'),
ROW(6, 'Category F', 'primary'),
ROW(7, 'Category G', 'tertiary'),
ROW(8, 'Category H', 'secondary'),
ROW(9, 'Category I', 'primary'),
ROW(10, 'Category J', 'tertiary');

-- ============================================================
-- Basic IN predicate on integer column
-- ============================================================
-- query 18
-- @order_sensitive=true
SELECT id, int_col, value FROM ${case_db}.t_int WHERE int_col IN (1000, 2000, 3000, 4000) ORDER BY id;

-- query 19
-- @order_sensitive=true
SELECT id, int_col, value FROM ${case_db}.t_int WHERE int_col NOT IN (1000, 2000, 3000, 4000) AND int_col IS NOT NULL ORDER BY id;

-- ============================================================
-- Basic IN predicate on varchar column
-- ============================================================
-- query 20
-- @order_sensitive=true
SELECT id, varchar_col, category FROM ${case_db}.t_string WHERE varchar_col IN ('apple', 'banana', 'carrot', 'dog') ORDER BY id;

-- query 21
-- @order_sensitive=true
SELECT id, varchar_col, category FROM ${case_db}.t_string WHERE varchar_col NOT IN ('apple', 'banana', 'carrot', 'dog') AND varchar_col IS NOT NULL ORDER BY id;

-- ============================================================
-- Other integer types: tinyint, smallint, bigint
-- ============================================================
-- query 22
-- @order_sensitive=true
SELECT id, tinyint_col FROM ${case_db}.t_int WHERE tinyint_col IN (10, 20, 30, 40) ORDER BY id;

-- query 23
-- @order_sensitive=true
SELECT id, smallint_col FROM ${case_db}.t_int WHERE smallint_col IN (100, 200, 300, 400) ORDER BY id;

-- query 24
-- @order_sensitive=true
SELECT id, bigint_col FROM ${case_db}.t_int WHERE bigint_col IN (10000, 20000, 30000, 40000) ORDER BY id;

-- ============================================================
-- char_col IN
-- ============================================================
-- query 25
-- @order_sensitive=true
SELECT id, char_col FROM ${case_db}.t_string WHERE char_col IN ('fruit', 'animal', 'building', 'nature') ORDER BY id;

-- ============================================================
-- Expressions inside IN: (col + 1000), upper(), cast, case, coalesce
-- ============================================================
-- query 26
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE (int_col + 1000) IN (2000, 3000, 4000, 5000) ORDER BY id;

-- query 27
-- @order_sensitive=true
SELECT id, varchar_col FROM ${case_db}.t_string WHERE upper(varchar_col) IN ('APPLE', 'BANANA', 'CARROT', 'DOG') ORDER BY id;

-- query 28
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE cast(int_col AS VARCHAR) IN ('1000', '2000', '3000', '4000') ORDER BY id;

-- query 29
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE CASE WHEN int_col > 5000 THEN int_col ELSE 0 END IN (6000, 7000, 8000, 9000) ORDER BY id;

-- query 30
-- @order_sensitive=true
SELECT id, tinyint_col FROM ${case_db}.t_int WHERE coalesce(tinyint_col, 0) IN (10, 20, 30, 40) ORDER BY id;

-- ============================================================
-- GROUP BY + HAVING with IN
-- ============================================================
-- query 31
-- @order_sensitive=true
SELECT int_col, count(*) AS cnt FROM ${case_db}.t_int GROUP BY int_col HAVING int_col IN (1000, 2000, 3000, 4000) ORDER BY int_col;

-- ============================================================
-- Subquery with IN
-- ============================================================
-- query 32
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (SELECT id FROM ${case_db}.t_int WHERE int_col IN (1000, 2000, 3000, 4000)) ORDER BY id;

-- query 33
-- @order_sensitive=true
SELECT id, varchar_col FROM ${case_db}.t_string WHERE id IN (SELECT id FROM ${case_db}.t_int WHERE int_col IN (1000, 2000, 3000, 4000, 5000)) ORDER BY id;

-- ============================================================
-- Nested subqueries with IN
-- ============================================================
-- query 34
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT id FROM ${case_db}.t_large WHERE category_id IN (
        SELECT dim_id FROM ${case_db}.t_dimension WHERE dim_id IN (1, 2, 3, 4, 5)
    )
) ORDER BY id;

-- query 35
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE int_col IN (
    SELECT max(int_col) FROM ${case_db}.t_int WHERE tinyint_col IN (10, 20, 30, 40, 50) GROUP BY tinyint_col
) ORDER BY id;

-- ============================================================
-- Combined IN conditions
-- ============================================================
-- query 36
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE
    id IN (SELECT id FROM ${case_db}.t_large WHERE category_id IN (1, 2, 3, 4))
    AND int_col IN (SELECT int_col FROM ${case_db}.t_int WHERE tinyint_col IN (10, 20, 30, 40))
ORDER BY id;

-- ============================================================
-- Subquery join with IN
-- ============================================================
-- query 37
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT l.id FROM ${case_db}.t_large l
    JOIN ${case_db}.t_dimension d ON l.category_id = d.dim_id
    WHERE d.dim_id IN (1, 2, 3, 4, 5)
) ORDER BY id;

-- ============================================================
-- EXISTS with IN
-- ============================================================
-- query 38
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int t1 WHERE EXISTS (
    SELECT 1 FROM ${case_db}.t_large t2
    WHERE t2.id = t1.id
    AND t2.category_id IN (1, 2, 3, 4, 5)
    AND t2.status IN ('active', 'pending', 'completed', 'processing')
) ORDER BY id;

-- ============================================================
-- Window function with IN filter
-- ============================================================
-- query 39
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT id FROM (
        SELECT id, category_id, row_number() OVER (PARTITION BY category_id ORDER BY score DESC) AS rn
        FROM ${case_db}.t_large WHERE category_id IN (1, 2, 3, 4, 5)
    ) ranked WHERE rn <= 2
) ORDER BY id;

-- ============================================================
-- CASE inside IN predicate
-- ============================================================
-- query 40
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT id FROM ${case_db}.t_large WHERE
    CASE WHEN score > 80 THEN category_id ELSE 0 END IN (1, 2, 3, 4, 5)
) ORDER BY id;

-- ============================================================
-- HAVING with IN in subquery
-- ============================================================
-- query 41
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT category_id FROM ${case_db}.t_large
    GROUP BY category_id
    HAVING category_id IN (1, 2, 3, 4, 5) AND avg(score) > 75
) ORDER BY id;

-- ============================================================
-- CTE with IN
-- ============================================================
-- query 42
-- @order_sensitive=true
WITH high_score_categories AS (
    SELECT category_id FROM ${case_db}.t_large
    WHERE score > 85 AND category_id IN (1, 2, 3, 4, 5, 6, 7, 8)
    GROUP BY category_id
)
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT category_id FROM high_score_categories
) ORDER BY id;

-- ============================================================
-- Scalar subquery with IN filter
-- ============================================================
-- query 43
-- @order_sensitive=true
SELECT id, int_col, round((
    SELECT avg(score) FROM ${case_db}.t_large WHERE category_id IN (1, 2, 3, 4, 5)
), 2) AS avg_score FROM ${case_db}.t_int WHERE id <= 5 ORDER BY id;

-- ============================================================
-- DISTINCT with IN in subquery
-- ============================================================
-- query 44
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT DISTINCT category_id FROM ${case_db}.t_large WHERE status IN ('active', 'pending', 'completed', 'processing')
) ORDER BY id;

-- ============================================================
-- ORDER BY + LIMIT in subquery with IN
-- ============================================================
-- query 45
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT id FROM ${case_db}.t_large WHERE category_id IN (1, 2, 3, 4, 5)
    ORDER BY score DESC LIMIT 10
) ORDER BY id;

-- ============================================================
-- Triple-nested subquery with IN
-- ============================================================
-- query 46
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT id FROM ${case_db}.t_large WHERE category_id IN (
        SELECT dim_id FROM ${case_db}.t_dimension WHERE dim_type IN (
            SELECT DISTINCT dim_type FROM ${case_db}.t_dimension WHERE dim_id IN (1, 2, 3, 4, 5)
        )
    )
) ORDER BY id;

-- ============================================================
-- Correlated subquery count with IN
-- ============================================================
-- query 47
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int t1 WHERE (
    SELECT count(*) FROM ${case_db}.t_large t2
    WHERE t2.category_id = t1.id AND t2.status IN ('active', 'pending', 'completed', 'processing')
) > 0 ORDER BY id;

-- ============================================================
-- Multi-join with IN in subquery
-- ============================================================
-- query 48
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT l.id FROM ${case_db}.t_large l
    JOIN ${case_db}.t_dimension d1 ON l.category_id = d1.dim_id
    JOIN ${case_db}.t_dimension d2 ON d1.dim_id = d2.dim_id
    WHERE l.category_id IN (1, 2, 3, 4, 5) AND d1.dim_type IN ('primary', 'secondary', 'tertiary', 'quaternary')
) ORDER BY id;

-- ============================================================
-- Self-referencing subquery with IN
-- ============================================================
-- query 49
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT l1.id FROM ${case_db}.t_large l1 WHERE l1.category_id IN (
        SELECT l2.category_id FROM ${case_db}.t_large l2 WHERE l2.id IN (1, 2, 3, 4, 5)
    )
) ORDER BY id;

-- ============================================================
-- Nested CASE with IN
-- ============================================================
-- query 50
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT id FROM ${case_db}.t_large WHERE
    CASE
        WHEN score > 90 THEN category_id
        WHEN score > 80 THEN category_id + 10
        ELSE 0
    END IN (1, 2, 3, 4, 5, 11, 12, 13, 14, 15)
) ORDER BY id;

-- ============================================================
-- CTE + IN
-- ============================================================
-- query 51
-- @order_sensitive=true
WITH filtered_data AS (
    SELECT id, int_col, value FROM ${case_db}.t_int WHERE int_col IN (1000, 2000, 3000, 4000)
)
SELECT * FROM filtered_data ORDER BY id;

-- ============================================================
-- Join with IN filter
-- ============================================================
-- query 52
-- @order_sensitive=true
SELECT t.id, t.int_col, d.dim_name
FROM ${case_db}.t_int t
JOIN ${case_db}.t_dimension d ON t.id = d.dim_id
WHERE t.int_col IN (1000, 2000, 3000, 4000)
ORDER BY t.id;

-- ============================================================
-- IN + additional filter
-- ============================================================
-- query 53
-- @order_sensitive=true
SELECT id, int_col, bigint_col FROM ${case_db}.t_int
WHERE int_col IN (1000, 2000, 3000, 4000) AND bigint_col > 15000
ORDER BY id;

-- ============================================================
-- Window function + IN filter
-- ============================================================
-- query 54
-- @order_sensitive=true
SELECT id, int_col, row_number() OVER (ORDER BY int_col) AS rn
FROM ${case_db}.t_int
WHERE int_col IN (1000, 2000, 3000, 4000)
ORDER BY id;

-- ============================================================
-- UNION ALL with IN
-- ============================================================
-- query 55
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE int_col IN (1000, 2000, 3000, 4000)
UNION ALL
SELECT id, int_col FROM ${case_db}.t_int WHERE int_col IN (5000, 6000, 7000, 8000)
ORDER BY id;

-- ============================================================
-- Aggregate with IN
-- ============================================================
-- query 56
SELECT count(*), sum(int_col), avg(int_col), min(int_col), max(int_col)
FROM ${case_db}.t_int
WHERE int_col IN (1000, 2000, 3000, 4000, 5000, 6000);

-- ============================================================
-- IN + ORDER BY DESC + LIMIT
-- ============================================================
-- query 57
-- @order_sensitive=true
SELECT id, int_col, value FROM ${case_db}.t_int
WHERE int_col IN (1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000)
ORDER BY int_col DESC
LIMIT 5;

-- ============================================================
-- Duplicate values in IN list
-- ============================================================
-- query 58
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE int_col IN (1000, 1000, 2000, 2000, 3000, 3000) ORDER BY id;

-- ============================================================
-- Special characters in IN list
-- ============================================================
-- query 59
-- @order_sensitive=true
SELECT id, varchar_col FROM ${case_db}.t_string WHERE varchar_col IN ('a''b', 'c"d', 'e\f', 'normal') ORDER BY id;

-- ============================================================
-- Empty string in IN list
-- ============================================================
-- query 60
-- @order_sensitive=true
SELECT id, varchar_col FROM ${case_db}.t_string WHERE varchar_col IN ('', 'apple', 'banana', 'carrot') ORDER BY id;

-- ============================================================
-- Large IN list (15 values)
-- ============================================================
-- query 61
-- @order_sensitive=true
SELECT id, category_id FROM ${case_db}.t_large
WHERE category_id IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15)
ORDER BY id;

-- ============================================================
-- Empty result: non-existent values
-- ============================================================
-- query 62
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT id FROM ${case_db}.t_large WHERE category_id IN (999, 998, 997, 996)
) ORDER BY id;

-- ============================================================
-- NULL results from subquery IN
-- ============================================================
-- query 63
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT CASE WHEN score > 100 THEN id ELSE null END FROM ${case_db}.t_large
    WHERE category_id IN (1, 2, 3, 4, 5)
) ORDER BY id;

-- ============================================================
-- Expression subquery with floor + IN
-- ============================================================
-- query 64
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT floor(score / 10) FROM ${case_db}.t_large
    WHERE category_id IN (1, 2, 3, 4, 5) AND score IS NOT NULL
) ORDER BY id;

-- ============================================================
-- Self-join with IN
-- ============================================================
-- query 65
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE id IN (
    SELECT t1.id FROM ${case_db}.t_int t1 JOIN ${case_db}.t_int t2 ON t1.id = t2.id + 1
    WHERE t1.tinyint_col IN (20, 30, 40, 50, 60) AND t2.tinyint_col IN (10, 20, 30, 40, 50)
) ORDER BY id;

-- ============================================================
-- date_val IN
-- ============================================================
-- query 66
-- @order_sensitive=true
SELECT id, date_val FROM ${case_db}.t_mixed WHERE date_val IN (DATE '2024-01-01', DATE '2024-01-02', DATE '2024-01-03', DATE '2024-01-04') ORDER BY id;

-- ============================================================
-- A VARCHAR cannot be compared to numeric IN-list items without a cast.
-- ============================================================
-- query 67
-- @expect_error=[sql.analyze.type_mismatch]
SELECT id, varchar_col FROM ${case_db}.t_string WHERE varchar_col IN (1, 2, 3, 4) ORDER BY id;

-- ============================================================
-- Complex window + IN
-- ============================================================
-- query 68
-- @order_sensitive=true
SELECT
    id,
    category_id,
    status,
    score,
    row_number() OVER (PARTITION BY category_id ORDER BY score DESC) AS rank_in_category
FROM ${case_db}.t_large
WHERE category_id IN (1, 2, 3, 4, 5) AND status IN ('active', 'pending', 'inactive', 'completed')
ORDER BY category_id, rank_in_category;

-- ============================================================
-- Four-table join with multiple IN filters
-- ============================================================
-- query 69
-- @order_sensitive=true
SELECT
    t.id,
    t.int_col,
    s.varchar_col,
    d.dim_name,
    l.status,
    l.score
FROM ${case_db}.t_int t
JOIN ${case_db}.t_string s ON t.id = s.id
JOIN ${case_db}.t_dimension d ON t.id = d.dim_id
JOIN ${case_db}.t_large l ON t.id = l.id
WHERE t.int_col IN (1000, 2000, 3000, 4000, 5000)
  AND s.varchar_col IN ('apple', 'banana', 'carrot', 'dog', 'elephant')
  AND l.status IN ('active', 'pending', 'inactive', 'completed')
ORDER BY t.id;

-- ============================================================
-- NOT IN (without NULL guard)
-- ============================================================
-- query 70
-- @order_sensitive=true
SELECT id, int_col FROM ${case_db}.t_int WHERE int_col NOT IN (1000, 2000, 3000, 4000) ORDER BY id;

-- ============================================================
-- Query cache + IN
-- ============================================================
-- query 71
-- @skip_result_check=true

-- query 72
-- @order_sensitive=true
SELECT dim_id, count(dim_id) FROM ${case_db}.t_dimension WHERE dim_id IN (1,2,3,4,5) GROUP BY dim_id ORDER BY dim_id;

-- query 73
-- @skip_result_check=true

-- query 74
-- @skip_result_check=true
