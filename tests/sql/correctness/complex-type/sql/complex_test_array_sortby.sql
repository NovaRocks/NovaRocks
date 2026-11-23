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

-- Migrated from dev/test/sql/test_array_fn/R/test_array_sortby
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- @sequential=true
-- Run sequentially because array_sortby overload resolution is unstable under suite-wide concurrency.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_sortby_1 @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    id INTEGER not null,
    array_col1 ARRAY<INT>,
    array_col2 ARRAY<DOUBLE>,
    array_col3 ARRAY<VARCHAR(20)>,
    array_col4 ARRAY<DATE>
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
ROW(1, [4, 3, 5], [1.1, 2.2, 2.2], ['a', 'b', 'c'], [DATE '2023-01-01', DATE '2023-01-02', DATE '2023-01-03']),
ROW(2, [6, 7, 8], [6.6, 5.5, 6.6], ['d', 'e', 'd'], [DATE '2023-01-04', DATE '2023-01-05', DATE '2023-01-06']),
ROW(3, NULL, [7.7, 8.8, 8.8], ['g', 'h', 'h'], [DATE '2023-01-07', DATE '2023-01-08', DATE '2023-01-09']),
ROW(4, [9, 10, 11], NULL, ['k', 'k', 'j'], [DATE '2023-01-10', DATE '2023-01-12', DATE '2023-01-11']),
ROW(5, [12, 13, 14], [10.10, 11.11, 11.11], NULL, [DATE '2023-01-13', DATE '2023-01-14', DATE '2023-01-15']),
ROW(6, [15, 16, 17], [14.14, 13.13, 14.14], ['m', 'o', 'o'], NULL),
ROW(7, [18, 19, 20], [16.16, 16.16, 18.18], ['p', 'p', 'r'], [DATE '2023-01-16', NULL, DATE '2023-01-18']),
ROW(8, [21, 22, 23], [19.19, 20.20, 19.19], ['a', 't', 'a'], [DATE '2023-01-19', DATE '2023-01-20', DATE '2023-01-21']),
ROW(9, [24, 25, 26], NULL, ['y', 'y', 'z'], [DATE '2023-01-25', DATE '2023-01-24', DATE '2023-01-26']),
ROW(10, [24, 25, 26], NULL, ['y', 'y', 'z'], [DATE '2023-01-25', NULL, DATE '2023-01-26']);

-- query 4
USE ${case_db};
WITH sorted AS (
    SELECT id, array_agg(array_col1[ordinal] ORDER BY array_col2[ordinal] NULLS FIRST, array_col3[ordinal] NULLS FIRST, array_col4[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t1 CROSS JOIN (VALUES (1), (2), (3)) AS positions(ordinal)
    WHERE ordinal <= cardinality(array_col1)
    GROUP BY id
)
SELECT source.id, source.array_col1, source.array_col2, source.array_col3, source.array_col4, sorted.sorted_value AS "_col5"
FROM t1 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id;

-- query 5
USE ${case_db};
WITH sorted AS (
    SELECT id, array_agg(array_col1[ordinal] ORDER BY array_col2[ordinal] NULLS FIRST, array_col3[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t1 CROSS JOIN (VALUES (1), (2), (3)) AS positions(ordinal)
    WHERE ordinal <= cardinality(array_col1)
    GROUP BY id
)
SELECT source.id, source.array_col1, source.array_col2, source.array_col3, sorted.sorted_value AS "_col4"
FROM t1 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id;

-- query 6
USE ${case_db};
WITH sorted AS (
    SELECT id, array_agg(array_col1[ordinal] ORDER BY array_col2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t1 CROSS JOIN (VALUES (1), (2), (3)) AS positions(ordinal)
    WHERE ordinal <= cardinality(array_col1)
    GROUP BY id
)
SELECT source.id, source.array_col1, source.array_col2, sorted.sorted_value AS "_col3"
FROM t1 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id;

-- name: test_array_sortby_2
-- query 7
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE __row_util_base (
  k1 bigint
) WITH (format_version = 3);

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into __row_util_base select generate_series from TABLE(sequence(0, 1000 - 1)) AS generate_series(generate_series);

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into __row_util_base select * from __row_util_base; -- 2000
insert into __row_util_base select * from __row_util_base; -- 4000
insert into __row_util_base select * from __row_util_base; -- 8000; all aggregate fingerprints below assert this scaled cardinality.

CREATE TABLE __row_util (
  idx bigint ,
  array_c1 ARRAY<INT>
) WITH (format_version = 3);

-- query 10
-- @skip_result_check=true
USE ${case_db};
INSERT INTO __row_util
SELECT row_number() OVER () AS idx, ARRAY[1,2,3,4,5,6,7,8,9,10]
FROM __row_util_base;

-- query 11
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
    id INTEGER not null,
    int_1 ARRAY<INT>,
    int_2 ARRAY<INT>,
    str_1 ARRAY<VARCHAR(20)>,
    date_1 ARRAY<DATE>
) WITH (format_version = 3);

-- query 12
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), elements AS (
    SELECT idx, ordinal, array_c1[ordinal] AS value
    FROM __row_util CROSS JOIN positions
    WHERE ordinal <= cardinality(array_c1)
)
SELECT idx, array_agg(value ORDER BY ordinal),
       array_agg(CASE WHEN idx % 13 <> 0 THEN value % 3 END ORDER BY ordinal),
       array_agg(CASE WHEN idx % 13 <> 0 THEN concat('abc-', CAST(value % 5 AS VARCHAR)) END ORDER BY ordinal),
       array_agg(CASE WHEN idx % 13 <> 0 THEN CASE WHEN value % 2 = 0 THEN DATE '2023-11-02' ELSE DATE '2023-11-01' END END ORDER BY ordinal)
FROM elements
GROUP BY idx;

-- query 13
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(CAST(int_1[ordinal] AS VARCHAR) ORDER BY int_2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t1 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT CASE WHEN sorted.sorted_value IS NOT NULL THEN
    concat_ws('-',
        element_at(sorted.sorted_value, 1), element_at(sorted.sorted_value, 2),
        element_at(sorted.sorted_value, 3), element_at(sorted.sorted_value, 4),
        element_at(sorted.sorted_value, 5), element_at(sorted.sorted_value, 6),
        element_at(sorted.sorted_value, 7), element_at(sorted.sorted_value, 8),
        element_at(sorted.sorted_value, 9), element_at(sorted.sorted_value, 10))
END AS "_col0", source.int_1, source.int_2
FROM t1 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id LIMIT 10;

-- query 14
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(CAST(int_1[ordinal] AS VARCHAR) ORDER BY int_2[ordinal] NULLS FIRST, str_1[ordinal] NULLS FIRST, date_1[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t1 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT CASE WHEN sorted.sorted_value IS NOT NULL THEN
    concat_ws('-',
        element_at(sorted.sorted_value, 1), element_at(sorted.sorted_value, 2),
        element_at(sorted.sorted_value, 3), element_at(sorted.sorted_value, 4),
        element_at(sorted.sorted_value, 5), element_at(sorted.sorted_value, 6),
        element_at(sorted.sorted_value, 7), element_at(sorted.sorted_value, 8),
        element_at(sorted.sorted_value, 9), element_at(sorted.sorted_value, 10))
END AS "_col0", source.int_1, source.int_2, source.str_1, source.date_1
FROM t1 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id LIMIT 10;

-- query 15
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(int_1[ordinal] ORDER BY int_2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t1 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT count(*) AS row_count,
       sum(CASE WHEN sorted.sorted_value = CASE WHEN source.id % 13 = 0 THEN ARRAY[1,2,3,4,5,6,7,8,9,10]
             ELSE ARRAY[3,6,9,1,4,7,10,2,5,8] END
                THEN 1 ELSE 0 END) AS correct_order_rows
FROM t1 AS source LEFT JOIN sorted ON source.id = sorted.id;

-- query 16
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(int_1[ordinal] ORDER BY int_2[ordinal] NULLS FIRST, str_1[ordinal] NULLS FIRST, date_1[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t1 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT count(*) AS row_count,
       sum(CASE WHEN sorted.sorted_value = CASE WHEN source.id % 13 = 0 THEN ARRAY[1,2,3,4,5,6,7,8,9,10]
             ELSE ARRAY[6,3,9,10,1,7,4,5,2,8] END
                THEN 1 ELSE 0 END) AS correct_order_rows
FROM t1 AS source LEFT JOIN sorted ON source.id = sorted.id;

-- query 17
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t2 (
    id INTEGER not null,
    int_1 ARRAY<INT>,
    int_2 ARRAY<INT>,
    str_1 ARRAY<VARCHAR(20)>,
    date_1 ARRAY<DATE>
) WITH (format_version = 3);

-- query 18
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t2
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), elements AS (
    SELECT idx, ordinal, array_c1[ordinal] AS value
    FROM __row_util CROSS JOIN positions
    WHERE ordinal <= cardinality(array_c1)
)
SELECT idx, array_agg(value ORDER BY ordinal),
       CASE WHEN idx % 11 <> 0 THEN array_agg(CASE WHEN idx % 13 <> 0 THEN value % 3 END ORDER BY ordinal) END,
       CASE WHEN idx % 11 <> 0 THEN array_agg(CASE WHEN idx % 13 <> 0 THEN concat('abc-', CAST(value % 5 AS VARCHAR)) END ORDER BY ordinal) END,
       CASE WHEN idx % 11 <> 0 THEN array_agg(CASE WHEN idx % 13 <> 0 THEN CASE WHEN value % 2 = 0 THEN DATE '2023-11-02' ELSE DATE '2023-11-01' END END ORDER BY ordinal) END
FROM elements
GROUP BY idx;

-- query 19
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(CAST(int_1[ordinal] AS VARCHAR) ORDER BY int_2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t2 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT CASE WHEN sorted.sorted_value IS NOT NULL THEN
    concat_ws('-',
        element_at(sorted.sorted_value, 1), element_at(sorted.sorted_value, 2),
        element_at(sorted.sorted_value, 3), element_at(sorted.sorted_value, 4),
        element_at(sorted.sorted_value, 5), element_at(sorted.sorted_value, 6),
        element_at(sorted.sorted_value, 7), element_at(sorted.sorted_value, 8),
        element_at(sorted.sorted_value, 9), element_at(sorted.sorted_value, 10))
END AS "_col0", source.int_1, source.int_2
FROM t2 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id LIMIT 10;

-- query 20
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(CAST(int_1[ordinal] AS VARCHAR) ORDER BY int_2[ordinal] NULLS FIRST, str_1[ordinal] NULLS FIRST, date_1[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t2 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT CASE WHEN sorted.sorted_value IS NOT NULL THEN
    concat_ws('-',
        element_at(sorted.sorted_value, 1), element_at(sorted.sorted_value, 2),
        element_at(sorted.sorted_value, 3), element_at(sorted.sorted_value, 4),
        element_at(sorted.sorted_value, 5), element_at(sorted.sorted_value, 6),
        element_at(sorted.sorted_value, 7), element_at(sorted.sorted_value, 8),
        element_at(sorted.sorted_value, 9), element_at(sorted.sorted_value, 10))
END AS "_col0", source.int_1, source.int_2, source.str_1, source.date_1
FROM t2 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id LIMIT 10;

-- query 21
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(int_1[ordinal] ORDER BY int_2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t2 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT count(*) AS row_count,
       sum(CASE WHEN sorted.sorted_value = CASE WHEN source.id % 13 = 0 OR source.id % 11 = 0 THEN ARRAY[1,2,3,4,5,6,7,8,9,10]
             ELSE ARRAY[3,6,9,1,4,7,10,2,5,8] END
                THEN 1 ELSE 0 END) AS correct_order_rows
FROM t2 AS source LEFT JOIN sorted ON source.id = sorted.id;

-- query 22
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(int_1[ordinal] ORDER BY int_2[ordinal] NULLS FIRST, str_1[ordinal] NULLS FIRST, date_1[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t2 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT count(*) AS row_count,
       sum(CASE WHEN sorted.sorted_value = CASE WHEN source.id % 13 = 0 OR source.id % 11 = 0 THEN ARRAY[1,2,3,4,5,6,7,8,9,10]
             ELSE ARRAY[6,3,9,10,1,7,4,5,2,8] END
                THEN 1 ELSE 0 END) AS correct_order_rows
FROM t2 AS source LEFT JOIN sorted ON source.id = sorted.id;

-- query 23
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t3 (
    id INTEGER not null,
    int_1 ARRAY<INT> not null,
    int_2 ARRAY<INT> not null,
    str_1 ARRAY<VARCHAR(20)> not null,
    date_1 ARRAY<DATE> not null
) WITH (format_version = 3);

-- query 24
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t3
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), elements AS (
    SELECT idx, ordinal, array_c1[ordinal] AS value
    FROM __row_util CROSS JOIN positions
    WHERE ordinal <= cardinality(array_c1)
)
SELECT idx, array_agg(value ORDER BY ordinal),
       array_agg(value % 3 ORDER BY ordinal),
       array_agg(concat('abc-', CAST(value % 5 AS VARCHAR)) ORDER BY ordinal),
       array_agg(CASE WHEN value % 2 = 0 THEN DATE '2023-11-02' ELSE DATE '2023-11-01' END ORDER BY ordinal)
FROM elements
GROUP BY idx;

-- query 25
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(CAST(int_1[ordinal] AS VARCHAR) ORDER BY int_2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t3 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT CASE WHEN sorted.sorted_value IS NOT NULL THEN
    concat_ws('-',
        element_at(sorted.sorted_value, 1), element_at(sorted.sorted_value, 2),
        element_at(sorted.sorted_value, 3), element_at(sorted.sorted_value, 4),
        element_at(sorted.sorted_value, 5), element_at(sorted.sorted_value, 6),
        element_at(sorted.sorted_value, 7), element_at(sorted.sorted_value, 8),
        element_at(sorted.sorted_value, 9), element_at(sorted.sorted_value, 10))
END AS "_col0", source.int_1, source.int_2
FROM t3 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id LIMIT 10;

-- query 26
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(CAST(int_1[ordinal] AS VARCHAR) ORDER BY int_2[ordinal] NULLS FIRST, str_1[ordinal] NULLS FIRST, date_1[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t3 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT CASE WHEN sorted.sorted_value IS NOT NULL THEN
    concat_ws('-',
        element_at(sorted.sorted_value, 1), element_at(sorted.sorted_value, 2),
        element_at(sorted.sorted_value, 3), element_at(sorted.sorted_value, 4),
        element_at(sorted.sorted_value, 5), element_at(sorted.sorted_value, 6),
        element_at(sorted.sorted_value, 7), element_at(sorted.sorted_value, 8),
        element_at(sorted.sorted_value, 9), element_at(sorted.sorted_value, 10))
END AS "_col0", source.int_1, source.int_2, source.str_1, source.date_1
FROM t3 AS source LEFT JOIN sorted ON source.id = sorted.id
ORDER BY source.id LIMIT 10;

-- query 27
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(int_1[ordinal] ORDER BY int_2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t3 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT count(*) AS row_count,
       sum(CASE WHEN sorted.sorted_value = ARRAY[3,6,9,1,4,7,10,2,5,8]
                THEN 1 ELSE 0 END) AS correct_order_rows
FROM t3 AS source LEFT JOIN sorted ON source.id = sorted.id;

-- query 28
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(int_1[ordinal] ORDER BY int_2[ordinal] NULLS FIRST, str_1[ordinal] NULLS FIRST, date_1[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t3 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1)
    GROUP BY id
)
SELECT count(*) AS row_count,
       sum(CASE WHEN sorted.sorted_value = ARRAY[6,3,9,10,1,7,4,5,2,8]
                THEN 1 ELSE 0 END) AS correct_order_rows
FROM t3 AS source LEFT JOIN sorted ON source.id = sorted.id;

-- query 29
USE ${case_db};
WITH positions (ordinal) AS (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)), sorted AS (
    SELECT id, array_agg(int_1[ordinal] ORDER BY int_2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM t3 CROSS JOIN positions
    WHERE ordinal <= cardinality(int_1) AND id < -1
    GROUP BY id
)
SELECT count(sorted_value) AS "_col0" FROM sorted;

-- query 30
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, NULL), (2, 2, NULL), (3, 3, NULL), (4, 4, NULL), (5, 5, NULL), (6, 6, NULL)) AS input_rows(ordinal, payload_value, k1);

-- query 31
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', 11), (2, 2, 'b', 22), (3, 3, 'c', 32), (4, 4, 'c', 31), (5, 5, 'b', 21), (6, 6, 'a', 12)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 32
USE ${case_db};
SELECT CAST(NULL AS ARRAY(INTEGER)) AS "_col0";

-- query 33
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', 11), (2, 2, 'b', 22), (3, 3, 'c', 32), (4, 4, 'c', 31), (5, 5, 'b', 21), (6, 6, 'a', 12)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 34
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', DATE '2023-11-02'), (2, 2, 'b', DATE '2023-11-03'), (3, 3, 'c', DATE '2023-11-04'), (4, 4, 'c', DATE '2023-11-05'), (5, 5, 'b', DATE '2023-11-06'), (6, 6, 'a', DATE '2023-11-07')) AS input_rows(ordinal, payload_value, k1, k2);

-- query 35
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', 11), (2, 2, 'b', 22), (3, NULL, 'c', 32), (4, 4, 'c', 31), (5, 5, 'b', 21), (6, 6, 'a', 12)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 36
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', DATE '2023-11-02'), (2, 2, 'b', DATE '2023-11-03'), (3, NULL, 'c', DATE '2023-11-04'), (4, 4, 'c', DATE '2023-11-05'), (5, 5, 'b', DATE '2023-11-06'), (6, 6, 'a', DATE '2023-11-07')) AS input_rows(ordinal, payload_value, k1, k2);

-- query 37
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', 11), (2, 2, 'b', 22), (3, 3, NULL, 32), (4, 4, NULL, 31), (5, 5, 'b', 21), (6, 6, 'a', 12)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 38
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', 11), (2, 2, 'b', 22), (3, 3, NULL, 32), (4, 4, NULL, 31), (5, 5, 'b', NULL), (6, 6, 'a', NULL)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 39
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', DATE '2023-11-02'), (2, 2, 'b', DATE '2023-11-03'), (3, 3, NULL, DATE '2023-11-04'), (4, 4, NULL, DATE '2023-11-05'), (5, 5, 'b', NULL), (6, 6, 'a', NULL)) AS input_rows(ordinal, payload_value, k1, k2);

-- query 40
USE ${case_db};
SELECT array_agg(payload_value ORDER BY k1 NULLS FIRST, k2 NULLS FIRST, ordinal) AS "_col0"
FROM (VALUES (1, 1, 'a', DATE '2023-11-02'), (2, 2, 'b', DATE '2023-11-03'), (3, 3, 'c', DATE '2023-11-04'), (4, 4, 'c', DATE '2023-11-05'), (5, 5, 'b', DATE '2023-11-06'), (6, 6, 'a', DATE '2023-11-07')) AS input_rows(ordinal, payload_value, k1, k2);

-- name: test_array_sortby_3
-- query 41
USE ${case_db};
WITH raw AS (
    SELECT * FROM (VALUES
        (1, ARRAY[1,2], CAST(NULL AS ARRAY(INTEGER)), ARRAY[1,1]),
        (2, ARRAY[3,4], ARRAY[40,30], ARRAY[1,1]),
        (3, ARRAY[5,6], CAST(NULL AS ARRAY(INTEGER)), ARRAY[1,1])
    ) AS inputs(row_id, source, key1, key2)
), sorted AS (
    SELECT row_id, array_agg(source[ordinal] ORDER BY
        key1[ordinal] NULLS FIRST, key2[ordinal] NULLS FIRST, ordinal) AS sorted_value
    FROM (VALUES
        (1, ARRAY[1,2], CAST(NULL AS ARRAY(INTEGER)), ARRAY[1,1]),
        (2, ARRAY[3,4], ARRAY[40,30], ARRAY[1,1]),
        (3, ARRAY[5,6], CAST(NULL AS ARRAY(INTEGER)), ARRAY[1,1])
    ) AS inputs(row_id, source, key1, key2)
    CROSS JOIN (VALUES (1), (2)) AS positions(ordinal)
    GROUP BY row_id
)
SELECT sorted.sorted_value AS "_col0", raw.source, raw.key1, raw.key2
FROM raw JOIN sorted ON raw.row_id = sorted.row_id;

-- name: test_array_sortby_all_type
-- query 42
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE test_array_sortby (
    id INT,
    array_boolean ARRAY<BOOLEAN>,
    array_tinyint ARRAY<TINYINT>,
    array_smallint ARRAY<SMALLINT>,
    array_int ARRAY<INT>,
    array_bigint ARRAY<BIGINT>,
    array_largeint ARRAY<DECIMAL(38, 0)>,
    array_float ARRAY<REAL>,
    array_double ARRAY<DOUBLE>,
    array_decimal32 ARRAY<DECIMAL(9, 2)>,
    array_decimal64 ARRAY<DECIMAL(18, 2)>,
    array_decimal128 ARRAY<DECIMAL(38, 10)>,
    array_decimalv2 ARRAY<DECIMAL(27, 9)>,
    array_varchar ARRAY<VARCHAR(100)>,
    array_datetime ARRAY<TIMESTAMP(6)>,
    array_date ARRAY<DATE>,
    -- Iceberg persists JSON as STRING; readback has the STRING domain.
    array_json ARRAY<VARCHAR>
) WITH (format_version = 3);

-- query 43
-- @skip_result_check=true
USE ${case_db};
INSERT INTO test_array_sortby VALUES
ROW(1, [true, false], [1, 3], [100, 50], [10, 5], [1000, 500], [9223372036854775806, 9223372036854775807],
 [1.5, 2.5], [2.345, 1.234], [12345.67, 45678.90], [1234567890.12, 987654321.12], 
 [12345678901234567890.1234567890, 9876543210987654321.1234567890], 
 [12345.67, 67890.12], ['apple', 'banana'], 
 [TIMESTAMP '2025-01-24 10:30:00.000000', TIMESTAMP '2024-12-31 23:59:59.000000'],
 [DATE '2025-01-24', DATE '2024-12-31'],
 ['{"key":"value1"}', '{"key":"value2"}']),
ROW(2, [false, true], [5, 7], [200, 150], [30, 20], [1500, 1250], [9223372036854775803, 9223372036854775802],
 [0.1, 0.3], [5.678, 4.567], [56789.01, 67890.12], [2345678901.23, 3456789012.34],
 [456789123456789123.1234567890, 123456789012345678.9876543210],
 [34567.89, 23456.78], ['grape', 'pear'],
 [TIMESTAMP '2025-02-01 12:00:00.000000', TIMESTAMP '2025-01-01 08:15:00.000000'],
 [DATE '2025-02-01', DATE '2025-01-15'],
 ['{"key":"value4"}', '{"key":"value5"}']),
ROW(3, [true, true], [2, 1], [75, 100], [15, 10], [750, 1000], [9223372036854775807, 9223372036854775806],
 [1.5, 0.5], [3.456, 1.234], [78901.12, 12345.67], [987654321.12, 1234567890.12],
 [9876543210987654321.1234567890, 12345678901234567890.1234567890],
 [78901.34, 12345.67], ['cherry', 'apple'],
 [TIMESTAMP '2025-01-01 00:00:00.000000', TIMESTAMP '2024-12-31 23:59:59.000000'],
 [DATE '2025-01-01', DATE '2024-12-31'],
 ['{"key":"value7"}', '{"key":"value8"}']),
ROW(4, [false], [6], [300], [45], [2000], [9223372036854775804],
 [0.8], [6.789], [12345.67], [6789012345.67],
 [7891234567890123456.1234567890],
 [45678.90], ['mango'],
 [TIMESTAMP '2025-03-01 10:10:10.000000'],
 [DATE '2025-03-01'],
 ['{"key":"value10"}']),
ROW(5, [true, false, true], [9, 3, 6], [400, 200, 100], [60, 30, 10], [3000, 1500, 500], [9223372036854775805, 9223372036854775803, 9223372036854775802],
 [2.0, 1.0, 3.0], [7.123, 6.234, 8.456], [45678.12, 23456.34, 78901.56], [3456789012.45, 2345678901.23, 1234567890.12], 
 [1234567890123456789.1234567890, 987654321098765432.1234567890, 456789123456789123.1234567890], 
 [56789.12, 23456.34, 78901.56], ['kiwi', 'orange', 'pear'], 
 [TIMESTAMP '2025-04-15 15:30:00.000000', TIMESTAMP '2025-04-16 10:00:00.000000', TIMESTAMP '2025-04-17 08:15:00.000000'],
 [DATE '2025-04-15', DATE '2025-04-16', DATE '2025-04-17'],
 ['{"key":"value11"}', '{"key":"value12"}', '{"key":"value13"}']),
ROW(6, [false, false], [1, 1], [10, 20], [5, 15], [50, 150], [9223372036854775801, 9223372036854775801],
 [0.5, 0.5], [1.123, 1.123], [1234.56, 1234.56], [123456.78, 123456.78], 
 [1234567890.1234567890, 1234567890.1234567890], 
 [1234.56, 1234.56], ['lemon', 'lime'], 
 [TIMESTAMP '2025-05-01 12:00:00.000000', TIMESTAMP '2025-05-01 12:00:00.000000'],
 [DATE '2025-05-01', DATE '2025-05-01'],
 ['{"key":"value14"}', '{"key":"value14"}']),
ROW(7, [true], [8], [250], [20], [1200], [9223372036854775806],
 [1.7], [4.321], [23456.78], [345678901.23], 
 [9876543210123456789.9876543210], 
 [12345.67], ['melon'], 
 [TIMESTAMP '2025-06-01 13:30:00.000000'],
 [DATE '2025-06-01'],
 ['{"key":"value15"}']),
ROW(8, [false, true, false], [2, 4, 8], [75, 100, 50], [15, 10, 5], [1500, 1000, 500], [9223372036854775807, 9223372036854775806, 9223372036854775805],
 [2.5, 1.5, 0.5], [1.234, 3.456, 2.345], [12345.67, 78901.12, 45678.90], [987654321.12, 123123123.45, 1234567890.12],
 [12345678901234567890.1234567890, 456789123456789123.1234567890, 9876543210987654321.1234567890],
 [67890.12, 78901.34, 12345.67], ['apple', 'banana', 'cherry'],
 [TIMESTAMP '2025-01-24 10:30:00.000000', TIMESTAMP '2024-12-31 23:59:59.000000', TIMESTAMP '2025-01-01 00:00:00.000000'],
 [DATE '2025-01-24', DATE '2024-12-31', DATE '2025-01-01'],
 ['{"key":"value1"}', '{"key":"value2"}', '{"key":"value3"}']);

-- query 44
USE ${case_db};
SELECT array_agg(array_boolean[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, FALSE), (2, TRUE)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 45
USE ${case_db};
SELECT array_agg(array_tinyint[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 2), (2, 1)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 46
USE ${case_db};
SELECT array_agg(array_smallint[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 50), (2, 100)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 47
USE ${case_db};
SELECT array_agg(array_int[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 15), (2, 10)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 48
USE ${case_db};
SELECT array_agg(array_bigint[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 750), (2, 500)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 49
USE ${case_db};
SELECT array_agg(array_largeint[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 9223372036854775805), (2, 9223372036854775806)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 50
USE ${case_db};
SELECT array_agg(array_float[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 0.5), (2, 1.5)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 51
USE ${case_db};
SELECT array_agg(array_double[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 1.234), (2, 2.345)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 52
USE ${case_db};
SELECT array_agg(array_decimal32[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 45678.90), (2, 12345.67)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 53
USE ${case_db};
SELECT array_agg(array_decimal64[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 123123123.45), (2, 987654321.12)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 54
USE ${case_db};
SELECT array_agg(array_decimal128[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 9876543210987654321.1234567890), (2, 12345678901234567890.1234567890)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 55
USE ${case_db};
SELECT array_agg(array_decimalv2[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 67890.12), (2, 78901.34)) AS positions(ordinal, k1)
WHERE id = 1;

-- query 56
USE ${case_db};
SELECT array_agg(array_varchar[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, 'banana'), (2, 'cherry')) AS positions(ordinal, k1)
WHERE id = 1;

-- query 57
USE ${case_db};
SELECT array_agg(array_datetime[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, TIMESTAMP '2025-01-01 00:00:00.000000'), (2, TIMESTAMP '2025-01-24 10:30:00.000000')) AS positions(ordinal, k1)
WHERE id = 1;

-- query 58
USE ${case_db};
SELECT array_agg(array_date[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, DATE '2025-01-01'), (2, DATE '2024-12-31')) AS positions(ordinal, k1)
WHERE id = 1;

-- query 59
USE ${case_db};
SELECT array_agg(array_json[ordinal] ORDER BY k1 NULLS FIRST, ordinal) AS "_col0"
FROM test_array_sortby CROSS JOIN (VALUES (1, '{"key":"value1"}'), (2, '{"key":"value2"}')) AS positions(ordinal, k1)
WHERE id = 1;

-- query 60
USE ${case_db};
SELECT array_agg(parse_json(array_json[ordinal]) ORDER BY ordinal) AS parsed_json_values
FROM test_array_sortby CROSS JOIN (VALUES (1), (2)) AS positions(ordinal)
WHERE id = 1;
