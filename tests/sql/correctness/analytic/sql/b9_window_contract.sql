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

-- @order_sensitive=true

-- query 1
SELECT seq,
       array_join(array_agg(v ORDER BY k) OVER growing, ',') AS ordered_values,
       array_join(array_agg(DISTINCT v) OVER growing, ',') AS distinct_values,
       array_join(array_agg(DISTINCT v ORDER BY v) OVER growing, ',') AS distinct_ordered,
       listagg(v, '|') WITHIN GROUP (ORDER BY k) OVER growing AS ordered_text,
       listagg(DISTINCT v, '|') WITHIN GROUP (ORDER BY v) OVER growing AS distinct_ordered_text
FROM (VALUES (1, 'b', 2), (2, 'b', 1), (3, 'a', 3)) t(seq, v, k)
WINDOW growing AS (ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
ORDER BY seq;

-- query 2
-- @expect_trino_error=EXPRESSION_NOT_IN_DISTINCT,USER_ERROR
SELECT array_agg(DISTINCT v ORDER BY k, seq) OVER (
    ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (VALUES (1, 'b', 2), (2, 'b', 1), (3, 'a', 1)) t(seq, v, k);

-- query 3
-- @expect_trino_error=EXPRESSION_NOT_IN_DISTINCT,USER_ERROR
SELECT listagg(DISTINCT v, ',') WITHIN GROUP (ORDER BY k, seq) OVER (
    ORDER BY seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM (VALUES (1, 'b', 2), (2, 'b', 1), (3, 'a', 1)) t(seq, v, k);

-- query 4
SELECT seq, count_if(p) OVER w AS n, bool_and(p) OVER w AS both_values,
       every(p) OVER w AS every_value, var_pop(x) OVER w AS variance,
       stddev_pop(x) OVER w AS deviation
FROM (VALUES (1, true, DOUBLE '4'), (2, NULL, DOUBLE '2'),
             (3, false, DOUBLE '8')) t(seq, p, x)
WINDOW w AS (ORDER BY seq ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING)
ORDER BY seq;

-- query 5
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT count_if(DISTINCT p) OVER (ORDER BY seq)
FROM (VALUES (1, true), (2, false)) t(seq, p);

-- query 6
WITH collected AS (
    SELECT seq, array_agg(DISTINCT m) OVER (
        ORDER BY seq ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS representatives
    FROM (VALUES (1, MAP(ARRAY[2, 1], ARRAY[20, 10])),
                 (2, MAP(ARRAY[1, 2], ARRAY[10, 20])),
                 (3, MAP(ARRAY[1, 2], ARRAY[10, 20]))) t(seq, m)
)
SELECT seq, cardinality(representatives) AS count_of_keys,
       map_entries(representatives[1])[1][1] AS first_key
FROM collected
ORDER BY seq;

-- query 7
WITH collected AS (
    SELECT seq, j, array_agg(j) OVER (ORDER BY seq ROWS UNBOUNDED PRECEDING) AS items
    FROM (VALUES (1, JSON '{"a":1,"b":2}'), (2, JSON '{"b":2,"a":1}')) t(seq, j)
)
SELECT seq, j IN (JSON '{"a":1,"b":2}') AS same_value,
       json_format(items[seq]) AS observed_member_order
FROM collected
ORDER BY seq;

-- query 8
-- @expect_trino_error=NOT_SUPPORTED,USER_ERROR
SELECT array_agg(DISTINCT j) OVER (ORDER BY seq)
FROM (VALUES (1, JSON '{"a":1,"b":2}'), (2, JSON '{"b":2,"a":1}')) t(seq, j);

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.b9_mixed (
    part_id INTEGER, seq INTEGER, v VARCHAR, sort_key INTEGER, a ARRAY(INTEGER)
) WITH (format_version = 3, partitioning = ARRAY['part_id']);
INSERT INTO ${case_db}.b9_mixed
SELECT p, seq, v, k, ARRAY[seq]
FROM (VALUES 1, 2, 3, 4, 5, 6) partitions(p)
CROSS JOIN (VALUES (1, 'b', 2), (2, 'a', 1), (3, 'c', 3)) payload(seq, v, k);

-- query 10
-- @explain_contains=WINDOW
-- @be_log_be_count_at_least=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK,2
WITH observed AS (
    SELECT part_id, seq, min_by(v, seq) OVER w AS lowest,
           lag(a, 1, ARRAY[-seq]) OVER w AS prior,
           array_join(array_agg(v ORDER BY sort_key) OVER w, ',') AS items
    FROM ${case_db}.b9_mixed
    WINDOW w AS (PARTITION BY part_id ORDER BY seq)
)
SELECT count(*) AS num_rows, count(DISTINCT part_id) AS partitions,
       bool_and(lowest = 'b' AND prior[1] = IF(seq = 1, -1, seq - 1)
           AND items = CASE seq WHEN 1 THEN 'b' WHEN 2 THEN 'a,b' ELSE 'a,b,c' END) AS verified
FROM observed;

-- query 11
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT lag(ARRAY[seq], 0, ARRAY[CAST(value AS INTEGER)]) OVER (ORDER BY seq)
FROM (VALUES (1, '1'), (2, 'invalid')) t(seq, value);

-- query 12
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.b9_mixed;
