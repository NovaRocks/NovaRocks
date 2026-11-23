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

-- Migrated from dev/test/sql/test_array_agg_over_window/R/test_array_agg_over_window
-- Test Objective:
-- Preserve array test coverage migrated from dev/test.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_array_agg_over_window
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t0" (
  "v1" INTEGER,
  "v2" INTEGER,
  "v3" BIGINT,
  "v4" varchar(50),
  "v5" decimal(10,2),
  "v6" REAL
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 select i/19 as v1, i/11 as v2, i/5 as v3, concat('item_', cast(i as varchar)) as v4, i/3.14 as v5, i/2.71 as v6 from table(sequence(1,100)) t(i);

-- query 4
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over() as arr_basic,
  array_agg(distinct v2) over() as arr_distinct,
  array_agg(v3 ORDER BY v2) over() as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 5
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1) as arr_basic,
  array_agg(distinct v2) over(partition by v1) as arr_distinct,
  array_agg(v3 ORDER BY v2) over(partition by v1) as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 6
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 7
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(order by v2) as arr_basic,
  array_agg(distinct v2) over(order by v2) as arr_distinct,
  array_agg(v3 ORDER BY v2) over(order by v2) as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 8
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over() as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 9
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v2) over() as arr_distinct
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 10
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v2) over() as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 11
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1) as arr_basic,
  array_agg(distinct v2) over(partition by v1) as arr_distinct
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 12
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 13
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 14
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 15
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(order by v2) as arr_basic,
  array_agg(distinct v2) over(order by v2) as arr_distinct,
  array_agg(v3 ORDER BY v2) over(order by v2) as arr_order_by
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 16
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_int
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_int, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 17
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v5,
  array_agg(v5) over(partition by v1 order by v2) as arr_decimal
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_decimal, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 18
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v1) over(partition by v1 order by v2) as arr_distinct_int
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct_int, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 19
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v5,
  array_agg(distinct v5) over(partition by v1 order by v2) as arr_distinct_decimal
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct_decimal, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 20
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by_int
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by_int, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 21
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v5,
  array_agg(v5 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by_decimal
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by_decimal, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 22
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1 + v2) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 23
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1 * v2) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 24
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(cast(v3 as decimal(10,2))) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 25
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(length(v4)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 26
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(round(v6, 2)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 27
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(coalesce(v5, 0.0)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 28
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(sqrt(abs(v6))) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 29
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(pow(v6, 2)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 30
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(CAST(year(date_add('day', v3, DATE '2025-01-01')) AS INTEGER)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 31
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(CAST(month(date_add('day', v3, DATE '2025-01-01')) AS INTEGER)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 32
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(nullif(v1, 1)) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 33
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_basic,
  array_agg(v1) over(partition by v1 order by v2 range between unbounded preceding and unbounded following) as arr_complex,
  array_agg(distinct nullif(v2, 2)) over(partition by v1 order by v2) as arr_with_null
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_complex, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_with_null, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 34
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_basic,
  array_agg(v1) over(partition by v1 order by v2 range between current row and unbounded following) as arr_complex,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_with_null
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_complex, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_with_null, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 35
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_large,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_large2,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_large3
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_large, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 36
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1 + v2) over(partition by v1 order by v2) as arr_large,
  array_agg(cast(v3 as decimal(19,2))) over(partition by v1 order by v2) as arr_large2,
  array_agg(length(v4)) over(partition by v1 order by v2) as arr_large3
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_large, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 37
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(0) over(partition by v1 order by v2) as arr_basic,
  array_agg('constant') over(partition by v1 order by v2) as arr_basic2,
  array_agg(3.14159) over(partition by v1 order by v2) as arr_basic3
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 38
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  row_number() over(partition by v1 order by v2) as rn,
  rank() over(partition by v1 order by v2) as rank
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 39
-- @skip_result_check=true
-- Original StarRocks test asserted a parse error here because StarRocks
-- reserves the identifier `lag`. NovaRocks does not reserve it; renaming
-- the alias keeps the array_agg coverage and verifies the engine accepts
-- the multi-window-function SELECT.
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_basic,
  dense_rank() over(partition by v1 order by v2) as d_rank,
  lag(v1, 1) over(partition by v1 order by v2) as lag_val
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 40
-- @skip_result_check=true
-- Same StarRocks parser quirk as query 49 but for `lead`.
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_basic,
  lead(v1, 1) over(partition by v1 order by v2) as lead_val,
  sum(v1) over(partition by v1 order by v2) as sum_window
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 41
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1 order by v2) as arr_basic,
  avg(v1) over(partition by v1 order by v2) as avg_window,
  count(v1) over(partition by v1 order by v2) as count_window
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 42
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(123456789012345678901234567890.123456789) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 43
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(-DECIMAL '9876543210.987654321') over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 44
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over(partition by v1, v2 order by v3) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 45
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v1) over(partition by v1, v2 order by v3) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 46
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1 ORDER BY v2, v3) over(partition by v1 order by v2) as arr_basic
  from t0
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 47
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "t1" (
  "v1" INTEGER,
  "v2" INTEGER,
  "v3" BIGINT,
  "v4" varchar(50),
  "v5" decimal(10,2),
  "v6" REAL
) WITH (format_version = 3);

-- query 48
-- @skip_result_check=true
USE ${case_db};
insert into t1 values
  ROW(1, 1, 100, 'a', 1.1, 1.1),
  ROW(1, 2, NULL, 'b', 2.2, NULL),
  ROW(1, NULL, 300, NULL, 3.3, 3.3),
  ROW(NULL, 4, 400, 'd', NULL, 4.4),
  ROW(2, 1, 500, 'e', 5.5, 5.5),
  ROW(2, NULL, NULL, NULL, NULL, NULL),
  ROW(2, 3, 700, 'g', 7.7, 7.7),
  ROW(NULL, NULL, NULL, NULL, NULL, NULL),
  ROW(3, 1, 900, 'i', 9.9, 9.9),
  ROW(3, 2, 1000, 'j', 10.1, 10.1);

-- query 49
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1) over() as arr_basic
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 50
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v1) over() as arr_distinct
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 51
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v2) over() as arr_order_by
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 52
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v5,
  array_agg(v5) over() as arr_decimal
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_decimal, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 53
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v2) over(partition by v1) as arr_basic
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 54
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v2) over(partition by v1) as arr_distinct
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 55
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v2) over(partition by v1) as arr_order_by
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 56
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 57
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v3) over(partition by v1 order by v2) as arr_distinct
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 58
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 59
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2 range between unbounded preceding and unbounded following) as arr_frame
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_frame, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 60
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic,
  row_number() over(partition by v1 order by v2) as rn,
  sum(v3) over(partition by v1 order by v2) as sum_val
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 61
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v1 + v2 + v3) over(partition by v1 order by v2) as arr_expr
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_expr, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 62
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1, v2 order by v3) as arr_basic
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 63
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3) over() as arr_basic
  from t1
  where v1 = 1 and v2 = 2
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 64
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(coalesce(v3, -1)) over(partition by v1 order by v2) as arr_coalesce
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_coalesce, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 65
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v2) over(order by v1, v2) as arr_basic
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 66
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v3) over(partition by v1) as arr_order_by
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 67
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3 ORDER BY v2, v3) over(partition by v1) as arr_order_by
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 68
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic
  from t1
  where v1 is null
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 69
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(distinct v5) over(partition by v1) as arr_distinct
  from t1
  where v1 = 2
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 70
USE ${case_db};
SELECT sum(fingerprint_value) AS fingerprint FROM (
  select v1, v2, v3,
  array_agg(v3) over(partition by v1 order by v2) as arr_basic,
  array_agg(distinct v2) over(partition by v1 order by v2) as arr_distinct,
  array_agg(v3 ORDER BY v2) over(partition by v1 order by v2) as arr_order_by
  from t1
) as t
CROSS JOIN UNNEST(concat(
    coalesce(transform(arr_basic, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_distinct, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    coalesce(transform(arr_order_by, x -> crc32(to_utf8(CAST(coalesce(x, 0) AS VARCHAR)))), CAST(ARRAY[] AS ARRAY<BIGINT>)),
    ARRAY[CAST(0 AS BIGINT)]
)) AS fingerprints(fingerprint_value);

-- query 71
-- @skip_result_check=true
