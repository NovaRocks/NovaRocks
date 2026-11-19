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

-- Migrated from dev/test/sql/test_agg_function/R/test_bitmap_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_bitmap_agg
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    c1 int,
    c2 boolean,
    c3 tinyint,
    c4 int,
    c5 bigint,
    c6 largeint,
    c7 string
    ) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 values
    ROW(1, true, 11, 111, 1111, 11111, '111111'),
    ROW(2, false, 22, 222, 2222, 22222, '222222'),
    ROW(3, true, 33, 333, 3333, 33333, '333333'),
    ROW(4, null, null, null, null, null, null),
    ROW(5, -1, -11, -111, -1111, -11111, '-111111'),
    ROW(6, null, null, null, null, '36893488147419103232', '680564733841876926926749214863536422912');

-- query 4
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_AGG(c2)) FROM t1;

-- query 5
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_AGG(c3)) FROM t1;

-- query 6
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_AGG(c4)) FROM t1;

-- query 7
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_AGG(c5)) FROM t1;

-- query 8
-- @expect_error=bitmap aggregate expects
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_AGG(c6)) FROM t1;

-- query 9
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_AGG(c7)) FROM t1;

-- query 10
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 11
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c3))) FROM t1;

-- query 12
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c4))) FROM t1;

-- query 13
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c5))) FROM t1;

-- query 14
-- The scalar overload is rejected during analysis, before aggregate execution.
-- @expect_error_tier=drift
-- @expect_sql_code=sql.analyze.type_mismatch
-- @expect_sql_phase=Analyze
-- @expect_error=no matching declared overload
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c6))) FROM t1;

-- query 15
-- @skip_result_check=true
-- The legacy source checked StarRocks FE bitmap MV rewrite here. Keep this
-- aggregate case focused on bitmap aggregate semantics; MV rewrite coverage
-- belongs to the materialized-view suite.
USE ${case_db};

-- query 16
-- @skip_result_check=true
USE ${case_db};

-- query 17
-- @skip_result_check=true
USE ${case_db};

-- query 18
-- @skip_result_check=true
USE ${case_db};

-- query 19
-- @skip_result_check=true
USE ${case_db};

-- query 20
-- @skip_result_check=true
USE ${case_db};

-- query 21
-- @skip_result_check=true
USE ${case_db};

-- query 22
-- @skip_result_check=true
USE ${case_db};

-- query 23
-- @skip_result_check=true
USE ${case_db};

-- query 24
-- @skip_result_check=true
USE ${case_db};

-- query 25
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 26
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c3))) FROM t1;

-- query 27
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c4))) FROM t1;

-- query 28
USE ${case_db};
select c1, BITMAP_TO_STRING(bitmap_union(to_bitmap(c2))), BITMAP_TO_STRING(bitmap_union(to_bitmap(c3))), BITMAP_TO_STRING(bitmap_agg(c4)) from t1 group by c1 order by c1;

-- query 29
USE ${case_db};
select c1, count(distinct c2), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1 order by c1;

-- query 30
USE ${case_db};
select c1, bitmap_union_count(to_bitmap(c4)), BITMAP_TO_STRING(bitmap_agg(c4)) from t1 group by c1 order by c1;

-- query 31
USE ${case_db};
select c1, multi_distinct_count(c2), multi_distinct_count(c3), multi_distinct_count(c4) from t1 group by c1 order by c1;

-- query 32
USE ${case_db};
select c1, count(distinct c2), count(distinct c3), count(distinct c4) from t1 group by c1 order by c1;

-- query 33
-- @skip_result_check=true
-- No materialized view is created in this aggregate-focused case.
USE ${case_db};

-- query 34
-- @skip_result_check=true
USE ${case_db};

-- query 35
-- @skip_result_check=true
USE ${case_db};

-- query 36
-- @skip_result_check=true
USE ${case_db};

-- query 37
-- @skip_result_check=true
USE ${case_db};

-- query 38
-- @skip_result_check=true
USE ${case_db};

-- query 39
-- @skip_result_check=true
USE ${case_db};

-- query 40
-- @skip_result_check=true
USE ${case_db};

-- query 41
-- @skip_result_check=true
USE ${case_db};

-- query 42
-- @skip_result_check=true
USE ${case_db};

-- query 43
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c2))) FROM t1;

-- query 44
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c3))) FROM t1;

-- query 45
USE ${case_db};
SELECT BITMAP_TO_STRING(BITMAP_UNION(TO_BITMAP(c4))) FROM t1;

-- query 46
USE ${case_db};
select ifnull(nullif(BITMAP_TO_STRING(bitmap_union(to_bitmap(c2))), ''), 'NULL'),
       ifnull(nullif(BITMAP_TO_STRING(bitmap_union(to_bitmap(c3))), ''), 'NULL'),
       ifnull(nullif(BITMAP_TO_STRING(bitmap_agg(c4)), ''), 'NULL')
from t1
group by c1
order by c1;

-- query 47
USE ${case_db};
select c1, count(distinct c2), bitmap_union(to_bitmap(c3)), bitmap_agg(c4) from t1 group by c1 order by c1;

-- query 48
USE ${case_db};
select c1, bitmap_union_count(to_bitmap(c4)), BITMAP_TO_STRING(bitmap_agg(c4)) from t1 group by c1 order by c1;

-- query 49
-- @skip_result_check=true
-- No materialized view is created in this aggregate-focused case.
USE ${case_db};
