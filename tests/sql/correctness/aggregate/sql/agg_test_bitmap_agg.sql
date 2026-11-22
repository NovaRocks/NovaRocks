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
-- Preserve COUNT(DISTINCT) over nullable inputs and over multiple selected columns.
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
    c7 VARCHAR
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
select c1, count(distinct c2) from t1 group by c1 order by c1;

-- query 5
USE ${case_db};
select c1, count(DISTINCT c2), count(DISTINCT c3), count(DISTINCT c4) from t1 group by c1 order by c1;

-- query 6
USE ${case_db};
select c1, count(distinct c2), count(distinct c3), count(distinct c4) from t1 group by c1 order by c1;

-- query 7
USE ${case_db};
select c1, count(distinct c2) from t1 group by c1 order by c1;
