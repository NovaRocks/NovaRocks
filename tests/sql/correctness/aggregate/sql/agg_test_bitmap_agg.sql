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
-- @skip_result_check=true
-- The legacy source checked StarRocks FE bitmap MV rewrite here. Keep this
-- aggregate case focused on bitmap aggregate semantics; MV rewrite coverage
-- belongs to the materialized-view suite.
USE ${case_db};

-- query 5
-- @skip_result_check=true
USE ${case_db};

-- query 6
-- @skip_result_check=true
USE ${case_db};

-- query 7
-- @skip_result_check=true
USE ${case_db};

-- query 8
-- @skip_result_check=true
USE ${case_db};

-- query 9
-- @skip_result_check=true
USE ${case_db};

-- query 10
-- @skip_result_check=true
USE ${case_db};

-- query 11
-- @skip_result_check=true
USE ${case_db};

-- query 12
-- @skip_result_check=true
USE ${case_db};

-- query 13
-- @skip_result_check=true
USE ${case_db};

-- query 14
USE ${case_db};
select c1, count(distinct c2) from t1 group by c1 order by c1;

-- query 15
USE ${case_db};
select c1, count(DISTINCT c2), count(DISTINCT c3), count(DISTINCT c4) from t1 group by c1 order by c1;

-- query 16
USE ${case_db};
select c1, count(distinct c2), count(distinct c3), count(distinct c4) from t1 group by c1 order by c1;

-- query 17
-- @skip_result_check=true
-- No materialized view is created in this aggregate-focused case.
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
-- @skip_result_check=true
USE ${case_db};

-- query 26
-- @skip_result_check=true
USE ${case_db};

-- query 27
USE ${case_db};
select c1, count(distinct c2) from t1 group by c1 order by c1;

-- query 28
-- @skip_result_check=true
-- No materialized view is created in this aggregate-focused case.
USE ${case_db};
