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

-- Migrated from dev/test/sql/test_agg_function/R/test_mann_whitney
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: testMannWhitney
-- query 2
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'two-sided')
from
    TABLE(generate_series(1, 10)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 3
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'less')
from
    TABLE(generate_series(1, 10)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 4
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'greater')
from
    TABLE(generate_series(1, 10)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 5
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'two-sided')
from
    TABLE(generate_series(1000, 10000)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 6
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'less')
from
    TABLE(generate_series(1000, 10000)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 7
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'greater')
from
    TABLE(generate_series(1000, 10000)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- name: testMannWhitney
-- query 8
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'two-sided', 0)
from
    TABLE(generate_series(1, 10)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 9
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'less', 0)
from
    TABLE(generate_series(1, 10)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 10
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'greater', 0)
from
    TABLE(generate_series(1, 10)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 11
USE ${case_db};
select
    mann_whitney_u_test(x, t, 'two-sided', 0)
from
    TABLE(generate_series(1000, 10000)) as numbers(x), TABLE(generate_series(0, 1)) as idx(t);

-- query 12
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

-- query 13
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 values
    ROW(1, true, 11, 111, 1111, 11111, '111111'),
    ROW(2, false, 22, 222, 2222, 22222, '222222'),
    ROW(3, true, 33, 333, 3333, 33333, '333333'),
    ROW(4, null, null, null, null, null, null),
    ROW(5, -1, -11, -111, -1111, -11111, '-111111'),
    ROW(6, null, null, null, null, '36893488147419103232', '680564733841876926926749214863536422912');

-- query 14
USE ${case_db};
select
    mann_whitney_u_test(c3, c2, 'two-sided')
from t1;

-- query 15
USE ${case_db};
select
    mann_whitney_u_test(c3, c2, 'greater')
from t1;

-- query 16
USE ${case_db};
select
    mann_whitney_u_test(c3, c2, 'less')
from t1;

-- query 17
USE ${case_db};
select
    mann_whitney_u_test(c4, c2, 'two-sided')
from t1;

-- query 18
USE ${case_db};
select
    mann_whitney_u_test(c4, c2, 'greater')
from t1;

-- query 19
USE ${case_db};
select
    mann_whitney_u_test(c4, c2, 'less')
from t1;

-- query 20
USE ${case_db};
select
    mann_whitney_u_test(c5, c2, 'two-sided')
from t1;

-- query 21
USE ${case_db};
select
    mann_whitney_u_test(c5, c2, 'greater')
from t1;

-- query 22
USE ${case_db};
select
    mann_whitney_u_test(c5, c2, 'less')
from t1;

-- query 23
USE ${case_db};
select
    mann_whitney_u_test(c6, c2, 'two-sided')
from t1;

-- query 24
USE ${case_db};
select
    mann_whitney_u_test(c6, c2, 'greater')
from t1;

-- query 25
USE ${case_db};
select
    mann_whitney_u_test(c6, c2, 'less')
from t1;

-- query 26
USE ${case_db};
select
    mann_whitney_u_test(c6, c2, 'less')
from t1
where c1 > 100;

-- query 27
USE ${case_db};
select
    c1 % 3, mann_whitney_u_test(c6, c2, 'less')
from t1
group by c1 % 3;

-- query 28
USE ${case_db};
select
    c1 % 2, mann_whitney_u_test(c6, c2, 'less')
from t1
where c1 > 100
group by c1 % 2;

-- query 29
USE ${case_db};
SELECT mann_whitney_u_test(col1, col2) FROM (VALUES ROW(1, false)) AS tmp(col1, col2);

-- query 30
USE ${case_db};
SELECT mann_whitney_u_test(col1, col2) FROM (VALUES ROW(1, true),ROW(1, false),ROW(2, true),ROW(2, false),ROW(3, false),ROW(3, null)) AS tmp(col1, col2);

-- query 31
USE ${case_db};
SELECT mann_whitney_u_test(1, 1);
