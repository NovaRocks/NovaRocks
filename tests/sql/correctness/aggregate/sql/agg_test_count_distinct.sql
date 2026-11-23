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

-- Migrated from dev/test/sql/test_agg_function/R/test_count_distinct
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_count_distinct
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE "test_cc" (
  "v1" varchar(65533),
  "v2" varchar(65533),
  "v3" TIMESTAMP(6),
  "v4" int ,
  "v5" decimal(32, 2) ,
  "v6" array<int> ,
  "v7" ROW(a BIGINT, b char(20))
) WITH (format_version = 3);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('a','a', '2022-04-18 01:01:00', 1, 1.2,  [1, 2, 3], row(1, 'a'));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('a','b', '2022-04-18 02:01:00', 2, 1.3,  [2, 1, 3], row(2, 'a'));

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('a','a', '2022-04-18 02:05:00', 1, 2.3,  [2, 2, 3], row(3, 'a'));

-- query 6
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('a','b', '2022-04-18 02:15:00', 3, 3.31,  [2, 2, 3], row(4, 'a'));

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('a','b', '2022-04-18 03:15:00', 1, 100.3,  [3, 1, 3], row(2, 'a'));

-- query 8
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('c','a', '2022-04-18 03:45:00', 1, 200.3,  [2, 2, 3], row(3, 'a'));

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('c','a', '2022-04-18 03:25:00', 2, 300.3,  null, row(2, 'a'));

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into test_cc values ROW('c','a', '2022-04-18 03:27:00', 3, 400.3,  [3, 1, 3], null);

-- query 11
USE ${case_db};
select v2, count(1), count(distinct v1) from test_cc group by v2 order by v2;

-- query 12
USE ${case_db};
select v2, count(distinct v1) AS _col2 from test_cc group by v2 order by v2;

-- query 13
USE ${case_db};
select v2, count(distinct v1) AS _col2 from test_cc group by v2 order by v2;

-- query 14
USE ${case_db};
select count(distinct 1, 2, 3, 4) from test_cc;

-- query 15
USE ${case_db};
select count(distinct 1, 2, 3, 4) from test_cc group by v2;

-- query 16
USE ${case_db};
select count(distinct 1, v2) from test_cc;

-- query 17
USE ${case_db};
select count(distinct 1, v2) from test_cc;

-- query 18
USE ${case_db};
select count(distinct 1, 2, 3, 4), sum(distinct 1), avg(distinct 1), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3) from test_cc;

-- query 19
USE ${case_db};
select count(distinct 1, 2, 3, 4), sum(distinct 1), avg(distinct 1), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3) from test_cc group by v2 order by v2;

-- query 20
USE ${case_db};
select v2, count(distinct v1), sum(distinct v1), avg(distinct v1), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3)  from test_cc group by v2 order by v2;

-- query 21
USE ${case_db};
select v2, count(distinct v4), sum(distinct v4), avg(distinct v4), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3) from test_cc group by v2 order by v2;

-- query 22
USE ${case_db};
select v2, count(distinct v5), sum(distinct v5), avg(distinct v5), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3) from test_cc group by v2 order by v2;

-- query 23
USE ${case_db};
select v2, count(distinct v6), array_agg(distinct v6 ORDER BY v6), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3) from test_cc group by v2 order by v2;

-- query 24
USE ${case_db};
select v2, count(distinct v7), array_agg(distinct v7 ORDER BY v7), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3) from test_cc group by v2 order by v2;

-- query 25
USE ${case_db};
select v2, count(distinct v1, v3, v6), sum(distinct v1), avg(distinct v1), array_agg(v5 ORDER BY v5), LISTAGG(DISTINCT concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(1 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.3)  from test_cc group by v2 order by v2;

-- query 26
USE ${case_db};
select count(distinct v4, v5), sum(distinct v4), avg(distinct v4), LISTAGG(DISTINCT concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.456 ORDER BY 1.456) from test_cc;

-- query 27
USE ${case_db};
select v2, count(distinct v4, v5), sum(distinct v5), avg(distinct v5), LISTAGG(DISTINCT concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.456 ORDER BY 1.456) from test_cc group by v2 order by v2;

-- query 28
USE ${case_db};
select v2, count(distinct v3, v5), sum(distinct v4), avg(distinct v5), LISTAGG(DISTINCT concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct 1.456 ORDER BY 1.456) from test_cc group by v2, v3 order by v2, v3;

-- query 29
USE ${case_db};
select count(distinct v4, v5), sum(distinct v4), avg(distinct v4), LISTAGG(DISTINCT concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct v4 ORDER BY v4) from test_cc;

-- query 30
USE ${case_db};
select v2, count(distinct v4, v5), sum(distinct v5), avg(distinct v5), LISTAGG(DISTINCT concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct v5 ORDER BY v5) from test_cc group by v2 order by v2;

-- query 31
USE ${case_db};
select v2, count(distinct v3, v5), sum(distinct v4), avg(distinct v5), LISTAGG(DISTINCT concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR)), ',') WITHIN GROUP (ORDER BY concat(CAST(v4 AS VARCHAR), CAST(v5 AS VARCHAR), CAST(2 AS VARCHAR))), array_agg(distinct v5 ORDER BY v5) from test_cc group by v2, v3 order by v2, v3;
