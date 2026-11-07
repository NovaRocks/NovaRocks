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

-- Migrated from dev/test/sql/test_agg/R/test_agg_with_limit
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained SQL test case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_with_limit @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
create table t0 (
    c0 STRING,
    c1 STRING NOT NULL,
    c2 int,
    c3 int NOT NULL
)
TBLPROPERTIES ("format-version" = "3");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  20000));

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  20000));

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t0 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  20000));

-- query 6
-- @skip_result_check=true
USE ${case_db};
create table t1 (
    c0 STRING NOT NULL,
    c1 STRING,
    c2 int,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 7
-- @skip_result_check=true
USE ${case_db};
create table t2 (
    c0 int NOT NULL,
    c1 int,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 8
-- @skip_result_check=true
USE ${case_db};
create table t3 (
    c0 int,
    c1 int NOT NULL,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 9
-- @skip_result_check=true
USE ${case_db};
insert into t1 select * from t0;

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into t2 select * from t0;

-- query 11
-- @skip_result_check=true
USE ${case_db};
insert into t3 select * from t0;

-- query 12
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 13
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 14
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 15
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 16
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 17
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 18
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 19
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 20
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 21
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t1 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 22
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t2 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 23
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c1 from t3 group by c1 limit 10), cte1 as (select avg(c3), sum(c3) sc3, c1 from t0 group by c1 ) select count(*) from (select l.sc3, l.c1 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c1 <=> r.c1) t ;

-- query 24
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 25
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 26
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 27
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 28
-- @skip_result_check=true
USE ${case_db};

-- query 29
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 30
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t1 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 31
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t2 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 32
USE ${case_db};
with cte0 as (select avg(c3), sum(c3) sc3, c0 from t3 group by c0 limit 10000), cte1 as (select avg(c3), sum(c3) sc3, c0 from t0 group by c0 ) select count(*) from (select l.sc3, l.c0 from cte0 l join cte1 r on l.sc3 <=> r.sc3 and l.c0 <=> r.c0) t ;

-- query 33
-- @skip_result_check=true
USE ${case_db};
create table t4 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 34
-- @skip_result_check=true
USE ${case_db};
insert into t4 SELECT generate_series % 4, generate_series % 9, generate_series % 9, generate_series %9 FROM TABLE(generate_series(1,  9000));

-- query 35
-- @skip_result_check=true
USE ${case_db};
insert into t4 SELECT generate_series % 4, null, null, null FROM TABLE(generate_series(1,  9000));

-- query 36
-- @skip_result_check=true
USE ${case_db};
create table t5 (
    c0 int,
    c1 int,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 37
-- @skip_result_check=true
USE ${case_db};
insert into t5 select * from t4;

-- query 38
-- @skip_result_check=true
USE ${case_db};

-- query 39
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c0 from t5 group by c0 limit 10) t order by 3;

-- query 40
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c1 from t5 group by c1 limit 10) t order by 3;

-- query 41
-- @skip_result_check=true
USE ${case_db};

-- query 42
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c0 from t5 group by c0 limit 10) t order by 3;

-- query 43
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c1 from t5 group by c1 limit 10) t order by 3;

-- query 44
USE ${case_db};
select * from (select max(c3), sum(c3) sc3, c2 from t5 group by c2 limit 10) t order by 3;

-- query 45
-- @skip_result_check=true
USE ${case_db};

-- query 46
-- @skip_result_check=true
USE ${case_db};
create table t6 (
    c0 int,
    c1 float,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 47
-- @skip_result_check=true
USE ${case_db};
insert into t6 SELECT generate_series, generate_series, generate_series, generate_series FROM TABLE(generate_series(1,  10000));

-- query 48
USE ${case_db};
select count(*) from (select sum(c3) from t6 group by c1, c2 limit 10) t;

-- query 49
USE ${case_db};
select count(*) from (select sum(c3) from t6 group by c1, c2, c3 limit 10) t;

-- query 50
USE ${case_db};
select count(*) from (select sum(c3) from t6 group by c2, c1 limit 10) t;

-- query 51
-- @skip_result_check=true
USE ${case_db};
create table tempty (
    c0 int,
    c1 float,
    c2 string,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 52
USE ${case_db};
select sum(c3) from tempty group by c1, c2 limit 10;

-- query 53
USE ${case_db};
select sum(c3) from tempty group by c1, c2, c3 limit 10;

-- query 54
USE ${case_db};
select sum(c3) from tempty group by c2, c1 limit 10;

-- query 55
-- @skip_result_check=true
USE ${case_db};
create table tarray (
    c0 int,
    c1 array<int>,
    c3 int
)
TBLPROPERTIES ("format-version" = "3");

-- query 56
-- @skip_result_check=true
USE ${case_db};
insert into tarray SELECT generate_series, [generate_series], generate_series FROM TABLE(generate_series(1,  10000));

-- query 57
USE ${case_db};
select count(*) from (select sum(c3) from tarray group by c1, c3 limit 10) t;

-- query 58
USE ${case_db};
select count(*) from (select sum(c3) from tarray group by c1 limit 10) t;
