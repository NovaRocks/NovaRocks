-- Migrated from dev/test/sql/test_agg_function/R/test_statistic
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: testCorr
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE aggtest(
                        no int,
                        k decimal(10,2) ,
                        v decimal(10,2))
                        DUPLICATE KEY (no)
                        DISTRIBUTED BY HASH (no)
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_format" = "v2"
                    );

-- query 3
USE ${case_db};
select CORR(k, v) FROM aggtest;

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into aggtest values(1, 10, NULL);

-- query 5
USE ${case_db};
select CORR(k, v) FROM aggtest;

-- query 6
USE ${case_db};
select CORR(k, v) over (partition by no) FROM aggtest;

-- query 7
-- @skip_result_check=true
USE ${case_db};
insert into aggtest values(2, 10, 11), (2, 20, 22), (2, 25, NULL), (2, 30, 35);

-- query 8
USE ${case_db};
select abs(co - 0.9988445981121532)  / 0.9988445981121532 < 0.00001 from (select CORR(k, v)co FROM aggtest limit 1)result;

-- query 9
USE ${case_db};
select abs(co - 0.9988445981121532)  / 0.9988445981121532 < 0.00001 from (select /*+ SET_VAR (new_planner_agg_stage='1') */CORR(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 10
USE ${case_db};
select abs(co - 0.9988445981121532)  / 0.9988445981121532 < 0.00001 from (select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming',new_planner_agg_stage='2') */CORR(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 11
USE ${case_db};
select abs(co - 0.9988445981121532)  / 0.9988445981121532 < 0.00001 from (select /*+ SET_VAR (streaming_preaggregation_mode = 'FORCE_PREAGGREGATION',new_planner_agg_stage='2') */CORR(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 12
USE ${case_db};
select co is NULL,total = 1  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/CORR(k, v)co, count(distinct k)total FROM aggtest group by no order by no limit 0,1)result;

-- query 13
USE ${case_db};
select abs(co - 0.9988445981121532)  / 0.9988445981121532 < 0.00001,total = 4  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/CORR(k, v)co, count(distinct k)total FROM aggtest group by no order by no limit 1,1)result;

-- query 14
USE ${case_db};
select abs(co - 0.9988445981121532)  / 0.9988445981121532 < 0.00001,total = 4  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/CORR(k, v)co, count(distinct k)total FROM aggtest)result;

-- query 15
USE ${case_db};
select co is null from (select CORR(k, v) over (partition by no)co FROM aggtest order by co limit 0,1)result;

-- query 16
USE ${case_db};
select abs(co - 0.9988445981121532)  / 0.9988445981121532 < 0.00001 from (select CORR(k, v) over (partition by no)co FROM aggtest order by co limit 1,4)result;

-- name: testCovarPop
-- query 17
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS aggtest;
CREATE TABLE aggtest(
                        no int,
                        k decimal(10,2) ,
                        v decimal(10,2))
                        DUPLICATE KEY (no)
                        DISTRIBUTED BY HASH (no)
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_format" = "v2"
                    );

-- query 18
USE ${case_db};
select covar_pop(k, v)co FROM aggtest;

-- query 19
-- @skip_result_check=true
USE ${case_db};
insert into aggtest values(1, 10, NULL);

-- query 20
USE ${case_db};
select covar_pop(k, v)co FROM aggtest;

-- query 21
USE ${case_db};
select covar_pop(k, v) over (partition by no) FROM aggtest;

-- query 22
-- @skip_result_check=true
USE ${case_db};
insert into aggtest values(2, 10, 11), (2, 20, 22), (2, 25, NULL), (2, 30, 35);

-- query 23
USE ${case_db};
select abs(co - 80)  / 80 < 0.00001 from (select covar_pop(k, v)co FROM aggtest limit 1)result;

-- query 24
USE ${case_db};
select abs(co - 80)  / 80 < 0.00001 from (select /*+ SET_VAR (new_planner_agg_stage='1') */covar_pop(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 25
USE ${case_db};
select abs(co - 80)  / 80 < 0.00001 from (select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming',new_planner_agg_stage='2') */covar_pop(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 26
USE ${case_db};
select abs(co - 80)  / 80 < 0.00001 from (select /*+ SET_VAR (streaming_preaggregation_mode = 'FORCE_PREAGGREGATION',new_planner_agg_stage='2') */covar_pop(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 27
USE ${case_db};
select co is NULL,total = 1  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/covar_pop(k, v)co, count(distinct k)total FROM aggtest group by no order by no limit 0,1)result;

-- query 28
USE ${case_db};
select abs(co - 80)  / 80 < 0.00001,total = 4  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/covar_pop(k, v)co, count(distinct k)total FROM aggtest group by no order by no limit 1,1)result;

-- query 29
USE ${case_db};
select abs(co - 80)  / 80 < 0.00001,total = 4  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/covar_pop(k, v)co, count(distinct k)total FROM aggtest)result;

-- query 30
USE ${case_db};
select co is null from (select covar_pop(k, v) over (partition by no)co FROM aggtest order by co  limit 0,1)result;

-- query 31
USE ${case_db};
select abs(co - 80)  / 80 < 0.00001 from (select covar_pop(k, v) over (partition by no)co FROM aggtest order by co limit 1,4)result;

-- name: testCovarSamp
-- query 32
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS aggtest;
CREATE TABLE aggtest(
                        no int,
                        k decimal(10,2) ,
                        v decimal(10,2))
                        DUPLICATE KEY (no)
                        DISTRIBUTED BY HASH (no)
                        PROPERTIES (
                        "replication_num" = "1",
                        "storage_format" = "v2"
                    );

-- query 33
USE ${case_db};
select COVAR_SAMP(k, v)co FROM aggtest;

-- query 34
-- @skip_result_check=true
USE ${case_db};
insert into aggtest values(1, 10, NULL);

-- query 35
USE ${case_db};
select COVAR_SAMP(k, v)co FROM aggtest;

-- query 36
USE ${case_db};
select COVAR_SAMP(k, v) over (partition by no) FROM aggtest;

-- query 37
-- @skip_result_check=true
USE ${case_db};
insert into aggtest values(2, 10, 11), (2, 20, 22), (2, 25, NULL), (2, 30, 35);

-- query 38
USE ${case_db};
select abs(co - 120)  / 120 < 0.00001 from (select COVAR_SAMP(k, v)co FROM aggtest limit 1)result;

-- query 39
USE ${case_db};
select abs(co - 120)  / 120 < 0.00001 from (select /*+ SET_VAR (new_planner_agg_stage='1') */COVAR_SAMP(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 40
USE ${case_db};
select abs(co - 120)  / 120 < 0.00001 from (select /*+ SET_VAR (streaming_preaggregation_mode = 'force_streaming',new_planner_agg_stage='2') */COVAR_SAMP(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 41
USE ${case_db};
select abs(co - 120)  / 120 < 0.00001 from (select /*+ SET_VAR (streaming_preaggregation_mode = 'FORCE_PREAGGREGATION',new_planner_agg_stage='2') */COVAR_SAMP(k, v)co FROM aggtest group by no order by no limit 1,1)result;

-- query 42
USE ${case_db};
select co is NULL,total = 1  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/COVAR_SAMP(k, v)co, count(distinct k)total FROM aggtest group by no order by no limit 0,1)result;

-- query 43
USE ${case_db};
select abs(co - 120)  / 120 < 0.00001,total = 4  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/COVAR_SAMP(k, v)co, count(distinct k)total FROM aggtest group by no order by no limit 1,1)result;

-- query 44
USE ${case_db};
select abs(co - 120)  / 120 < 0.00001,total = 4  from (select /*+ SET_VAR (new_planner_agg_stage='3')*/COVAR_SAMP(k, v)co, count(distinct k)total FROM aggtest)result;

-- query 45
USE ${case_db};
select co is null from (select COVAR_SAMP(k, v) over (partition by no)co FROM aggtest order by co  limit 0,1)result;

-- query 46
USE ${case_db};
select abs(co - 120)  / 120 < 0.00001 from (select COVAR_SAMP(k, v) over (partition by no)co FROM aggtest order by co limit 1,4)result;
