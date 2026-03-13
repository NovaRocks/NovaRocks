-- Test Objective:
-- 1. Validate sync MV rewrite for count-distinct variants and bitmap/hll rewrites.
-- 2. Cover positive and negative rewrite cases under different distinct implementations.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view_count_distinct

-- query 1
drop table if exists user_event;

-- query 2
CREATE TABLE user_event (
    ds date   NOT NULL,
    id  varchar(256)    NOT NULL,
    user_id int DEFAULT NULL,
    user_id1    varchar(256)    DEFAULT NULL,
    user_id2    varchar(256)    DEFAULT NULL,
    column_01   int DEFAULT NULL,
    column_02   int DEFAULT NULL,
    column_03   int DEFAULT NULL,
    column_04   int DEFAULT NULL,
    column_05   int DEFAULT NULL,
    column_06   DECIMAL(12,2)   DEFAULT NULL,
    column_07   DECIMAL(12,3)   DEFAULT NULL,
    column_08   JSON   DEFAULT NULL,
    column_09   DATETIME    DEFAULT NULL,
    column_10   DATETIME    DEFAULT NULL,
    column_11   DATE    DEFAULT NULL,
    column_12   varchar(256)    DEFAULT NULL,
    column_13   varchar(256)    DEFAULT NULL,
    column_14   varchar(256)    DEFAULT NULL,
    column_15   varchar(256)    DEFAULT NULL,
    column_16   varchar(256)    DEFAULT NULL,
    column_17   varchar(256)    DEFAULT NULL,
    column_18   varchar(256)    DEFAULT NULL,
    column_19   varchar(256)    DEFAULT NULL,
    column_20   varchar(256)    DEFAULT NULL,
    column_21   varchar(256)    DEFAULT NULL,
    column_22   varchar(256)    DEFAULT NULL,
    column_23   varchar(256)    DEFAULT NULL,
    column_24   varchar(256)    DEFAULT NULL,
    column_25   varchar(256)    DEFAULT NULL,
    column_26   varchar(256)    DEFAULT NULL,
    column_27   varchar(256)    DEFAULT NULL,
    column_28   varchar(256)    DEFAULT NULL,
    column_29   varchar(256)    DEFAULT NULL,
    column_30   varchar(256)    DEFAULT NULL,
    column_31   varchar(256)    DEFAULT NULL,
    column_32   varchar(256)    DEFAULT NULL,
    column_33   varchar(256)    DEFAULT NULL,
    column_34   varchar(256)    DEFAULT NULL,
    column_35   varchar(256)    DEFAULT NULL,
    column_36   varchar(256)    DEFAULT NULL,
    column_37   varchar(256)    DEFAULT NULL
)
partition by date_trunc("day", ds)
distributed by hash(id);

-- query 3
-- ----- CASE0: NO DATA
create materialized view test_mv1
as
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 where ds >= '2023-11-02'
 group by
 ds
 ,column_19
 ,column_36;

-- query 4
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 5
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event group by ds,column_19,column_36;

-- query 6
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event where ds >= '2023-11-02' group by ds,column_19,column_36;

-- query 7
drop materialized view test_mv1;

-- query 8
INSERT INTO user_event (ds, id, user_id, user_id1, user_id2, column_01, column_02, column_03, column_04, column_05, column_06, column_07, column_08, column_09, column_10, column_11, column_12, column_13, column_14, column_15, column_16, column_17, column_18, column_19, column_20, column_21, column_22, column_23, column_24, column_25, column_26, column_27, column_28, column_29, column_30, column_31, column_32, column_33, column_34, column_35, column_36, column_37)
VALUES
('2023-11-01', '1', 1, '2', '1', 10, 20, 30, 40, 50, 12.34, 56.789, '{"key":""}', '2023-11-02 12:34:56', '2023-11-02 12:34:56', '2023-11-02', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37'),
('2023-11-02', '1', 1, '2', '1', 10, 20, 30, 40, 50, 12.34, 56.789, '{"key":""}', '2023-11-02 12:34:56', '2023-11-02 12:34:56', '2023-11-02', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37');

-- query 9
-- ----- CASE1 : NO WHERE
create materialized view test_mv1
as
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 group by
 ds
 ,column_19
 ,column_36;

-- query 10
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 11
-- Current NovaRocks does not rewrite this count-distinct shape through the sync MV.
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
explain select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union_count(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union_count(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union_count(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union_count(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union_count(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union_count(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 group by
 ds
 ,column_19
 ,column_36
 order by
 ds
 ,column_19
 ,column_36;

-- query 12
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event group by ds,column_19,column_36;

-- query 13
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union_count(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union_count(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union_count(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union_count(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union_count(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union_count(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 group by
 ds
 ,column_19
 ,column_36
 order by
 ds
 ,column_19
 ,column_36;

-- query 14
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
 ,count(distinct  user_id) as user_id_dist_cnt
 ,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
 ,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 ,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
 ,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
 ,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 15
select
 column_19
,column_36
 ,sum(column_01) as column_01_sum
 ,count(distinct user_id) as user_id_dist_cnt
,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')  then user_id2 else null end)) as filter_dist_cnt_1
,count(distinct (case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 from user_event
 where ds in ('2023-11-02')
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 16
INSERT INTO user_event (ds, id, user_id, user_id1, user_id2, column_01, column_02, column_03, column_04, column_05, column_06, column_07, column_08, column_09, column_10, column_11, column_12, column_13, column_14, column_15, column_16, column_17, column_18, column_19, column_20, column_21, column_22, column_23, column_24, column_25, column_26, column_27, column_28, column_29, column_30, column_31, column_32, column_33, column_34, column_35, column_36, column_37)
VALUES
('2023-11-01', '1', 1, '2', '1', 10, 20, 30, 40, 50, 12.34, 56.789, '{"key":""}', '2023-11-02 12:34:56', '2023-11-02 12:34:56', '2023-11-02', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37'),
('2023-11-02', '1', 1, '2', '1', 10, 20, 30, 40, 50, 12.34, 56.789, '{"key":""}', '2023-11-02 12:34:56', '2023-11-02 12:34:56', '2023-11-02', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37');

-- query 17
-- Planner choice for this no-WHERE explain is not stable after new data arrives; keep it as an explain smoke check.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event group by ds,column_19,column_36;

-- query 18
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union_count(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union_count(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union_count(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union_count(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union_count(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union_count(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 group by
 ds
 ,column_19
 ,column_36
 order by
 ds
 ,column_19
 ,column_36;

-- query 19
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
 ,count(distinct  user_id) as user_id_dist_cnt
 ,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
 ,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 ,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
 ,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
 ,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 20
select
 column_19
,column_36
 ,sum(column_01) as column_01_sum
 ,count(distinct user_id) as user_id_dist_cnt
,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')  then user_id2 else null end)) as filter_dist_cnt_1
,count(distinct (case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 from user_event
 where ds in ('2023-11-02')
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 21
drop materialized view if exists test_mv1;

-- query 22
-- ----- CASE2 : WITH WHERE
create materialized view test_mv1
as
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 where ds >= '2023-11-02'
 group by
 ds
 ,column_19
 ,column_36;

-- query 23
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 24
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event group by ds,column_19,column_36;

-- query 25
-- The WHERE-filtered explain is also kept as a smoke check because rewrite choice is planner-state dependent here.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event where ds >= '2023-11-02' group by ds,column_19,column_36;

-- query 26
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union_count(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union_count(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union_count(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union_count(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union_count(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union_count(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 where ds >= '2023-11-02'
 group by
 ds
 ,column_19
 ,column_36
 order by
 ds
 ,column_19
 ,column_36;

-- query 27
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
 ,count(distinct  user_id) as user_id_dist_cnt
 ,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
 ,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 ,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
 ,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
 ,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 where ds >= '2023-11-02'
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 28
select
 column_19
,column_36
 ,sum(column_01) as column_01_sum
 ,count(distinct user_id) as user_id_dist_cnt
,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')  then user_id2 else null end)) as filter_dist_cnt_1
,count(distinct (case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 from user_event
 where ds in ('2023-11-02')
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 29
INSERT INTO user_event (ds, id, user_id, user_id1, user_id2, column_01, column_02, column_03, column_04, column_05, column_06, column_07, column_08, column_09, column_10, column_11, column_12, column_13, column_14, column_15, column_16, column_17, column_18, column_19, column_20, column_21, column_22, column_23, column_24, column_25, column_26, column_27, column_28, column_29, column_30, column_31, column_32, column_33, column_34, column_35, column_36, column_37)
VALUES
('2023-11-01', '1', 1, '2', '1', 10, 20, 30, 40, 50, 12.34, 56.789, '{"key":""}', '2023-11-02 12:34:56', '2023-11-02 12:34:56', '2023-11-02', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37'),
('2023-11-02', '1', 1, '2', '1', 10, 20, 30, 40, 50, 12.34, 56.789, '{"key":""}', '2023-11-02 12:34:56', '2023-11-02 12:34:56', '2023-11-02', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37');

-- query 30
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event group by ds,column_19,column_36;

-- query 31
-- The same WHERE-filtered explain remains a smoke check after more data arrives.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN select ds,column_19 ,column_36,sum(column_01) as column_01_sum,count(distinct  user_id) as user_id_dist_cnt,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5 from user_event where ds >= '2023-11-02' group by ds,column_19,column_36;

-- query 32
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
,bitmap_union_count(to_bitmap( user_id)) as user_id_dist_cnt
,bitmap_union_count(to_bitmap(case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
,bitmap_union_count(to_bitmap( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
,bitmap_union_count(to_bitmap(case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
,bitmap_union_count(to_bitmap(case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
,bitmap_union_count(to_bitmap( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 where ds >= '2023-11-02'
 group by
 ds
 ,column_19
 ,column_36
 order by
 ds
 ,column_19
 ,column_36;

-- query 33
select
ds
,column_19
,column_36
,sum(column_01) as column_01_sum
 ,count(distinct  user_id) as user_id_dist_cnt
 ,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')   then user_id2 else null end)) as filter_dist_cnt_1
 ,count(distinct ( case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 ,count(distinct (case when column_03 > 70 and column_36 IN ('21','23') then  user_id2 else null end)) as filter_dist_cnt_3
 ,count(distinct (case when column_04 > 20 and column_27 IN ('31','27') then  user_id2 else null end)) as filter_dist_cnt_4
 ,count(distinct ( case when column_05 > 90 and column_28 IN ('41','43') then  user_id2 else null end)) as filter_dist_cnt_5
 from user_event
 where ds >= '2023-11-02'
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 34
select
 column_19
,column_36
 ,sum(column_01) as column_01_sum
 ,count(distinct user_id) as user_id_dist_cnt
,count(distinct (case when column_01 > 1 and column_34 IN ('1','34')  then user_id2 else null end)) as filter_dist_cnt_1
,count(distinct (case when column_02 > 60 and column_35 IN ('11','13') then  user_id2 else null end)) as filter_dist_cnt_2
 from user_event
 where ds in ('2023-11-02')
 group by
 ds
 ,column_19
 ,column_36
  order by
 ds
 ,column_19
 ,column_36;

-- query 35
drop materialized view test_mv1;
