-- Migrated from dev/test/sql/test_bitmap_functions/T/test_unnest_bitmap
-- Test Objective:
-- 1. Validate unnest_bitmap table function: aggregation over all unnested values.
-- 2. Validate ordered unnest with limit (ascending and descending).
-- 3. Validate unnest_bitmap with enable_rewrite_unnest_bitmap_to_array optimization.
-- 4. Validate bitmap_to_string, bitmap_to_array, and unnest of bitmap_to_array in CTEs.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_unnest;
CREATE TABLE ${case_db}.t_unnest (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap BITMAP_UNION NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 2
-- @skip_result_check=true
insert into ${case_db}.t_unnest select 1, bitmap_empty();
insert into ${case_db}.t_unnest select 2, to_bitmap(2);
insert into ${case_db}.t_unnest select 3, bitmap_agg(generate_series) from table(generate_series(3, 12280));
insert into ${case_db}.t_unnest select 4, bitmap_agg(generate_series) from table(generate_series(4, 20));
insert into ${case_db}.t_unnest select 5, null;

-- query 3
-- Sum of all c1 values participating in unnest (rows with empty/null bitmap excluded from unnest_bitmap)
-- c1=1: bitmap_empty → no rows; c1=2: 1 row; c1=3: 12278 rows; c1=4: 17 rows; c1=5: null → no rows
-- sum(c1) = 2*1 + 3*12278 + 4*17 = 2 + 36834 + 68 = 36904
-- sum(unnest_bitmap) = 2 + sum(3..12280) + sum(4..20) = 2 + (3+12280)*12278/2 + (4+20)*17/2
select sum(c1), sum(unnest_bitmap) from ${case_db}.t_unnest, unnest_bitmap(c2);

-- query 4
-- @order_sensitive=true
-- First 5 rows ascending: smallest values from c1=2 (just 2), then c1=3 (3,4,5,6)
select c1, unnest_bitmap as c3 from ${case_db}.t_unnest, unnest_bitmap(c2) order by c1 asc, c3 asc limit 5;

-- query 5
-- @order_sensitive=true
-- Last 5 rows descending: from c1=4 (20,19,18,17,16)
select c1, unnest_bitmap as c3 from ${case_db}.t_unnest, unnest_bitmap(c2) order by c1 desc, c3 desc limit 5;

-- query 6
-- @skip_result_check=true
set enable_rewrite_unnest_bitmap_to_array=true;

-- query 7
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.test_tags;
CREATE TABLE ${case_db}.test_tags (
  c1 varchar(65533) NOT NULL,
  tag_name varchar(65533) NOT NULL,
  tag_value varchar(65533) NOT NULL,
  rb bitmap NOT NULL
) ENGINE=OLAP
  PRIMARY KEY(c1, tag_name, tag_value)
PARTITION BY (c1)
DISTRIBUTED BY HASH(tag_name, tag_value)
PROPERTIES ("replication_num" = "1");

-- query 8
-- @skip_result_check=true
insert into ${case_db}.test_tags(c1, tag_name, tag_value, rb) SELECT '20250114050000','a','57',bitmap_from_string("57");
insert into ${case_db}.test_tags(c1, tag_name, tag_value, rb) SELECT '20250114050000','a','a',bitmap_from_string("57,22253296,29101576,43027104");

-- query 9
-- @order_sensitive=true
-- bitmap_to_string: 'a','57' → "57"; 'a','a' → "57,22253296,29101576,43027104"
WITH result AS (SELECT rb FROM ${case_db}.test_tags),
     page_result AS (SELECT sub_bitmap(bitmap_union(rb), 0, 20) rb FROM result),
     all_tag AS (SELECT tag_name, tag_value, c1, bitmap_and(t0.rb, t1.rb) rb2 FROM ${case_db}.test_tags t0 JOIN page_result t1 WHERE bitmap_has_any(t0.rb, t1.rb))
SELECT tag_name, bitmap_to_string(rb2) as row_id_ FROM all_tag order by 1, 2;

-- query 10
-- @order_sensitive=true
-- bitmap_to_array: same data as above but as arrays
WITH result AS (SELECT rb FROM ${case_db}.test_tags),
     page_result AS (SELECT sub_bitmap(bitmap_union(rb), 0, 20) rb FROM result),
     all_tag AS (SELECT tag_name, tag_value, c1, bitmap_and(t0.rb, t1.rb) rb2 FROM ${case_db}.test_tags t0 JOIN page_result t1 WHERE bitmap_has_any(t0.rb, t1.rb))
SELECT tag_name, bitmap_to_array(rb2) as row_id_ FROM all_tag order by 1, 2;

-- query 11
-- @order_sensitive=true
-- unnest of bitmap_to_array: should expand to individual values
WITH result AS (SELECT rb FROM ${case_db}.test_tags),
     page_result AS (SELECT sub_bitmap(bitmap_union(rb), 0, 20) rb FROM result),
     all_tag AS (SELECT tag_name, tag_value, c1, bitmap_and(t0.rb, t1.rb) rb2 FROM ${case_db}.test_tags t0 JOIN page_result t1 WHERE bitmap_has_any(t0.rb, t1.rb))
SELECT tag_name, unnest as row_id_ FROM all_tag, unnest(bitmap_to_array(rb2)) order by 1, 2;
