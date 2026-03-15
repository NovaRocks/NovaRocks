-- @order_sensitive=true
-- Test Objective:
-- 1. Validate CTAS with automatic partition by date_trunc.
-- 2. Validate CTAS with explicit partition range boundary.
-- 3. Validate CTAS with column expression partition.
-- 4. Validate CTAS with ORDER BY clause.
-- 5. Validate CTAS with INDEX clause.
-- Migrated from: dev/test/sql/test_automatic_partition/T/test_automatic_partition_ctas

-- query 1
-- Create source table with range partition and seed data
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineorder(
  lo_orderkey int(11) NOT NULL,
  lo_linenumber int(11) NOT NULL,
  lo_custkey int(11) NOT NULL,
  lo_partkey int(11) NOT NULL,
  lo_suppkey int(11) NOT NULL,
  lo_orderdate date NOT NULL,
  lo_orderpriority varchar(16) NOT NULL,
  lo_shippriority int(11) NOT NULL,
  lo_quantity int(11) NOT NULL,
  lo_extendedprice int(11) NOT NULL,
  lo_ordtotalprice int(11) NOT NULL,
  lo_discount int(11) NOT NULL,
  lo_revenue int(11) NOT NULL,
  lo_supplycost int(11) NOT NULL,
  lo_tax int(11) NOT NULL,
  lo_commitdate int(11) NOT NULL,
  lo_shipmode varchar(11) NOT NULL)
DUPLICATE KEY(lo_orderkey)
PARTITION BY RANGE(lo_orderdate)
(PARTITION p1 VALUES [("19920101"), ("19930101")),
PARTITION p2 VALUES [("19930101"), ("19940101")),
PARTITION p3 VALUES [("19940101"), ("19950101")),
PARTITION p4 VALUES [("19950101"), ("19960101")),
PARTITION p5 VALUES [("19960101"), ("19970101")),
PARTITION p6 VALUES [("19970101"), ("19980101")),
PARTITION p7 VALUES [("19980101"), ("19990101")))
DISTRIBUTED BY HASH(lo_orderkey) BUCKETS 48
PROPERTIES ("replication_num" = "1", "compression" = "LZ4");
INSERT INTO ${case_db}.lineorder VALUES(1,1,1,1,1,19920101,'LOW',1,100,200,300,10,190,100,5,19920201,'AIR');
INSERT INTO ${case_db}.lineorder VALUES(2,1,2,2,2,19930201,'MEDIUM',2,110,210,310,11,200,110,6,19930301,'SHIP');
INSERT INTO ${case_db}.lineorder VALUES(3,1,3,3,3,19940201,'HIGH',3,120,220,320,12,210,120,7,19940301,'RAIL');
INSERT INTO ${case_db}.lineorder VALUES(4,1,4,4,4,19950201,'LOW',1,130,230,330,13,220,130,8,19950301,'TRUCK');
INSERT INTO ${case_db}.lineorder VALUES(5,1,5,5,5,19960201,'MEDIUM',2,140,240,340,14,230,140,9,19960301,'AIR');
INSERT INTO ${case_db}.lineorder VALUES(6,1,6,6,6,19970201,'HIGH',3,150,250,350,15,240,150,10,19970301,'SHIP');
INSERT INTO ${case_db}.lineorder VALUES(7,1,7,7,7,19980201,'LOW',1,160,260,360,16,250,160,11,19980301,'RAIL');

-- query 2
-- CTAS with auto partition by date_trunc('year')
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineorder_auto
PARTITION BY date_trunc('year', LO_ORDERDATE)
AS SELECT * FROM ${case_db}.lineorder;

-- query 3
SELECT COUNT(*) FROM ${case_db}.lineorder_auto;

-- query 4
-- CTAS with auto partition and explicit range boundary
-- @skip_result_check=true
CREATE TABLE ${case_db}.lineorder_auto2
PARTITION BY date_trunc('year', LO_ORDERDATE)
(START ("1992-01-01") END ("1999-01-01") EVERY (INTERVAL 1 YEAR))
DISTRIBUTED BY HASH (LO_ORDERKEY)
AS SELECT * FROM ${case_db}.lineorder;

-- query 5
SELECT COUNT(*) FROM ${case_db}.lineorder_auto2;

-- query 6
-- CTAS with column expression partition
-- @skip_result_check=true
CREATE TABLE ${case_db}.site_access(
  event_day DATE, site_id INT DEFAULT '10',
  city_code VARCHAR(100), user_name VARCHAR(32) DEFAULT '', pv BIGINT DEFAULT '0')
DUPLICATE KEY(event_day, site_id, city_code, user_name)
PARTITION BY RANGE(event_day)(PARTITION p20200321 VALUES LESS THAN ("2020-03-22"))
DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE TABLE ${case_db}.test_col_expr PARTITION BY (event_day) AS SELECT event_day FROM ${case_db}.site_access;

-- query 7
-- CTAS with ORDER BY clause
-- @skip_result_check=true
CREATE TABLE ${case_db}.customers(
  customer_id int(11) NOT NULL,
  first_name varchar(65533) NULL,
  last_name varchar(65533) NULL,
  first_order date NULL,
  most_recent_order date NULL,
  number_of_orders bigint(20) NULL,
  customer_lifetime_value decimal128(38, 9) NULL)
PRIMARY KEY(customer_id)
DISTRIBUTED BY HASH(customer_id)
PROPERTIES ("replication_num" = "1");
CREATE TABLE ${case_db}.cust_order_by ORDER BY (first_name, last_name) AS SELECT * FROM ${case_db}.customers;

-- query 8
-- CTAS with INDEX clause
-- @skip_result_check=true
CREATE TABLE ${case_db}.cust_index (INDEX idx_bitmap_customer_id (customer_id) USING BITMAP) AS SELECT * FROM ${case_db}.customers;
