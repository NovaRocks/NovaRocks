-- Auto-generated static DDL for sql_test_catalog bootstrap.
-- Database: ssb

CREATE TABLE `customer` (
  `c_custkey` INT NOT NULL,
  `c_name` STRING NOT NULL,
  `c_address` STRING NOT NULL,
  `c_city` STRING NOT NULL,
  `c_nation` STRING NOT NULL,
  `c_region` STRING NOT NULL,
  `c_phone` STRING NOT NULL,
  `c_mktsegment` STRING NOT NULL
);

CREATE TABLE `dates` (
  `d_datekey` INT NOT NULL,
  `d_date` STRING NOT NULL,
  `d_dayofweek` STRING NOT NULL,
  `d_month` STRING NOT NULL,
  `d_year` INT NOT NULL,
  `d_yearmonthnum` INT NOT NULL,
  `d_yearmonth` STRING NOT NULL,
  `d_daynuminweek` INT NOT NULL,
  `d_daynuminmonth` INT NOT NULL,
  `d_daynuminyear` INT NOT NULL,
  `d_monthnuminyear` INT NOT NULL,
  `d_weeknuminyear` INT NOT NULL,
  `d_sellingseason` STRING NOT NULL,
  `d_lastdayinweekfl` INT NOT NULL,
  `d_lastdayinmonthfl` INT NOT NULL,
  `d_holidayfl` INT NOT NULL,
  `d_weekdayfl` INT NOT NULL
);

CREATE TABLE `lineorder` (
  `lo_orderkey` INT NOT NULL,
  `lo_linenumber` INT NOT NULL,
  `lo_custkey` INT NOT NULL,
  `lo_partkey` INT NOT NULL,
  `lo_suppkey` INT NOT NULL,
  `lo_orderdate` INT NOT NULL,
  `lo_orderpriority` STRING NOT NULL,
  `lo_shippriority` INT NOT NULL,
  `lo_quantity` INT NOT NULL,
  `lo_extendedprice` INT NOT NULL,
  `lo_ordtotalprice` INT NOT NULL,
  `lo_discount` INT NOT NULL,
  `lo_revenue` INT NOT NULL,
  `lo_supplycost` INT NOT NULL,
  `lo_tax` INT NOT NULL,
  `lo_commitdate` INT NOT NULL,
  `lo_shipmode` STRING NOT NULL
);

CREATE TABLE `part` (
  `p_partkey` INT NOT NULL,
  `p_name` STRING NOT NULL,
  `p_mfgr` STRING NOT NULL,
  `p_category` STRING NOT NULL,
  `p_brand` STRING NOT NULL,
  `p_color` STRING NOT NULL,
  `p_type` STRING NOT NULL,
  `p_size` INT NOT NULL,
  `p_container` STRING NOT NULL
);

CREATE TABLE `supplier` (
  `s_suppkey` INT NOT NULL,
  `s_name` STRING NOT NULL,
  `s_address` STRING NOT NULL,
  `s_city` STRING NOT NULL,
  `s_nation` STRING NOT NULL,
  `s_region` STRING NOT NULL,
  `s_phone` STRING NOT NULL
);
