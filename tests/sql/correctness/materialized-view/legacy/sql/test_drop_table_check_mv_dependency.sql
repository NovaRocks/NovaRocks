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

-- Test Objective:
-- 1. Validate base-table drop operations respect MV dependency checks.
-- 2. Cover dependency error paths before cleanup succeeds.
-- Source: dev/test/sql/test_materialized_view/T/test_drop_table_check_mv_dependency

-- query 1
drop database if exists db_drop_table_check_mv_dependency force;
create database db_drop_table_check_mv_dependency;

-- query 2
use db_drop_table_check_mv_dependency;

-- query 3
-- mv on table
create table t1 (c1 int, c2 string)
TBLPROPERTIES ("format-version" = "3");

-- query 4
create materialized view mv1 refresh async as select * from t1;

-- query 5
-- mv on view
create view v1 as select * from t1;

-- query 6
create materialized view mv2 refresh async as select * from v1;

-- query 7
-- mv on mv
create materialized view mv3 refresh async as select * from mv1;

-- query 8
-- check mv dependency


-- query 9
-- Dropping the base table must be blocked while dependent MVs still exist.
-- @expect_error=exists mv dependencies
drop table t1;

-- query 10
-- Dropping the view is also blocked while an MV still depends on it.
-- @expect_error=exists mv dependencies
drop view v1;

-- query 11
-- Dropping the parent MV is blocked while another MV is built on top of it.
-- @expect_error=exists mv dependencies
drop materialized view mv1;

-- query 12
-- not check mv dependency


-- query 13
drop table t1;

-- query 14
drop view v1;

-- query 15
drop materialized view mv1;

-- query 16
drop database db_drop_table_check_mv_dependency;
