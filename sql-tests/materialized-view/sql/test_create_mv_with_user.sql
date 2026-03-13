-- Test Objective:
-- 1. Validate MV refresh privilege checks under different users and roles.
-- 2. Cover refresh authorization success and denial paths.
-- Source: dev/test/sql/test_materialized_view/T/test_create_mv_with_user

-- query 1
drop database if exists test_create_mv_with_user;

-- query 2
create database test_create_mv_with_user;

-- query 3
use test_create_mv_with_user;

-- query 4
-- sql-tests always uses a wildcard host pattern here, so the old dev/test shell discovery step is unnecessary.
-- @skip_result_check=true
SELECT '%' AS mv_creator_host_pattern;

-- query 5
DROP USER IF EXISTS mv_creator@'%';

-- query 6
CREATE USER mv_creator@'%';

-- query 7
GRANT DELETE, DROP, INSERT, SELECT, ALTER, UPDATE ON ALL TABLES IN DATABASE test_create_mv_with_user
    TO USER mv_creator@'%';

-- query 8
GRANT CREATE TABLE, CREATE MATERIALIZED VIEW ON DATABASE test_create_mv_with_user
    TO USER mv_creator@'%';

-- query 9
GRANT SELECT, DROP, ALTER, REFRESH ON ALL MATERIALIZED VIEWS IN DATABASE test_create_mv_with_user
    TO USER mv_creator@'%';

-- query 10
-- switch user mv_creator
GRANT IMPERSONATE ON USER root TO mv_creator@'%';

-- query 11
EXECUTE AS mv_creator@'%' with no revert;

-- query 12
-- create & use materialized view
create table t1(c1 int, c2 int);

-- query 13
insert into t1 values(1,1);

-- query 14
create materialized view mv1 REFRESH MANUAL as select * from t1;

-- query 15
-- Successful sync refresh returns a dynamic QUERY_ID. Validate behavior through follow-up SQL instead
-- of snapshotting the transient identifier.
-- @skip_result_check=true
refresh materialized view mv1;

-- query 16
alter materialized view mv1 refresh manual;

-- query 17
-- @skip_result_check=true
refresh materialized view mv1;

-- query 18
select * from mv1;

-- query 19
drop materialized view mv1;

-- query 20
execute as root with no revert;

-- query 21
drop user mv_creator@'%';

-- query 22
-- Test TaskRun role activation fix: User with multiple roles, only one has REFRESH privilege
-- This test verifies that MV refresh works regardless of session default roles
-- because TaskRun context activates all user roles
-- Tests both creator-based and root-based authorization modes
DROP USER IF EXISTS mv_multi_role_user@'%';

-- query 23
DROP ROLE IF EXISTS role_with_refresh;

-- query 24
DROP ROLE IF EXISTS role_without_refresh;

-- query 25
CREATE ROLE role_with_refresh;

-- query 26
GRANT SELECT, DROP, ALTER, REFRESH ON ALL MATERIALIZED VIEWS IN DATABASE test_create_mv_with_user TO ROLE role_with_refresh;

-- query 27
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN DATABASE test_create_mv_with_user TO ROLE role_with_refresh;

-- query 28
GRANT CREATE TABLE, CREATE MATERIALIZED VIEW ON DATABASE test_create_mv_with_user TO ROLE role_with_refresh;

-- query 29
CREATE ROLE role_without_refresh;

-- query 30
GRANT SELECT, DROP ON ALL MATERIALIZED VIEWS IN DATABASE test_create_mv_with_user TO ROLE role_without_refresh;

-- query 31
GRANT SELECT  ON ALL TABLES IN DATABASE test_create_mv_with_user TO ROLE role_without_refresh;

-- query 32
GRANT CREATE TABLE, CREATE MATERIALIZED VIEW ON DATABASE test_create_mv_with_user TO ROLE role_without_refresh;

-- query 33
CREATE USER mv_multi_role_user@'%';

-- query 34
GRANT role_with_refresh, role_without_refresh TO USER mv_multi_role_user@'%';

-- query 35
GRANT IMPERSONATE ON USER root TO mv_multi_role_user@'%';

-- query 36
-- Grant direct privileges to user so they can create tables even with default_role=none
-- GRANT CREATE TABLE, CREATE MATERIALIZED VIEW ON DATABASE test_create_mv_with_user TO USER mv_multi_role_user@'${ip[1]}';
-- GRANT SELECT ON ALL TABLES IN DATABASE test_create_mv_with_user TO USER mv_multi_role_user@'${ip[1]}';
-- Test Case 1: Creator-based authorization (mv_use_creator_based_authorization=true)
-- This tests the default behavior where MV refresh uses the creator's context
SET DEFAULT ROLE NONE TO mv_multi_role_user@'%';

-- query 37
EXECUTE AS mv_multi_role_user@'%' with no revert;

-- query 38
SET ROLE role_with_refresh;

-- query 39
CREATE TABLE test_mv_table(id int, name varchar(20), value int);

-- query 40
INSERT INTO test_mv_table VALUES (1, 'test1', 100), (2, 'test2', 200);

-- query 41
CREATE MATERIALIZED VIEW test_mv_async REFRESH MANUAL AS
SELECT id, name, SUM(value) as total_value
FROM test_mv_table
GROUP BY id, name;

-- query 42
SET ROLE role_with_refresh;

-- query 43
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async WITH SYNC MODE;

-- query 44
SET ROLE role_without_refresh;

-- query 45
-- @expect_error=Access denied; you need (at least one of) the REFRESH privilege(s) on MATERIALIZED VIEW test_mv_async for this operation. Please ask the admin to grant permission(s) or try activating existing roles using <set [default] role>. Current role(s): [role_without_refresh]. Inactivated role(s): [role_with_refresh].
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async WITH SYNC MODE;

-- query 46
-- Test Case 2: Creator-based authorization with default role
SET DEFAULT ROLE role_with_refresh TO mv_multi_role_user@'%';

-- query 47
SET ROLE role_with_refresh;

-- query 48
CREATE TABLE test_mv_table2(id int, name varchar(20), value int);

-- query 49
INSERT INTO test_mv_table2 VALUES (1, 'test1', 100), (2, 'test2', 200);

-- query 50
CREATE MATERIALIZED VIEW test_mv_async2 REFRESH MANUAL AS
SELECT id, name, SUM(value) as total_value
FROM test_mv_table2
GROUP BY id, name;

-- query 51
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async2 WITH SYNC MODE;

-- query 52
SET ROLE role_without_refresh;

-- query 53
-- @expect_error=Access denied; you need (at least one of) the REFRESH privilege(s) on MATERIALIZED VIEW test_mv_async2 for this operation. Please ask the admin to grant permission(s) or try activating existing roles using <set [default] role>. Current role(s): [role_without_refresh]. Inactivated role(s): [role_with_refresh].
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async2 WITH SYNC MODE;

-- query 54
-- Test Case 3: Creator-based authorization with insufficient default role
SET DEFAULT ROLE role_without_refresh TO mv_multi_role_user@'%';

-- query 55
SET ROLE role_with_refresh;

-- query 56
CREATE TABLE test_mv_table3(id int, name varchar(20), value int);

-- query 57
INSERT INTO test_mv_table3 VALUES (1, 'test1', 100), (2, 'test2', 200);

-- query 58
CREATE MATERIALIZED VIEW test_mv_async3 REFRESH MANUAL AS
SELECT id, name, SUM(value) as total_value
FROM test_mv_table3
GROUP BY id, name;

-- query 59
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async3 WITH SYNC MODE;

-- query 60
SET ROLE role_without_refresh;

-- query 61
-- @expect_error=Access denied; you need (at least one of) the REFRESH privilege(s) on MATERIALIZED VIEW test_mv_async3 for this operation. Please ask the admin to grant permission(s) or try activating existing roles using <set [default] role>. Current role(s): [role_without_refresh]. Inactivated role(s): [role_with_refresh].
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async3 WITH SYNC MODE;

-- query 62
-- Test Case 4: Root-based authorization (mv_use_creator_based_authorization=false)
-- This tests when MV refresh always uses ROOT user regardless of creator
EXECUTE AS root with no revert;

-- query 63
REVOKE role_with_refresh FROM mv_multi_role_user@'%';

-- query 64
SET DEFAULT ROLE NONE TO mv_multi_role_user@'%';

-- query 65
EXECUTE AS mv_multi_role_user@'%' with no revert;

-- query 66
SET ROLE role_without_refresh;

-- query 67
CREATE TABLE test_mv_table4(id int, name varchar(20), value int);

-- query 68
CREATE MATERIALIZED VIEW test_mv_async4 REFRESH DEFERRED MANUAL AS
SELECT id, name, SUM(value) as total_value
FROM test_mv_table4
GROUP BY id, name;

-- query 69
EXECUTE AS root with no revert;

-- query 70
INSERT INTO test_mv_table4 VALUES (1, 'test1', 100), (2, 'test2', 200);

-- query 71
ADMIN SET FRONTEND CONFIG('mv_use_creator_based_authorization' = 'true');

-- query 72
SELECT count(*) from test_mv_async4;

-- query 73
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async4 WITH SYNC MODE;

-- query 74
SELECT count(*) from test_mv_async4;

-- query 75
ADMIN SET FRONTEND CONFIG('mv_use_creator_based_authorization' = 'false');

-- query 76
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_create_mv_with_user.test_mv_async4 WITH SYNC MODE;

-- query 77
SELECT count(*) from test_mv_async4;

-- query 78
-- Cleanup all created objects
ADMIN SET FRONTEND CONFIG('mv_use_creator_based_authorization' = 'true');

-- query 79
DROP ROLE role_with_refresh;

-- query 80
DROP ROLE role_without_refresh;

-- query 81
DROP USER mv_multi_role_user@'%';
