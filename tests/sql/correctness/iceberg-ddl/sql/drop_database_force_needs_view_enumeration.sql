-- Test Objective:
-- A catalog that cannot enumerate views must say so, rather than let
-- DROP DATABASE ... FORCE assume the namespace holds none. This suite's
-- default catalog type is hadoop, which cannot hold views at all.
--
-- FORCE expands into a listing of the namespace's children, so a catalog that
-- cannot list one kind of child cannot answer the question. It used to answer
-- "no views" anyway, which is indistinguishable from an authoritative result.
-- The refusal is a deliberate, user-visible behavior change.

-- query 1
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_drop_force_views_${uuid0};
CREATE DATABASE sql_tests_drop_force_views_${uuid0};
USE sql_tests_drop_force_views_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE force_probe (id BIGINT);
INSERT INTO force_probe VALUES (42);

-- query 3
-- An absent target never reaches view enumeration: the namespace check runs
-- first, and IF EXISTS makes it a no-op.
-- @skip_result_check=true
DROP DATABASE IF EXISTS sql_tests_drop_force_absent_${uuid0} FORCE;

-- query 4
-- The namespace exists, so FORCE must enumerate its views, and this catalog
-- cannot answer that.
-- @expect_error=not supported by this catalog
DROP DATABASE sql_tests_drop_force_views_${uuid0} FORCE;

-- query 5
-- The rejected FORCE must leave both the namespace and the child data intact.
SELECT id FROM force_probe;

-- query 6
-- @cleanup=true
-- @skip_result_check=true
USE sql_tests_drop_force_views_${uuid0};
DROP TABLE IF EXISTS force_probe;
DROP DATABASE IF EXISTS sql_tests_drop_force_views_${uuid0};
