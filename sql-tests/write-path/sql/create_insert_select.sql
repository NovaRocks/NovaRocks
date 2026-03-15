-- @order_sensitive=true
-- Test Objective:
-- 1. Validate CREATE TABLE and multi-row INSERT for primitive + nullable values.
-- 2. Validate deterministic read-back for inserted rows.
-- Test Flow:
-- 1. Create/reset a basic table in the write-path database.
-- 2. Insert fixed rows including a NULL value.
-- 3. Read rows with ORDER BY and compare with expected result.
DROP TABLE IF EXISTS ${case_db}.t_basic;
CREATE TABLE ${case_db}.t_basic (
  id BIGINT,
  name STRING,
  qty BIGINT
);
INSERT INTO ${case_db}.t_basic VALUES
  (1, 'apple', 10),
  (2, 'banana', 20),
  (3, 'banana', NULL);
SELECT id, name, qty
FROM ${case_db}.t_basic
ORDER BY id;
