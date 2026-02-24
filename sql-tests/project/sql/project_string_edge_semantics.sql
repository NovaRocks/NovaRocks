-- @order_sensitive=true
-- @tags=project,string,regexp,field,unhex
-- Test Objective:
-- 1. Validate regexp_extract no-match behavior returns empty string, not NULL.
-- 2. Validate field() accepts NULL-typed first argument and returns 0.
-- 3. Validate unhex() returns empty string for invalid/odd-length hex inputs.
-- Test Flow:
-- 1. Evaluate regexp_extract with no-match and NULL input.
-- 2. Evaluate field with NULL first argument.
-- 3. Evaluate unhex invalid/odd inputs and assert empty-string semantics.
SELECT
  regexp_extract('foo=123', 'bar=([0-9]+)', 1) AS re_no_match,
  regexp_extract(NULL, 'x', 1) AS re_null_input,
  field(NULL, 'a', 'b') AS field_null,
  field('b', 'a', 'b', 'c') AS field_hit,
  hex(unhex('ZZ')) AS unhex_bad_hex,
  unhex('ZZ') IS NULL AS unhex_bad_is_null,
  hex(unhex('F')) AS unhex_odd_hex,
  unhex('F') IS NULL AS unhex_odd_is_null;
