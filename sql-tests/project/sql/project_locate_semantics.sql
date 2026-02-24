-- @order_sensitive=true
-- @tags=project,string
-- Test Objective:
-- 1. Validate LOCATE argument order semantics and INSTR compatibility.
-- 2. Validate LOCATE with start position and UTF-8 character index behavior.
-- Test Flow:
-- 1. Evaluate constant LOCATE/INSTR expressions.
-- 2. Cover LOCATE(start_pos), empty needle, and UTF-8 input.
-- 3. Assert deterministic scalar outputs.
SELECT
  LOCATE('b', 'abc') AS locate_basic,
  INSTR('abc', 'b') AS instr_basic,
  LOCATE('b', 'abc', 2) AS locate_with_pos,
  LOCATE('', 'abc', 2) AS locate_empty_needle,
  LOCATE('é', 'aébc', 2) AS locate_utf8;
