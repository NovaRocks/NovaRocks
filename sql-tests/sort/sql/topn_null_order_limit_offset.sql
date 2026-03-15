-- @order_sensitive=true
-- @tags=sort,topn,null_order,offset
-- Test Objective:
-- 1. Validate ORDER BY with explicit NULLS FIRST/LAST under LIMIT/OFFSET.
-- 2. Prevent regressions in multi-key null ordering for TopN output slicing.
-- Test Flow:
-- 1. Create/reset source table.
-- 2. Insert deterministic rows containing NULL and non-NULL sort keys.
-- 3. Query ordered page with LIMIT/OFFSET and assert exact row order.
DROP TABLE IF EXISTS ${case_db}.t_topn_null_order_limit_offset;
CREATE TABLE ${case_db}.t_topn_null_order_limit_offset (
  id INT,
  k INT,
  s STRING
);
INSERT INTO ${case_db}.t_topn_null_order_limit_offset VALUES
  (1, NULL, 'a'),
  (2, 2, 'x'),
  (3, 1, 'z'),
  (4, NULL, 'b'),
  (5, 1, NULL),
  (6, 3, 'm');
SELECT id, k, s
FROM ${case_db}.t_topn_null_order_limit_offset
ORDER BY k ASC NULLS LAST, s DESC NULLS FIRST, id ASC
LIMIT 4 OFFSET 1;
