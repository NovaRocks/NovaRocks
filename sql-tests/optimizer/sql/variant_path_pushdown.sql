-- @tags=optimizer,iceberg,variant_path_pushdown
-- Test Objective:
-- Verify VariantPathPushdown exposes scan-level variant path materialization in
-- EXPLAIN VERBOSE, and that disabling the rule removes the scan hint.
DROP TABLE IF EXISTS ${case_db}.t_variant_path_pushdown FORCE;
CREATE TABLE ${case_db}.t_variant_path_pushdown (
  id INT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3"
);

-- @explain_contains=variant columns:
-- @explain_contains=variant_get(v, '$.a', 'bigint')
EXPLAIN VERBOSE SELECT id
FROM ${case_db}.t_variant_path_pushdown
WHERE variant_get(v, '$.a', 'bigint') = 1;

SET disable_optimizer_rules = 'VariantPathPushdown';

-- @explain_not_contains=variant columns:
EXPLAIN VERBOSE SELECT id
FROM ${case_db}.t_variant_path_pushdown
WHERE variant_get(v, '$.a', 'bigint') = 1;

SET disable_optimizer_rules = '';
DROP TABLE ${case_db}.t_variant_path_pushdown FORCE;
