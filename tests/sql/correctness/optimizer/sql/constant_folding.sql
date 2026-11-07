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

-- @tags=optimizer,constant_folding
-- Test Objective:
-- 1. FoldConstant evaluates constant scalar sub-trees during LogicalNormalize,
--    so the plan carries literals instead of arithmetic / cast / function nodes.
-- 2. A string literal compared against a DATE column folds to the same scan
--    predicate the typed DATE literal produces. That equality is the point of
--    the rule: static-predicate lowering only accepts a bare literal, so the
--    unfolded CAST shape never reached partition / row-group / page pruning.
-- 3. Volatile builtins and environment-sensitive builtins are never folded.
-- 4. SET disable_optimizer_rules = 'FoldConstant' restores the unfolded plan
--    while the results stay identical.
--
-- Note on labels: the output column label is rendered from the parsed
-- statement, so it still shows the original expression text after folding.
-- The assertions below therefore target the projection *expression* position
-- (`[<expr> AS <label>]`), not the label.
DROP TABLE IF EXISTS ${case_db}.t_fold_dates;
CREATE TABLE ${case_db}.t_fold_dates (id INT, dt DATE);
INSERT INTO ${case_db}.t_fold_dates VALUES
    (1, '2019-12-31'),
    (2, '2020-01-01'),
    (3, '2020-01-02');

-- ---------------------------------------------------------------------------
-- 1. Constant arithmetic collapses to one literal.
-- ---------------------------------------------------------------------------
-- @explain_contains=[42096 AS
-- @explain_not_contains=[40000 + 2000 + 96 AS
SELECT 40000 + 2000 + 96;

-- ---------------------------------------------------------------------------
-- 2. A constant function call collapses to its result literal. The string
--    argument binds to the DATE overload, so the intermediate cast folds to a
--    Date32 literal first and the whole call folds afterwards.
-- ---------------------------------------------------------------------------
-- @explain_contains=['2020-01' AS
-- @explain_not_contains=[date_format(
SELECT date_format('2020-01-01', '%Y-%m');

-- ---------------------------------------------------------------------------
-- 3. A string literal against a DATE column folds to a DATE constant,
--    rendered as a calendar date with no CAST left in the scan predicate.
-- ---------------------------------------------------------------------------
-- @explain_contains=predicates: dt = 2020-01-01
-- @explain_not_contains=CAST(
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = '2020-01-01';

-- ---------------------------------------------------------------------------
-- 4. The typed DATE literal must produce exactly the same scan predicate.
-- ---------------------------------------------------------------------------
-- @explain_contains=predicates: dt = 2020-01-01
-- @explain_not_contains=CAST(
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = DATE '2020-01-01';

-- ---------------------------------------------------------------------------
-- 5. Volatile builtins survive in the plan.
-- ---------------------------------------------------------------------------
-- @skip_result_check=true
-- @result_contains=[rand()]
EXPLAIN VERBOSE SELECT rand();

-- @skip_result_check=true
-- @result_contains=[now()]
EXPLAIN VERBOSE SELECT now();

-- ---------------------------------------------------------------------------
-- 6. Environment-sensitive builtins are immutable but read the process
--    timezone, so folding them on the frontend would move that read off the
--    backend. They must survive in the plan too.
-- ---------------------------------------------------------------------------
-- @skip_result_check=true
-- @result_contains=[from_unixtime(1577836800)]
EXPLAIN VERBOSE SELECT from_unixtime(1577836800);

-- ---------------------------------------------------------------------------
-- 7. Integer division by zero is a typed error in folded and runtime paths.
--    Constant folding must preserve the same DIVISION_BY_ZERO outcome.
-- ---------------------------------------------------------------------------
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT 1 / 0;

-- ---------------------------------------------------------------------------
-- 8. Disabling the rule restores the unfolded plan; results are unchanged.
-- ---------------------------------------------------------------------------
SET disable_optimizer_rules = 'FoldConstant';

-- @explain_contains=[40000 + 2000 + 96 AS
SELECT 40000 + 2000 + 96;

-- @explain_contains=predicates: dt = CAST('2020-01-01' AS Date32)
SELECT id FROM ${case_db}.t_fold_dates WHERE dt = '2020-01-01';

-- ---------------------------------------------------------------------------
-- 9. Each supported folding family keeps its value and constant plan shape.
--    The matching FoldConstant-disabled block below exercises today's compiled
--    runtime as an independent result oracle. Keep explicit aliases so plan
--    assertions cannot accidentally match a parsed expression in a column label.
-- ---------------------------------------------------------------------------
SET disable_optimizer_rules = '';
SET sql_mode = 32;

-- @explain_contains=['MIXED' AS folded_upper]
SELECT upper('MiXeD') AS folded_upper;

-- @explain_contains=[7 AS folded_abs]
SELECT abs(7) AS folded_abs;

-- @explain_contains=[10 AS folded_if]
SELECT if(true, 10, 20) AS folded_if;

-- @explain_contains=[8 AS folded_coalesce]
SELECT coalesce(CAST(NULL AS INT), 8) AS folded_coalesce;

-- @explain_contains=[9 AS folded_ifnull]
SELECT ifnull(CAST(NULL AS INT), 9) AS folded_ifnull;

-- @explain_contains=[NULL AS folded_nullif]
SELECT nullif(9, 9) AS folded_nullif;

-- @explain_contains=[NULL AS folded_and]
SELECT CAST(NULL AS BOOLEAN) AND true AS folded_and;

-- @explain_contains=[true AS folded_or]
SELECT CAST(NULL AS BOOLEAN) OR true AS folded_or;

-- @explain_contains=[NULL AS folded_not]
SELECT NOT CAST(NULL AS BOOLEAN) AS folded_not;

-- @explain_contains=[true AS folded_decimal_compare]
SELECT CAST(12.30 AS DECIMAL(4,2)) = CAST(12.3 AS DECIMAL(3,1)) AS folded_decimal_compare;

-- @explain_contains=[true AS folded_temporal_compare]
SELECT CAST('2020-01-01' AS DATE) = '2020-01-01' AS folded_temporal_compare;

-- @explain_contains=[true AS folded_nullsafe_equal]
SELECT CAST(NULL AS INT) <=> CAST(NULL AS INT) AS folded_nullsafe_equal;

-- @explain_contains=[false AS folded_nullsafe_distinct]
SELECT CAST(NULL AS INT) <=> CAST(1 AS INT) AS folded_nullsafe_distinct;

-- @explain_contains=[12 AS folded_narrow]
SELECT CAST(12 AS TINYINT) AS folded_narrow;

-- @explain_contains=[42 AS folded_integer_parse]
SELECT CAST('42' AS INT) AS folded_integer_parse;

-- @explain_contains=[2020-01-02 AS folded_date_parse]
SELECT CAST('2020-01-02' AS DATE) AS folded_date_parse;

-- @explain_contains=[12.35 AS folded_decimal_rounding]
SELECT CAST(CAST(12.345 AS DECIMAL(5,3)) AS DECIMAL(4,2)) AS folded_decimal_rounding;

-- @explain_contains=[NULL AS folded_decimal_overflow]
SELECT CAST(1000 AS DECIMAL(3,0)) AS folded_decimal_overflow;

-- ---------------------------------------------------------------------------
-- 10. Disable folding and require the original operation to remain visible.
--     Every result must match the corresponding folded query above.
-- ---------------------------------------------------------------------------
SET disable_optimizer_rules = 'FoldConstant';

-- @explain_contains=[upper(
SELECT upper('MiXeD') AS folded_upper;

-- @explain_contains=[abs(
SELECT abs(7) AS folded_abs;

-- @explain_contains=[if(
SELECT if(true, 10, 20) AS folded_if;

-- @explain_contains=[coalesce(
SELECT coalesce(CAST(NULL AS INT), 8) AS folded_coalesce;

-- @explain_contains=[ifnull(
SELECT ifnull(CAST(NULL AS INT), 9) AS folded_ifnull;

-- @explain_contains=[nullif(
SELECT nullif(9, 9) AS folded_nullif;

-- @explain_contains= AND true AS folded_and]
SELECT CAST(NULL AS BOOLEAN) AND true AS folded_and;

-- @explain_contains= OR true AS folded_or]
SELECT CAST(NULL AS BOOLEAN) OR true AS folded_or;

-- @explain_contains=[NOT
SELECT NOT CAST(NULL AS BOOLEAN) AS folded_not;

-- @explain_contains= = CAST(
SELECT CAST(12.30 AS DECIMAL(4,2)) = CAST(12.3 AS DECIMAL(3,1)) AS folded_decimal_compare;

-- @explain_contains= = CAST(
SELECT CAST('2020-01-01' AS DATE) = '2020-01-01' AS folded_temporal_compare;

-- @explain_contains=<=>
SELECT CAST(NULL AS INT) <=> CAST(NULL AS INT) AS folded_nullsafe_equal;

-- @explain_contains=<=>
SELECT CAST(NULL AS INT) <=> CAST(1 AS INT) AS folded_nullsafe_distinct;

-- @explain_contains=[CAST(12 AS
SELECT CAST(12 AS TINYINT) AS folded_narrow;

-- @explain_contains=[CAST('42' AS
SELECT CAST('42' AS INT) AS folded_integer_parse;

-- @explain_contains=[CAST('2020-01-02' AS
SELECT CAST('2020-01-02' AS DATE) AS folded_date_parse;

-- @explain_contains=[CAST(CAST(12.345 AS
SELECT CAST(CAST(12.345 AS DECIMAL(5,3)) AS DECIMAL(4,2)) AS folded_decimal_rounding;

-- @explain_contains=[CAST(1000 AS
SELECT CAST(1000 AS DECIMAL(3,0)) AS folded_decimal_overflow;

-- ---------------------------------------------------------------------------
-- 11. ERROR_IF_OVERFLOW still folds successful rounding, and an actual row
--     error stays unfolded so execution owns the same error in both modes.
-- ---------------------------------------------------------------------------
SET disable_optimizer_rules = '';
SET sql_mode = 'ERROR_IF_OVERFLOW';

-- @explain_contains=[12.35 AS checked_decimal_rounding]
SELECT CAST(CAST(12.345 AS DECIMAL(5,3)) AS DECIMAL(4,2)) AS checked_decimal_rounding;

-- @skip_result_check=true
-- @result_contains=[CAST(1000 AS
EXPLAIN VERBOSE SELECT CAST(1000 AS DECIMAL(3,0)) AS checked_decimal_overflow;

-- @skip_result_check=true
-- @expect_sql_code=query.execution.numeric_out_of_range
SELECT CAST(1000 AS DECIMAL(3,0)) AS checked_decimal_overflow;

SET disable_optimizer_rules = 'FoldConstant';

-- @explain_contains=[CAST(CAST(12.345 AS
SELECT CAST(CAST(12.345 AS DECIMAL(5,3)) AS DECIMAL(4,2)) AS checked_decimal_rounding;

-- @skip_result_check=true
-- @result_contains=[CAST(1000 AS
EXPLAIN VERBOSE SELECT CAST(1000 AS DECIMAL(3,0)) AS checked_decimal_overflow;

-- @skip_result_check=true
-- @expect_sql_code=query.execution.numeric_out_of_range
SELECT CAST(1000 AS DECIMAL(3,0)) AS checked_decimal_overflow;

SET sql_mode = 32;
SET disable_optimizer_rules = '';
