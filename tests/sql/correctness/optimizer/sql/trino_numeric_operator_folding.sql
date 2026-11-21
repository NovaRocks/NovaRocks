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

-- @order_sensitive=false
-- @tags=arithmetic,constant_folding,trino,self_contained
-- Test Objective: Fold valid bound arithmetic without evaluating a branch that is never selected.
-- Oracle: Trino 484 terminal values, type signatures, and error names.
-- Maintenance: Optimizer suite owner; the folded projection is checked against the original operator with folding disabled.

-- query 1
-- Reference: decimal_ratio
-- @explain_contains=[0.333333 AS folded_ratio]
SELECT DECIMAL '1.00' / DECIMAL '3.00' AS folded_ratio;

-- query 2
-- Reference: control_unselected
SELECT CASE WHEN FALSE THEN DECIMAL '1' / DECIMAL '0' ELSE DECIMAL '2' END;

-- query 3
-- Reference: control_unselected_values
SELECT CASE WHEN selected THEN n / d ELSE DECIMAL '2' END FROM (VALUES (FALSE, DECIMAL '1', DECIMAL '0'), (TRUE, DECIMAL '8', DECIMAL '2')) AS t(selected,n,d) ORDER BY selected;

-- query 4
-- Reference: control_selected
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT CASE WHEN TRUE THEN DECIMAL '1' / DECIMAL '0' ELSE DECIMAL '2' END;

-- query 5
-- Reference: control_selected_values
-- @expect_trino_error=DIVISION_BY_ZERO,USER_ERROR
SELECT CASE WHEN selected THEN n / d ELSE DECIMAL '2' END FROM (VALUES (TRUE, DECIMAL '1', DECIMAL '0'), (FALSE, DECIMAL '8', DECIMAL '2')) AS t(selected,n,d) ORDER BY selected;

-- query 6
-- @skip_result_check=true
SET SESSION disable_optimizer_rules = 'FoldConstant';

-- query 7
-- Reference: decimal_ratio
-- @explain_contains=1.00 / 3.00
-- @explain_not_contains=[0.333333 AS folded_ratio]
SELECT DECIMAL '1.00' / DECIMAL '3.00' AS folded_ratio;

-- query 8
-- @skip_result_check=true
RESET SESSION disable_optimizer_rules;
