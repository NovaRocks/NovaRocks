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

-- @order_sensitive=true
-- @tags=decimal,session,promotion
-- Test Objective:
-- Freeze Decimal or Float64 multiplication from declaration precision and the effective query setting.
-- Nested hints are ignored; all query scopes inherit the one frozen root setting.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.decimal_promotion_contract
(seq INT, d DECIMAL(30,10), e DECIMAL(18,9)) WITH (format_version = 3);
INSERT INTO ${case_db}.decimal_promotion_contract VALUES
ROW(1,12345678901234567890.1234567890,123456789.123456789),
ROW(2,0,123456789.123456789),ROW(3,NULL,123456789.123456789);

-- query 2
SET SESSION decimal_overflow_to_double = false;
SELECT seq,d*e AS product FROM ${case_db}.decimal_promotion_contract ORDER BY seq;

-- query 3
SET SESSION decimal_overflow_to_double = true;
SELECT seq,d*e AS product FROM ${case_db}.decimal_promotion_contract ORDER BY seq;

-- query 4
WITH SESSION decimal_overflow_to_double = false SELECT /*+ SET_VAR(decimal_overflow_to_double=false) */ seq,d*e AS product
FROM ${case_db}.decimal_promotion_contract ORDER BY seq;

-- query 5
SELECT seq,d*e AS product FROM ${case_db}.decimal_promotion_contract ORDER BY seq;

-- query 6
SET SESSION decimal_overflow_to_double = false;
WITH SESSION decimal_overflow_to_double = true SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ seq,d*e AS product
FROM ${case_db}.decimal_promotion_contract ORDER BY seq;

-- query 7
SELECT seq,d*e AS product FROM ${case_db}.decimal_promotion_contract ORDER BY seq;

-- query 8
SET SESSION decimal_overflow_to_double = true;
SELECT CAST(123456789.123456789 AS DECIMAL(18,9)) *
       CAST(123456789.123456789 AS DECIMAL(18,9)) AS exact_product;

-- query 9
-- @skip_result_check=true
-- @expect_error=[sql.admit.persisted_definition_semantics_unsupported] unsupported semantic setting: decimal_overflow_to_double=true is not captured for persistent VIEW or MATERIALIZED VIEW definition replay at line 1 column 1
CREATE VIEW default_catalog.${case_db}.decimal_promotion_view AS SELECT 1 AS v;

-- query 10
-- @skip_result_check=true
SET SESSION decimal_overflow_to_double = false
;
CREATE VIEW default_catalog.${case_db}.decimal_promotion_view AS
SELECT /*+ SET_VAR(decimal_overflow_to_double=false) */ 1 AS v
;
RESET SESSION decimal_overflow_to_double;
DROP VIEW default_catalog.${case_db}.decimal_promotion_view;

-- query 11
-- @skip_result_check=true
SET SESSION decimal_overflow_to_double = false;

-- query 12
-- @skip_result_check=true
-- @expect_error=[sql.admit.persisted_definition_semantics_unsupported] unsupported semantic setting: decimal_overflow_to_double=true is not captured for persistent VIEW or MATERIALIZED VIEW definition replay at line 1 column 1
SET SESSION decimal_overflow_to_double = true;
CREATE VIEW default_catalog.${case_db}.decimal_promotion_hint_view AS
SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 1 AS v;

-- query 13
SET SESSION decimal_overflow_to_double = false;
WITH promoted AS (SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS v),
checked AS (SELECT CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS v)
SELECT promoted.v AS promoted,checked.v AS checked,CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS outer_checked
FROM promoted CROSS JOIN checked;

-- query 14
SET SESSION decimal_overflow_to_double = true;
SELECT t.v AS checked,CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS inherited
FROM (SELECT /*+ SET_VAR(decimal_overflow_to_double=false) */ CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS v) t;

-- query 15
SET SESSION decimal_overflow_to_double = false;
SELECT (SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9))) AS promoted,
       (SELECT CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9))) AS checked,CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS outer_checked;

-- query 16
SELECT 1 AS branch,promoted AS product FROM
(SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS promoted) p
UNION ALL
SELECT 2 AS branch,CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS product
ORDER BY branch;

-- query 17

-- query 18

-- query 19

-- query 20
-- @skip_result_check=true
-- @expect_error=[sql.admit.persisted_definition_semantics_unsupported] unsupported semantic setting: decimal_overflow_to_double=true is not captured for persistent VIEW or MATERIALIZED VIEW definition replay at line 1 column 1
-- D32 target migration: reject an uncaptured root setting, retaining the derived query.
SET SESSION decimal_overflow_to_double = true;
CREATE VIEW default_catalog.${case_db}.decimal_promotion_nested_view AS
SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ v FROM (SELECT 1 AS v) t;

-- query 21
-- @skip_result_check=true
-- @expect_error=[sql.admit.persisted_definition_semantics_unsupported] unsupported semantic setting: decimal_overflow_to_double=true is not captured for persistent VIEW or MATERIALIZED VIEW definition replay at line 1 column 1
SET SESSION decimal_overflow_to_double = true;
CREATE MATERIALIZED VIEW ${case_db}.decimal_promotion_hint_mv AS SELECT /*+ SET_VAR(decimal_overflow_to_double=true) */ 1 AS v;

-- query 22
-- @skip_result_check=true
-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR,2
-- D32 target migration: validate the same invalid value at the root query boundary.
SET SESSION decimal_overflow_to_double = false;
WITH SESSION decimal_overflow_to_double = 'invalid' SELECT /*+ SET_VAR(decimal_overflow_to_double='invalid') */ 1 FROM (SELECT 1 AS v) t;

-- query 23
-- @skip_result_check=true
DROP TABLE ${case_db}.decimal_promotion_contract;

-- query 24
-- @skip_result_check=true
SET SESSION decimal_overflow_to_double = false;
CREATE VIEW default_catalog.${case_db}.decimal_replay_control_view AS SELECT CAST(12345678901234567890.1234567890 AS DECIMAL(30,10))*CAST(123456789.123456789 AS DECIMAL(18,9)) AS product;

-- query 25
-- @skip_result_check=true
SET SESSION decimal_overflow_to_double = true;

-- query 26
-- @skip_result_check=true
-- @expect_error=decimal_overflow_to_double=true is not captured
SELECT product FROM default_catalog.${case_db}.decimal_replay_control_view;

-- query 27
WITH SESSION decimal_overflow_to_double = false SELECT /*+ SET_VAR(decimal_overflow_to_double=false) */ product
FROM default_catalog.${case_db}.decimal_replay_control_view;

-- query 28
-- @skip_result_check=true
SET SESSION decimal_overflow_to_double = false;
DROP VIEW default_catalog.${case_db}.decimal_replay_control_view;

-- query 29
-- @skip_result_check=true
SET SESSION decimal_overflow_to_double = true;

-- query 30
-- @skip_result_check=true
-- @expect_error=[sql.admit.persisted_definition_semantics_unsupported] unsupported semantic setting: decimal_overflow_to_double=true is not captured for persistent VIEW or MATERIALIZED VIEW definition replay at line 1 column 1
REFRESH MATERIALIZED VIEW ${case_db}.decimal_uncaptured_missing_mv;

-- query 31
-- @skip_result_check=true
-- @expect_error=[sql.admit.persisted_definition_semantics_unsupported] unsupported semantic setting: decimal_overflow_to_double=true is not captured for persistent VIEW or MATERIALIZED VIEW definition replay at line 1 column 1
EXPLAIN REFRESH MATERIALIZED VIEW ${case_db}.decimal_uncaptured_missing_mv;

-- query 32
-- @skip_result_check=true
SET SESSION decimal_overflow_to_double = false;
