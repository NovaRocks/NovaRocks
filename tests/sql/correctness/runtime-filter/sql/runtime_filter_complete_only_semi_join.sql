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
-- @tags=runtime_filter,complete_only,semi_join
-- Test Objective:
-- 1. Left semi join RF remains enabled for the supported semi-join shape.
-- 2. Result is identical with RF enabled and disabled.

DROP TABLE IF EXISTS ${case_db}.rf_co_l;
DROP TABLE IF EXISTS ${case_db}.rf_co_r;

CREATE TABLE ${case_db}.rf_co_l (
    id INT,
    k INT
) WITH (format_version = 3);

CREATE TABLE ${case_db}.rf_co_r (
    k INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.rf_co_l VALUES
    ROW(1, 10),
    ROW(2, 20),
    ROW(3, 30),
    ROW(4, 40);

INSERT INTO ${case_db}.rf_co_r VALUES
    ROW(20),
    ROW(40);

SET SESSION disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=LEFT SEMI
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT l.id, l.k
FROM ${case_db}.rf_co_l l
WHERE l.k IN (SELECT k FROM ${case_db}.rf_co_r)
ORDER BY l.id;

SET SESSION disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT l.id, l.k
FROM ${case_db}.rf_co_l l
WHERE l.k IN (SELECT k FROM ${case_db}.rf_co_r)
ORDER BY l.id;

SET SESSION disable_optimizer_rules = '';
