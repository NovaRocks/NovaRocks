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
-- @tags=runtime_filter,complete_only,inner_join
-- Test Objective:
-- 1. Inner join result is identical with RF enabled and disabled.
-- 2. Complete-only lifecycle must never expose a partial filter to the probe.

DROP TABLE IF EXISTS ${case_db}.rf_co_orders;
DROP TABLE IF EXISTS ${case_db}.rf_co_customers;

CREATE TABLE ${case_db}.rf_co_customers (
    id INT,
    name VARCHAR
) WITH (format_version = 3);

CREATE TABLE ${case_db}.rf_co_orders (
    oid INT,
    customer_id INT
) WITH (format_version = 3);

INSERT INTO ${case_db}.rf_co_customers VALUES
    ROW(2, 'b'),
    ROW(4, 'd'),
    ROW(8, 'h');

INSERT INTO ${case_db}.rf_co_orders VALUES
    ROW(1, 2),
    ROW(2, 4),
    ROW(3, 8),
    ROW(4, 5),
    ROW(5, 4);

SET SESSION disable_optimizer_rules = '';
-- @explain_contains=HASH JOIN (
-- @explain_contains=producer binding
-- @explain_contains=consumer binding
SELECT o.oid, o.customer_id
FROM ${case_db}.rf_co_orders o
JOIN ${case_db}.rf_co_customers c ON o.customer_id = c.id
ORDER BY o.oid;

SET SESSION disable_optimizer_rules = 'RuntimeFilterPushDown';
SELECT o.oid, o.customer_id
FROM ${case_db}.rf_co_orders o
JOIN ${case_db}.rf_co_customers c ON o.customer_id = c.id
ORDER BY o.oid;

SET SESSION disable_optimizer_rules = '';
