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
-- @tags=decimal,arithmetic,persistent_view
-- Test Objective: Preserve an exact decimal product and the declared result of a saved expression.
-- Oracle: the unchanged exact product and Trino 484 decimal_multiply_positive_half.

-- query 1
SELECT CAST(123456789.123456789 AS DECIMAL(18,9)) *
       CAST(123456789.123456789 AS DECIMAL(18,9)) AS exact_product;

-- query 2
-- @skip_result_check=true
CREATE VIEW default_catalog.${case_db}.decimal_replay_control_view AS
SELECT CAST(1 AS DECIMAL(33,0)) * CAST('0.0000005' AS DECIMAL(7,7)) AS product;

-- query 3
SELECT product FROM default_catalog.${case_db}.decimal_replay_control_view;

-- query 4
-- @skip_result_check=true
DROP VIEW default_catalog.${case_db}.decimal_replay_control_view;
