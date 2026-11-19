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

-- @tags=low-cardinality,dictionary,observability
-- Verify EXPLAIN ANALYZE exposes dictionary carrier runtime counters.

CREATE TABLE ${case_db}.dict_observability_orders (
  order_id BIGINT,
  status STRING,
  amount INT
) TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.dict_observability_orders VALUES
  (1, 'NEW', 10),
  (2, 'PAID', 20),
  (3, 'PAID', 30),
  (4, 'CANCELLED', 40),
  (5, 'SHIPPED', 50),
  (6, NULL, 60);

-- @normalize_explain_timing=true
-- @result_contains=dict={in_rows=
-- @result_contains=kept_rows=
-- @result_contains=hydrated_rows=
-- @result_contains=in_cols=
-- @result_contains=kept_cols=
-- @result_contains=hydrated_cols=
-- @result_contains=unsupported_cols=
EXPLAIN ANALYZE
SELECT status, count() AS cnt
FROM ${case_db}.dict_observability_orders
WHERE status <> 'CANCELLED'
GROUP BY status
ORDER BY status;
