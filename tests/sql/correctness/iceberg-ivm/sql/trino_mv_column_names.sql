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

-- @sequential=true
-- @order_sensitive=false
-- @tags=column-naming,view-schema
-- Test Objective: Reject anonymous and duplicate MV output declarations before publication using the current MV syntax.

-- query 1
-- @expect_trino_error=MISSING_COLUMN_NAME,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_m01
DISTRIBUTED BY HASH(a) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT CAST(1 AS integer);

-- query 2
-- @expect_trino_error=DUPLICATE_COLUMN_NAME,USER_ERROR
CREATE MATERIALIZED VIEW ${case_db}.trc_m02
DISTRIBUTED BY HASH(a) BUCKETS 2
PROPERTIES ('storage_engine' = 'iceberg')
AS SELECT CAST(1 AS integer) AS a, CAST(2 AS integer) AS a;

-- query 3
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_m01;

-- query 4
-- @cleanup=true
-- @skip_result_check=true
DROP MATERIALIZED VIEW IF EXISTS ${case_db}.trc_m02;
