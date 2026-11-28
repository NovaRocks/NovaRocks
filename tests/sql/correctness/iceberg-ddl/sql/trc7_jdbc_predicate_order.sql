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

-- @tags=system_catalog,jdbc,optimizer
-- An exact snapshot key may reduce input only after the original prefix allows it.

-- query 1
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT count(*) AS retained_types FROM system.jdbc.table_types
WHERE CAST(table_type AS BIGINT) > 0 AND table_type = '__missing__';

-- query 2
SELECT count(*) AS retained_types FROM system.jdbc.table_types
WHERE table_type = '__missing__' AND CAST(table_type AS BIGINT) > 0;
