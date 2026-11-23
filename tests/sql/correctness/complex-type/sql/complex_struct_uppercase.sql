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

-- Test Objective:
-- 1. Validate case-insensitive field access on STRUCT columns with mixed-case field names.
-- 2. Cover both C2.field and table.column.field qualified access patterns.
-- 3. Use PRIMARY KEY table engine with struct column.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.struct_upper_case
    (c1 int,
    c2 ROW(c2_sub1 int, C2_SUB2 int)) WITH (format_version = 3);

insert into ${case_db}.struct_upper_case values ROW(1, CAST(ROW(1,1) AS ROW(c2_sub1 INTEGER,c2_sub2 INTEGER))), ROW(2, CAST(ROW(2,2) AS ROW("C2_SUB1" INTEGER,"C2_SUB2" INTEGER)));

-- query 2
-- Access uppercase field C2_SUB2 via uppercase alias C2
select C2.c2_sub1, C2.C2_SUB2 from ${case_db}.struct_upper_case order by c1;

-- query 3
-- Access fields using lowercase alias c2
select c2.C2_SUB1, c2.c2_sub2 from ${case_db}.struct_upper_case order by c1;

-- query 4
-- Fully qualified table.column.field access with mixed case
select ${case_db}.struct_upper_case.C2.C2_SUB1, ${case_db}.struct_upper_case.C2.c2_sub2 from ${case_db}.struct_upper_case order by c1;
